#!/usr/bin/env python3
"""Worker: unduh magnet via libtorrent, sediakan file lewat HTTP (support Range).

Endpoint:
  GET  /health                 -> {"ok": true}
  POST /add                    -> body {"magnet": "..."} -> {"id": "..."}
  GET  /status/<id>            -> {"status": "queued|downloading|ready|error", "progress": 0..1, ...}
  GET  /file/<id>/<filename>   -> file video (support Range request)
  POST /delete/<id>            -> hapus file lokal
"""
import argparse
import json
import os
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote

import libtorrent as lt

VIDEO_EXT = (".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm", ".m4v")
OUT_DIR = os.environ.get("WORKER_OUT_DIR", "/tmp/magnet_dl")
MIME = {
    ".mp4": "video/mp4", ".m4v": "video/mp4", ".webm": "video/webm",
    ".mkv": "video/x-matroska", ".avi": "video/x-msvideo",
    ".mov": "video/quicktime", ".wmv": "video/x-ms-wmv", ".flv": "video/x-flv",
}

TASKS = {}
LOCK = threading.Lock()


def download_one(task):
    os.makedirs(OUT_DIR, exist_ok=True)
    ses = lt.session()
    try:
        ses.listen_on(6881, 6891)
    except RuntimeError:
        pass  # port listen gagal tidak fatal, DHT tetap jalan
    for router, port in (("router.bittorrent.com", 6881),
                         ("dht.transmissionbt.com", 6881),
                         ("router.utorrent.com", 6881)):
        try:
            ses.add_dht_router(router, port)
        except RuntimeError:
            pass

    h = lt.add_magnet_uri(ses, task["magnet"], {"save_path": OUT_DIR})
    print(f"[worker] menunggu metadata {task['id']}", flush=True)
    t0 = time.time()
    while not h.has_metadata():
        if time.time() - t0 > 180:
            raise TimeoutError("metadata tidak diterima dalam 180s")
        time.sleep(1)

    info = h.get_torrent_info()
    files = [(f.path, f.size) for f in info.files()]
    videos = [(i, p, s) for i, (p, s) in enumerate(files) if p.lower().endswith(VIDEO_EXT)]
    if not videos:
        raise RuntimeError("tidak ada file video di torrent ini")
    videos.sort(key=lambda x: -x[2])
    target_idx, target_path, target_size = videos[0]
    task["name"] = os.path.basename(target_path)
    task["size"] = target_size
    print(f"[worker] {info.name()} -> unduh {task['name']} ({target_size/1e6:.1f} MB)", flush=True)

    t0 = time.time()
    while not h.is_seed():
        s = h.status()
        task["progress"] = round(s.progress, 4)
        task["rate"] = round(s.download_rate, 1)
        task["peers"] = s.num_peers
        # file target sudah utuh -> tidak perlu menunggu torrent 100% (seeder bisa hilang)
        try:
            if h.file_progress()[target_idx] >= target_size:
                h.pause()
                h.flush_cache()
                task["path"] = os.path.join(OUT_DIR, target_path)
                task["progress"] = 1.0
                task["status"] = "ready"
                print(f"[worker] {task['id']} siap (file target utuh): {task['path']}", flush=True)
                return
        except (RuntimeError, IndexError):
            pass  # handle bisa invalid saat race dengan status; coba lagi di iterasi berikut
        if time.time() - t0 > task["timeout"]:
            raise TimeoutError(f"unduhan tidak selesai dalam {task['timeout']}s")
        time.sleep(3)

    task["path"] = os.path.join(OUT_DIR, target_path)
    task["progress"] = 1.0
    task["status"] = "ready"
    print(f"[worker] {task['id']} siap: {task['path']}", flush=True)


def worker_loop():
    while True:
        task = None
        with LOCK:
            for t in TASKS.values():
                if t["status"] == "queued":
                    t["status"] = "downloading"
                    task = t
                    break
        if task:
            try:
                download_one(task)
            except Exception as e:  # noqa: BLE001 - laporkan semua kegagalan ke status
                task["status"] = "error"
                task["error"] = str(e)
                print(f"[worker] error {task['id']}: {e}", flush=True)
        else:
            time.sleep(1)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        print("[http] " + fmt % args, flush=True)

    def _json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parts = [unquote(p) for p in self.path.split("?")[0].split("/") if p]
        if parts == ["health"]:
            return self._json(200, {"ok": True, "tasks": len(TASKS)})
        if len(parts) >= 2 and parts[0] == "status":
            with LOCK:
                t = TASKS.get(parts[1])
            if not t:
                return self._json(404, {"error": "task tidak ditemukan"})
            return self._json(200, {k: v for k, v in t.items() if k != "magnet"})
        if len(parts) >= 2 and parts[0] == "file":
            with LOCK:
                t = TASKS.get(parts[1])
            if not t or t["status"] != "ready":
                return self._json(404, {"error": "file belum siap"})
            return self._serve_file(t)
        return self._json(404, {"error": "endpoint tidak dikenal"})

    def do_POST(self):
        parts = [unquote(p) for p in self.path.split("?")[0].split("/") if p]
        if parts == ["add"]:
            try:
                length = int(self.headers.get("Content-Length", 0))
                payload = json.loads(self.rfile.read(length) or b"{}")
                magnet = payload["magnet"]
            except (ValueError, KeyError) as e:
                return self._json(400, {"error": f"body tidak valid: {e}"})
            if not magnet.startswith("magnet:?"):
                return self._json(400, {"error": "bukan magnet link"})
            tid = uuid.uuid4().hex[:12]
            with LOCK:
                TASKS[tid] = {
                    "id": tid, "magnet": magnet, "status": "queued",
                    "progress": 0.0, "timeout": int(payload.get("timeout", 7200)),
                    "name": None, "size": None, "path": None,
                }
            return self._json(200, {"id": tid})
        if len(parts) == 2 and parts[0] == "delete":
            with LOCK:
                t = TASKS.pop(parts[1], None)
            if not t:
                return self._json(404, {"error": "task tidak ditemukan"})
            if t.get("path"):
                try:
                    os.remove(t["path"])
                except OSError:
                    pass
            return self._json(200, {"deleted": parts[1]})
        return self._json(404, {"error": "endpoint tidak dikenal"})

    def _serve_file(self, t):
        path, size = t["path"], t["size"] or os.path.getsize(t["path"])
        ext = os.path.splitext(path)[1].lower()
        ctype = MIME.get(ext, "application/octet-stream")

        range_hdr = self.headers.get("Range")
        start, end = 0, size - 1
        if range_hdr and range_hdr.startswith("bytes="):
            try:
                spec = range_hdr[6:].split(",")[0].strip()
                s_str, _, e_str = spec.partition("-")
                if s_str == "":
                    # suffix range: N byte terakhir
                    start = max(0, size - int(e_str))
                else:
                    start = int(s_str)
                    if e_str:
                        end = min(int(e_str), size - 1)
                if start > end or start >= size:
                    raise ValueError
            except ValueError:
                self.send_response(416)
                self.send_header("Content-Range", f"bytes */{size}")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return

        self.send_response(206 if range_hdr else 200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(end - start + 1))
        self.send_header("Accept-Ranges", "bytes")
        if range_hdr:
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.end_headers()

        with open(path, "rb") as f:
            f.seek(start)
            remaining = end - start + 1
            while remaining > 0:
                chunk = f.read(min(1024 * 1024, remaining))
                if not chunk:
                    break
                try:
                    self.wfile.write(chunk)
                except (BrokenPipeError, ConnectionResetError):
                    return
                remaining -= len(chunk)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8080)
    args = ap.parse_args()

    threading.Thread(target=worker_loop, daemon=True).start()
    srv = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    print(f"[worker] listening di 0.0.0.0:{args.port}, output dir {OUT_DIR}", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
