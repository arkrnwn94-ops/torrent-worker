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
import re
import shutil
import subprocess
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote

import libtorrent as lt

try:
    import vidsrc_extract  # modul resolver vidsrc.sh (satu folder dgn worker.py)
except ImportError:
    vidsrc_extract = None

VIDEO_EXT = (".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm", ".m4v")
OUT_DIR = os.environ.get("WORKER_OUT_DIR", "/tmp/magnet_dl")
MIME = {
    ".mp4": "video/mp4", ".m4v": "video/mp4", ".webm": "video/webm",
    ".mkv": "video/x-matroska", ".avi": "video/x-msvideo",
    ".mov": "video/quicktime", ".wmv": "video/x-ms-wmv", ".flv": "video/x-flv",
}

TASKS = {}
LOCK = threading.Lock()
MAX_CONCURRENT = int(os.environ.get("WORKER_CONCURRENCY", "2"))


def cleanup_task_files(task):
    """Hapus file video beserta folder torrent-nya (sub/junk YTS ikut terbuang)."""
    path = task.get("path")
    if path and os.path.isfile(path):
        try:
            os.remove(path)
        except OSError:
            return
    folder = task.get("folder")
    if folder and os.path.isdir(folder):
        shutil.rmtree(folder, ignore_errors=True)


def download_one(task):
    os.makedirs(OUT_DIR, exist_ok=True)
    ses = lt.session()
    # batasi memori: mesin codespace cuma 8GB — cache disk & peerlist default
    # libtorrent bisa membengkak sampai OOM saat 2-3 unduhan paralel.
    # apply_settings menerima dict di binding python; kalau gagal, lanjut default.
    try:
        ses.apply_settings({
            "cache_size": 512,               # 512 blok x 16KiB = 8MiB
            "cache_expiry": 60,
            "max_queued_disk_bytes": 4 * 1024 * 1024,
            "max_peerlist_size": 1000,
        })
    except (RuntimeError, TypeError, KeyError):
        pass
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
    rel_dir = os.path.dirname(target_path)  # None kalau file di root torrent (jangan rmtree OUT_DIR!)
    task["folder"] = os.path.join(OUT_DIR, rel_dir) if rel_dir else None
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
                task["done_at"] = time.time()
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
    task["done_at"] = time.time()
    print(f"[worker] {task['id']} siap: {task['path']}", flush=True)


def download_vidsrc(task):
    """Resolve vidsrc.sh → master.m3u8 (token IP-bound codespace) → ffmpeg mux MP4.
    Token generate.php terikat IP pemanggil, jadi resolve WAJIB di sini (codespace),
    bukan di klien. ffmpeg -c copy: remux TS→MP4 tanpa re-encode (cepat, hemat CPU)."""
    if vidsrc_extract is None:
        raise RuntimeError("modul vidsrc_extract tidak tersedia di worker")
    os.makedirs(OUT_DIR, exist_ok=True)
    info = vidsrc_extract.resolve(
        task["imdb"], task.get("mtype", "movie"),
        task.get("season"), task.get("episode"))
    master = info["master_url"]
    ref = info["referer"]
    print(f"[worker] {task['id']} vidsrc resolved: {info['origin']} "
          f"({len(info['all_variants'])} varian)", flush=True)

    safe = re.sub(r"[^A-Za-z0-9._-]", "_", task["imdb"])
    ep = f"_s{task['season']}e{task['episode']}" if task.get("season") else ""
    fname = f"{safe}{ep}.mp4"
    out_path = os.path.join(OUT_DIR, fname)
    task["name"] = fname
    task["folder"] = None  # file langsung di OUT_DIR, jangan rmtree

    # header wajib supaya CDN tak balas 401; ffmpeg pakai untuk playlist+segmen
    hdr = f"Referer: {ref}\r\nUser-Agent: {vidsrc_extract.UA}\r\nOrigin: {ref.rstrip('/')}\r\n"
    cmd = [
        "ffmpeg", "-y", "-hide_banner", "-loglevel", "warning",
        "-headers", hdr,
        "-protocol_whitelist", "file,http,https,tcp,tls,crypto",
        "-i", master,
        "-c", "copy", "-bsf:a", "aac_adtstoasc",
        "-movflags", "+faststart", out_path,
    ]
    print(f"[worker] {task['id']} ffmpeg mux → {fname}", flush=True)
    task["status"] = "downloading"
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    t0 = time.time()
    last = ""
    for line in proc.stdout:
        last = line.strip()
        if time.time() - t0 > task["timeout"]:
            proc.kill()
            raise TimeoutError(f"ffmpeg > {task['timeout']}s")
    rc = proc.wait()
    if rc != 0 or not os.path.isfile(out_path) or os.path.getsize(out_path) < 100000:
        raise RuntimeError(f"ffmpeg gagal (rc={rc}): {last}")
    task["path"] = out_path
    task["size"] = os.path.getsize(out_path)
    task["progress"] = 1.0
    task["status"] = "ready"
    task["done_at"] = time.time()
    print(f"[worker] {task['id']} siap: {out_path} ({task['size']/1e6:.1f} MB)", flush=True)


SUPJAV_REF = "https://supjav.com/"
SUPJAV_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
             "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
_RESOLVER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "supjav_resolve.mjs")


def download_supjav(task):
    """Resolve 4 host supjav (data_links hex) DI SINI (codespace, IP sama dgn
    yg mengunduh → token IP-bound sah), pilih server ukuran TERBESAR, unduh:
      - hls  → ffmpeg -c copy remux varian tertinggi → MP4
      - mp4  → unduh langsung (streamtape get_video, ikut 302 tapecontent)
    lalu sajikan supaya Streamtape /remotedl menariknya."""
    os.makedirs(OUT_DIR, exist_ok=True)
    dl = json.dumps(task["data_links"])
    cmd = ["node", _RESOLVER, dl]
    if task.get("only"):
        cmd.append("--only=" + task["only"])
    print(f"[worker] {task['id']} resolve supjav ({len(task['data_links'])} host)…", flush=True)
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if r.returncode != 0:
        raise RuntimeError(f"resolver gagal (rc={r.returncode}): {r.stderr.strip()[:300]}")
    hasil = json.loads(r.stdout.strip().splitlines()[-1])
    if not hasil.get("ok") or not hasil.get("pilih"):
        raise RuntimeError(f"tidak ada server valid: {hasil.get('pesan') or hasil.get('kandidat')}")
    pilih = hasil["pilih"]
    task["pilih"] = {k: pilih.get(k) for k in ("server", "tipe", "bytes", "tinggi")}
    task["kandidat"] = [{k: c.get(k) for k in ("server", "ok", "bytes", "tinggi")} for c in hasil.get("kandidat", [])]
    print(f"[worker] {task['id']} pilih {pilih['server']} {pilih['tipe']} "
          f"~{(pilih.get('bytes') or 0)/1e6:.0f}MB {pilih.get('tinggi')}p", flush=True)

    safe = re.sub(r"[^A-Za-z0-9._-]", "_", task.get("code") or task["id"])
    out_path = os.path.join(OUT_DIR, f"{safe}.mp4")
    task["name"] = f"{safe}.mp4"
    task["folder"] = None
    task["status"] = "downloading"

    if pilih["tipe"] == "hls":
        src = pilih.get("varian") or pilih["url"]
        hdr = f"Referer: {SUPJAV_REF}\r\nUser-Agent: {SUPJAV_UA}\r\n"
        cmd = ["ffmpeg", "-y", "-hide_banner", "-loglevel", "warning",
               "-headers", hdr,
               "-protocol_whitelist", "file,http,https,tcp,tls,crypto",
               "-i", src, "-c", "copy", "-bsf:a", "aac_adtstoasc",
               "-movflags", "+faststart", out_path]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        t0, last = time.time(), ""
        for line in proc.stdout:
            last = line.strip()
            if time.time() - t0 > task["timeout"]:
                proc.kill()
                raise TimeoutError(f"ffmpeg > {task['timeout']}s")
        rc = proc.wait()
        if rc != 0 or not os.path.isfile(out_path) or os.path.getsize(out_path) < 100000:
            raise RuntimeError(f"ffmpeg gagal (rc={rc}): {last}")
    else:  # mp4 progresif (streamtape)
        import urllib.request
        req = urllib.request.Request(pilih["url"], headers={"Referer": SUPJAV_REF, "User-Agent": SUPJAV_UA})
        t0 = time.time()
        with urllib.request.urlopen(req, timeout=120) as resp, open(out_path, "wb") as f:
            ctype = resp.headers.get("content-type", "")
            if "video" not in ctype and "octet" not in ctype:
                raise RuntimeError(f"unduhan mp4 ditolak: {resp.status} {ctype}")
            while True:
                chunk = resp.read(1 << 20)
                if not chunk:
                    break
                f.write(chunk)
                if time.time() - t0 > task["timeout"]:
                    raise TimeoutError(f"unduh mp4 > {task['timeout']}s")
        if os.path.getsize(out_path) < 100000:
            raise RuntimeError("file mp4 terlalu kecil")

    task["path"] = out_path
    task["size"] = os.path.getsize(out_path)
    task["progress"] = 1.0
    task["status"] = "ready"
    task["done_at"] = time.time()
    print(f"[worker] {task['id']} siap: {out_path} ({task['size']/1e6:.1f} MB)", flush=True)


def run_task(task):
    try:
        if task.get("kind") == "vidsrc":
            download_vidsrc(task)
        elif task.get("kind") == "supjav":
            download_supjav(task)
        else:
            download_one(task)
    except Exception as e:  # noqa: BLE001 - laporkan semua kegagalan ke status
        task["status"] = "error"
        task["error"] = str(e)
        task["done_at"] = time.time()
        print(f"[worker] error {task['id']}: {e}", flush=True)


def reaper_loop(max_age_sec=2 * 3600):
    """Bersihkan task ready/error yang sudah tua (file tertinggal karena kegagalan
    jaringan klien). ready < max_age tidak disentuh: streamtape bisa saja masih menarik."""
    while True:
        time.sleep(600)
        now = time.time()
        with LOCK:
            stale = [tid for tid, t in TASKS.items()
                     if t["status"] in ("ready", "error") and now - t.get("done_at", now) > max_age_sec
                     and tid not in _active_downloads()]
        for tid in stale:
            with LOCK:
                t = TASKS.pop(tid, None)
            if t:
                cleanup_task_files(t)
                print(f"[reaper] bersihkan task tua {tid}", flush=True)


def _active_downloads():
    return [t["id"] for t in TASKS.values() if t["status"] in ("queued", "downloading")]


def worker_loop():
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as pool:
        while True:
            task = None
            with LOCK:
                for t in TASKS.values():
                    if t["status"] == "queued":
                        t["status"] = "downloading"
                        task = t
                        break
            if task:
                pool.submit(run_task, task)
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
            return self._json(200, {k: v for k, v in t.items() if k not in ("magnet", "data_links")})
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
        if parts == ["add_vidsrc"]:
            try:
                length = int(self.headers.get("Content-Length", 0))
                payload = json.loads(self.rfile.read(length) or b"{}")
                imdb = payload["imdb"]
            except (ValueError, KeyError) as e:
                return self._json(400, {"error": f"body tidak valid: {e}"})
            tid = uuid.uuid4().hex[:12]
            with LOCK:
                TASKS[tid] = {
                    "id": tid, "kind": "vidsrc", "status": "queued",
                    "imdb": imdb, "mtype": payload.get("type", "movie"),
                    "season": payload.get("season"), "episode": payload.get("episode"),
                    "progress": 0.0, "timeout": int(payload.get("timeout", 3600)),
                    "name": None, "size": None, "path": None,
                }
            return self._json(200, {"id": tid})
        if parts == ["add_supjav"]:
            try:
                length = int(self.headers.get("Content-Length", 0))
                payload = json.loads(self.rfile.read(length) or b"{}")
                data_links = payload["data_links"]
                if not isinstance(data_links, list) or not data_links:
                    raise ValueError("data_links kosong")
            except (ValueError, KeyError) as e:
                return self._json(400, {"error": f"body tidak valid: {e}"})
            tid = uuid.uuid4().hex[:12]
            with LOCK:
                TASKS[tid] = {
                    "id": tid, "kind": "supjav", "status": "queued",
                    "data_links": data_links, "code": payload.get("code"),
                    "only": payload.get("only"),
                    "progress": 0.0, "timeout": int(payload.get("timeout", 5400)),
                    "name": None, "size": None, "path": None,
                }
            return self._json(200, {"id": tid})
        if len(parts) == 2 and parts[0] == "delete":
            with LOCK:
                t = TASKS.get(parts[1])
            if not t:
                return self._json(404, {"error": "task tidak ditemukan"})
            if t["status"] in ("queued", "downloading"):
                return self._json(409, {"error": "task masih berjalan, tunggu ready/error"})
            with LOCK:
                TASKS.pop(parts[1], None)
            cleanup_task_files(t)
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
    threading.Thread(target=reaper_loop, daemon=True).start()
    srv = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    print(f"[worker] listening di 0.0.0.0:{args.port}, output dir {OUT_DIR}, "
          f"{MAX_CONCURRENT} unduhan paralel", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
