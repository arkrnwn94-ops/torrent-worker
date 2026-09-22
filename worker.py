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

try:
    import libtorrent as lt  # cuma dipakai fallback download_one (kind=libtorrent)
except ImportError:
    lt = None  # aria2 = jalur utama; libtorrent opsional (pip-nya sering rewel di codespace)

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


def _match_file_index(files, want):
    """Cari indeks file yang cocok dengan `want` (path dari file_map DB).

    file_map dibangun dari daftar file torrent (nyaa), jadi biasanya identik dengan
    f.path libtorrent. Untuk tahan beda kecil (root folder), coba: exact → salah satu
    endswith yang lain → basename sama.
    """
    want = want.strip().lstrip("/")
    paths = [p for p, _ in files]
    for i, p in enumerate(paths):
        if p == want:
            return i
    for i, p in enumerate(paths):
        if p.endswith(want) or want.endswith(p):
            return i
    wb = os.path.basename(want)
    for i, p in enumerate(paths):
        if os.path.basename(p) == wb:
            return i
    return None


def download_one(task):
    if lt is None:
        raise RuntimeError("libtorrent tak terpasang di codespace ini — pakai kind aria2 (default)")
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

    want = task.get("file_path")
    if want:
        # Batch pack: unduh HANYA file episode yang diminta. Set semua file lain
        # priority 0 supaya libtorrent tak menarik ratusan GB isi pack.
        idx = _match_file_index(files, want)
        if idx is None:
            raise RuntimeError(f"file '{want}' tidak ada di torrent (dari {len(files)} file)")
        target_idx, (target_path, target_size) = idx, files[idx]
        prios = [0] * info.num_files()
        prios[target_idx] = 7  # prioritas tertinggi
        try:
            h.prioritize_files(prios)
        except (RuntimeError, TypeError):
            for i in range(info.num_files()):
                h.file_priority(i, 7 if i == target_idx else 0)
        print(f"[worker] {info.name()} -> file terpilih (batch): {os.path.basename(target_path)}", flush=True)
    else:
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


def _aria2_list_files(torrent_path):
    """Kembalikan [(idx1based, rel_path, size_bytes)] dari `aria2c -S <torrent>`."""
    r = subprocess.run(["aria2c", "-S", torrent_path], capture_output=True, text=True, timeout=120)
    out = r.stdout
    files, cur = [], None
    for line in out.splitlines():
        m = re.match(r"^\s*(\d+)\|(.+)$", line)
        if m:
            if cur:
                files.append(cur)
            p = m.group(2).strip()
            if p.startswith("./"):
                p = p[2:]
            cur = [int(m.group(1)), p, 0]
            continue
        m2 = re.search(r"\(([\d,]+)\)", line)  # baris "   |298MiB (312,730,573)"
        if m2 and cur:
            cur[2] = int(m2.group(1).replace(",", ""))
    if cur:
        files.append(cur)
    return files


_NON_EP_RE = re.compile(
    r"(?:^|/|_|\.|\s|\[)(?:nc(?:ed|op)?|ncop|nced|creditless|opening|ending|"
    r"movies?|extras?|specials?|ova|ona|oav|menu|preview|\bpv\b|scan|bonus|omake|"
    r"sp\d*|bd\s*menu)(?:\b|/|_|\.|\s|\])", re.I)


def _file_ep_num(basename):
    """Nomor episode dari NAMA FILE (longgar), atau None."""
    b = basename
    for pat in (r"[sS]\d+[eE](\d{1,4})", r"[\s._\-\[(]e(?:p|pisode)?[\s._]?(\d{1,4})\b",
                r"[\s._\-](\d{1,4})[\s._\-]", r"\b(\d{1,4})\b"):
        m = re.search(pat, b, re.I)
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                pass
    return None


def _match_episode_file(files, ep):
    """Pilih file video yang = episode `ep` dari daftar (buang NCED/OP/ED/movie/
    extra). files = [(idx, path, size)]. Balikan tuple file atau None.

    Cek NON-episode di BASENAME + folder terdekat saja — bukan seluruh path,
    karena nama folder rilis sering memuat tag '[Extras]'/'[Complete]' yang
    bukan berarti file-nya extra."""
    def is_extra(path):
        parts = path.replace("\\", "/").split("/")
        tail = "/".join(parts[-2:])  # basename + folder induk langsung
        return bool(_NON_EP_RE.search(os.path.basename(path))) or \
            any(seg.strip("[]() ").lower() in ("nc", "ncop", "nced", "extras", "extra",
                "movies", "movie", "specials", "special", "ova", "ona", "menu", "pv",
                "bonus", "omake", "creditless") for seg in parts[:-1])
    vids = [f for f in files if f[1].lower().endswith(VIDEO_EXT) and not is_extra(f[1])]
    cocok = [f for f in vids if _file_ep_num(os.path.basename(f[1])) == ep]
    if cocok:
        return max(cocok, key=lambda f: f[2])  # yg terbesar (hindari sample/preview)
    return None


def download_aria2(task):
    """Unduh via aria2c (lebih cepat: multi-koneksi + peer agresif). Selektif:
    kalau task['file_path'] ada (batch pack) → cuma unduh 1 file episode itu
    (--select-file), bukan seluruh pack. Kalau tidak, pilih file video terbesar."""
    workdir = os.path.join(OUT_DIR, task["id"])
    os.makedirs(workdir, exist_ok=True)
    task["folder"] = workdir  # cleanup_task_files hapus seluruh folder task ini

    # 1) metadata magnet → .torrent
    print(f"[worker] {task['id']} aria2 ambil metadata…", flush=True)
    subprocess.run(
        ["aria2c", "--bt-metadata-only=true", "--bt-save-metadata=true",
         "--bt-stop-timeout=180", "--seed-time=0", "-d", workdir, task["magnet"]],
        capture_output=True, text=True, timeout=240)
    tors = [f for f in os.listdir(workdir) if f.endswith(".torrent")]
    if not tors:
        raise RuntimeError("metadata torrent tidak diterima (aria2)")
    torrent_path = os.path.join(workdir, tors[0])

    # 2) daftar file → pilih index
    files = _aria2_list_files(torrent_path)
    if not files:
        raise RuntimeError("aria2 tidak bisa membaca daftar file torrent")
    want = task.get("file_path")
    ep = task.get("episode")
    if want:
        wb = os.path.basename(want).lower()
        pick = next((f for f in files if os.path.basename(f[1]).lower() == wb), None) \
            or next((f for f in files if f[1].lower().endswith(want.lower())), None) \
            or next((f for f in files if os.path.basename(want).lower() in f[1].lower()), None)
        if not pick and ep:  # file_path meleset → coba resolusi episode
            pick = _match_episode_file(files, int(ep))
        if not pick:
            raise RuntimeError(f"file '{want}' tidak ada di torrent ({len(files)} file)")
    elif ep is not None:
        # batch tanpa file_map: cari sendiri file episode ini di dalam pack
        pick = _match_episode_file(files, int(ep))
        if not pick:
            raise RuntimeError(f"episode {ep} tidak ketemu di torrent ({len(files)} file)")
    else:
        vids = [f for f in files if f[1].lower().endswith(VIDEO_EXT)]
        if not vids:
            raise RuntimeError("tidak ada file video di torrent ini")
        pick = max(vids, key=lambda f: f[2])
    idx, rel_path, total = pick
    target = os.path.join(workdir, rel_path)
    task["name"] = os.path.basename(rel_path)
    task["size"] = total
    print(f"[worker] {task['id']} aria2 select #{idx} {task['name']} ({total/1e6:.1f} MB)", flush=True)

    # 3) unduh file terpilih saja
    # fail-fast: kalau 300s tak ada data (torrent mati/seeder hilang) → stop, jangan
    # gantung 2 jam ngabisin slot+quota. Hard-cap tetap dijaga loop timeout Python.
    stall = min(300, task.get("timeout", 7200))
    cmd = ["aria2c", f"--select-file={idx}", "-d", workdir, "--seed-time=0",
           "--bt-stop-timeout=" + str(stall),
           "--max-connection-per-server=16", "--split=16", "--min-split-size=1M",
           "--bt-max-peers=300", "--file-allocation=none", "--summary-interval=10",
           "--console-log-level=warn", "--bt-remove-unselected-file=true", torrent_path]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    t0 = time.time()
    while proc.poll() is None:
        try:
            if os.path.isfile(target):
                cur = os.path.getsize(target)
                task["progress"] = round(cur / total, 4) if total else 0.0
                if total and cur >= total:
                    break
        except OSError:
            pass
        if time.time() - t0 > task["timeout"]:
            proc.kill()
            raise TimeoutError(f"aria2 unduh > {task['timeout']}s")
        time.sleep(3)
    if not os.path.isfile(target) or os.path.getsize(target) < 100000:
        tail = ""
        try:
            tail = (proc.stdout.read() or "")[-300:]
        except Exception:
            pass
        raise RuntimeError(f"aria2 gagal / file kosong: {tail}")
    try:
        proc.terminate()
    except Exception:
        pass
    # Pilih audio (mis. Jepang) + jadikan MP4 ST-friendly bila diminta.
    want_audio = task.get("audio")
    if want_audio:
        target = _remux_audio(target, want_audio, task)

    task["path"] = target
    task["size"] = os.path.getsize(target)
    task["progress"] = 1.0
    task["status"] = "ready"
    task["done_at"] = time.time()
    print(f"[worker] {task['id']} siap (aria2): {target} ({task['size']/1e6:.1f} MB)", flush=True)


def _remux_audio(src, lang, task):
    """Ambil track audio berbahasa `lang` (mis. 'jpn') jadi SATU-satunya audio,
    output MP4 (video copy + audio AAC = ST-friendly). Kalau track bahasa itu
    tak ada, fallback ke audio pertama (dicatat). StreamTape simpan track-1 saat
    MKV->MP4, jadi ini yang menjamin audionya Jepang, bukan Inggris."""
    try:
        pr = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "a",
             "-show_entries", "stream=index:stream_tags=language",
             "-of", "csv=p=0", src], capture_output=True, text=True, timeout=120)
    except Exception as e:
        print(f"[worker] {task['id']} ffprobe gagal: {e} - pakai file asli", flush=True)
        return src
    audio_langs = []
    for line in pr.stdout.splitlines():
        parts = line.split(",")
        audio_langs.append(parts[1].strip().lower() if len(parts) > 1 else "")
    if not audio_langs:
        return src
    sel = next((i for i, l in enumerate(audio_langs) if l.startswith(lang[:2]) or l == lang), None)
    if sel is None:
        print(f"[worker] {task['id']} track '{lang}' tak ada (audio={audio_langs}) - pakai audio-0", flush=True)
        sel = 0
    else:
        print(f"[worker] {task['id']} pilih audio a:{sel} ({audio_langs[sel]})", flush=True)
    out = os.path.splitext(src)[0] + f".{lang}.mp4"
    cmd = ["ffmpeg", "-y", "-hide_banner", "-loglevel", "warning", "-i", src,
           "-map", "0:v:0", "-map", f"0:a:{sel}",
           "-c:v", "copy", "-c:a", "aac", "-b:a", "192k",
           "-movflags", "+faststart", out]
    task["status"] = "remuxing"
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    t0, last = time.time(), ""
    for line in proc.stdout:
        last = line.strip()
        if time.time() - t0 > task["timeout"]:
            proc.kill()
            raise TimeoutError("ffmpeg remux audio timeout")
    if proc.wait() != 0 or not os.path.isfile(out) or os.path.getsize(out) < 100000:
        raise RuntimeError(f"ffmpeg remux gagal: {last}")
    try:
        os.remove(src)  # hemat disk codespace
    except OSError:
        pass
    return out


def run_task(task):
    try:
        if task.get("kind") == "vidsrc":
            download_vidsrc(task)
        elif task.get("kind") == "libtorrent":
            download_one(task)          # fallback lama (libtorrent)
        else:
            download_aria2(task)        # default: aria2 (lebih cepat)
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
                    # opsional: batch pack -> unduh hanya file episode ini
                    "file_path": payload.get("file_path"),
                    # opsional: nomor episode → worker cari sendiri file-nya di pack
                    # (batch tanpa file_map, mis. hasil prowlarr)
                    "episode": payload.get("episode"),
                    # opsional: pilih track audio bahasa ini (mis. "jpn") via ffmpeg
                    "audio": payload.get("audio"),
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
