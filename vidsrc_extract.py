#!/usr/bin/env python3
"""vidsrc_extract.py — resolve vidsrc.sh embed → HLS master m3u8 (+ IP-bound token).

Chain (terverifikasi 2026-09-20):
  1. GET  https://vidsrc.sh/embed/{type}/{imdb}[?season=&episode=]
        → HTML SPA, atribut  data-api="/vs_src.php?type=..&id=.."  (HTML-entity &amp;)
  2. GET  https://vidsrc.sh/vs_src.php?type=..&id=..   (Referer embed WAJIB)
        → {"src":"https://cloudorchestranova.com/embed/..?vs=<token>"}
  3. GET  <src>            → HTML dgn window.CFG.playerUrl (/embed/player/..?vs=..)
  4. GET  playerUrl        → HTML dgn window.CONFIG.api = data.vidsrc.sh/api.php?..&stream_urls
  5. GET  api (&stream_urls)  → JSON:
        data.stream_urls : string base64 (ChaCha20 nonce||ct)  ATAU array (plain)
        vs.wasm_url      : modul WASM ChaCha20 per-window (key tertanam)
  6. WASM decrypt(stream_urls) → daftar URL master.m3u8 (host: comityofcognomen.site dst)
  7. GET  <origin>/generate.php  → JWT token IP-bound (exp ~4 jam), tempel ?token=<jwt>

Token di step 7 TERIKAT ke IP pemanggil (payload ip_cidr). Karena itu ekstraksi
+ unduh HLS HARUS jalan di mesin yang sama yang akan menyajikan file (codespace),
bukan di lokal lalu URL dikirim ke Streamtape (IP beda → 401 "no token").

Dipakai oleh worker.py (endpoint /add_vidsrc). Bisa juga standalone:
    python3 vidsrc_extract.py --imdb tt0137523            # movie
    python3 vidsrc_extract.py --imdb tt0903747 --season 1 --episode 1   # tv
"""
import argparse
import base64
import html
import json
import os
import re
import subprocess
import sys
import tempfile
from urllib.parse import urljoin, urlparse

import requests

VIDSRC = "https://vidsrc.sh"
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")


def _get(url, referer=None, timeout=30):
    h = {"User-Agent": UA, "Accept": "*/*"}
    if referer:
        h["Referer"] = referer
        h["Origin"] = referer.rstrip("/")
        # Origin harus scheme+host saja
        p = urlparse(referer)
        h["Origin"] = f"{p.scheme}://{p.netloc}"
    r = requests.get(url, headers=h, timeout=timeout)
    return r


def _decrypt_wasm(wasm_bytes, enc_b64):
    """Jalankan modul WASM ChaCha20 (alloc/decrypt/memory) seperti vsdec.js:
    alloc(len) → tulis enc → decrypt(ptr,len) → decode dari (ptr+12, outLen).
    Pakai wasmtime kalau ada, jika tidak fallback ke node."""
    try:
        import wasmtime  # type: ignore
        store = wasmtime.Store()
        module = wasmtime.Module(store.engine, wasm_bytes)
        inst = wasmtime.Instance(store, module, [])
        ex = inst.exports(store)
        mem = ex["memory"]
        alloc = ex["alloc"]
        decrypt = ex["decrypt"]
        enc = base64.b64decode(enc_b64)
        ptr = alloc(store, len(enc))
        mem.write(store, enc, ptr)
        out_len = decrypt(store, ptr, len(enc))
        data = mem.read(store, ptr + 12, ptr + 12 + out_len)
        return data.decode("utf-8", "replace")
    except ImportError:
        pass  # fallback node
    # --- fallback: node ---
    with tempfile.TemporaryDirectory() as td:
        wp = os.path.join(td, "d.wasm")
        with open(wp, "wb") as f:
            f.write(wasm_bytes)
        script = (
            'const fs=require("fs");'
            'const wasm=fs.readFileSync(process.argv[1]);'
            'const enc=Buffer.from(process.argv[2],"base64");'
            '(async()=>{const m=await WebAssembly.compile(wasm);'
            'const i=await WebAssembly.instantiate(m,{});const e=i.exports;'
            'const u=new Uint8Array(enc);const p=e.alloc(u.length);'
            'new Uint8Array(e.memory.buffer,p,u.length).set(u);'
            'const n=e.decrypt(p,u.length);'
            'process.stdout.write(new TextDecoder().decode('
            'new Uint8Array(e.memory.buffer,p+12,n)));})()'
            '.catch(e=>{console.error(e);process.exit(1)});'
        )
        r = subprocess.run(["node", "-e", script, wp, enc_b64],
                           capture_output=True, text=True, timeout=60)
        if r.returncode != 0:
            raise RuntimeError(f"node wasm decrypt gagal: {r.stderr.strip()}")
        return r.stdout


def get_master_urls(imdb, mtype="movie", season=None, episode=None):
    """Kembalikan (master_urls[list], embed_url). URL master.m3u8 BELUM ber-token."""
    # step 1: embed
    if mtype == "movie":
        embed = f"{VIDSRC}/embed/movie/{imdb}"
    else:
        embed = f"{VIDSRC}/embed/tv/{imdb}"
        if season and episode:
            embed += f"/{season}/{episode}"
    r = _get(embed)
    if r.status_code != 200:
        raise RuntimeError(f"embed {r.status_code}")
    m = re.search(r'data-api=["\']([^"\']+)["\']', r.text)
    if not m:
        raise RuntimeError("data-api tidak ditemukan di embed")
    api_path = html.unescape(m.group(1))  # &amp; → &

    # step 2: vs_src.php (Referer embed wajib)
    r = _get(urljoin(VIDSRC, api_path), referer=embed)
    src = (r.json() or {}).get("src")
    if not src:
        raise RuntimeError(f"vs_src.php tak ada src: {r.text[:200]}")

    # step 3: cloudorchestranova embed → CFG.playerUrl
    r = _get(src, referer=VIDSRC + "/")
    m = re.search(r'"playerUrl"\s*:\s*"([^"]+)"', r.text)
    if not m:
        raise RuntimeError("playerUrl tidak ditemukan")
    origin_co = f"{urlparse(src).scheme}://{urlparse(src).netloc}"
    player_url = urljoin(origin_co + "/", m.group(1).replace("\\/", "/"))

    # step 4: player frame → CONFIG.api
    r = _get(player_url, referer=origin_co + "/")
    m = re.search(r'"api"\s*:\s*"([^"]+)"', r.text)
    if not m:
        raise RuntimeError("CONFIG.api tidak ditemukan")
    stream_api = m.group(1).encode().decode("unicode_escape")  # & → &

    # step 5: api stream_urls + vs.wasm_url
    r = _get(stream_api, referer=origin_co + "/")
    j = r.json()
    data = j.get("data") or {}
    su = data.get("stream_urls")
    if su is None:
        raise RuntimeError(f"stream_urls kosong: {json.dumps(j)[:200]}")
    if isinstance(su, list):
        return su, embed  # plain (tanpa proteksi)
    vs = j.get("vs") or {}
    wasm_url = vs.get("wasm_url")
    if wasm_url:
        wb = _get(wasm_url, referer=origin_co + "/").content
    elif vs.get("wasm"):
        wb = base64.b64decode(vs["wasm"])
    else:
        raise RuntimeError("vs.wasm(_url) tidak ada — tak bisa decrypt")

    # step 6: WASM decrypt
    out = _decrypt_wasm(wb, su)
    urls = [u.strip() for u in out.splitlines() if u.strip().startswith("http")]
    if not urls:
        raise RuntimeError(f"decrypt tak hasilkan URL: {out[:200]}")
    return urls, embed


def fetch_token(origin, referer):
    """Ambil JWT IP-bound dari <origin>/generate.php. Kembalikan string token."""
    r = _get(origin + "/generate.php", referer=referer)
    txt = r.text.strip()
    try:
        j = json.loads(txt)
        return j.get("token") or j.get("data") or j.get("string") or j.get("result") or ""
    except json.JSONDecodeError:
        return txt


def apply_token(url, token):
    if not token:
        return url
    if "__TOKEN__" in url:
        return url.replace("__TOKEN__", token)
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}token={token}"


def resolve(imdb, mtype="movie", season=None, episode=None):
    """Full resolve → dict siap-pakai untuk ffmpeg:
       {master_url (ber-token), referer, headers, all_variants}."""
    urls, embed = get_master_urls(imdb, mtype, season, episode)
    master = urls[0]
    origin = f"{urlparse(master).scheme}://{urlparse(master).netloc}"
    # referer untuk stream = player origin (cloudorchestranova). Pakai vidsrc jg diterima.
    ref = "https://cloudorchestranova.com/"
    token = fetch_token(origin, ref)
    stamped = apply_token(master, token)
    return {
        "master_url": stamped,
        "master_raw": master,
        "all_variants": urls,
        "referer": ref,
        "origin": origin,
        "token": token,
        "embed": embed,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--imdb", required=True)
    ap.add_argument("--type", default="movie", choices=["movie", "tv"])
    ap.add_argument("--season", type=int)
    ap.add_argument("--episode", type=int)
    args = ap.parse_args()
    info = resolve(args.imdb, args.type, args.season, args.episode)
    print(json.dumps(info, indent=2))


if __name__ == "__main__":
    main()
