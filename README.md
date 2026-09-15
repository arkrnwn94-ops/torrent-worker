# torrent-worker

Worker magnet→URL publik untuk remote upload Streamtape.

Jalankan di Codespace:

    pip install -r requirements.txt
    nohup python3 worker.py --port 8080 > /tmp/worker.log 2>&1 &

Endpoint:
- `GET /health`
- `POST /add` `{"magnet": "magnet:?xt=..."}` → `{"id": "..."}`
- `GET /status/<id>` → progress
- `GET /file/<id>/<nama-file>` → file video (support Range)
- `POST /delete/<id>` → hapus file lokal

Port 8080 dibuat otomatis public oleh devcontainer.
