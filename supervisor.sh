#!/bin/bash
# Penjaga worker: kalau worker.py mati (OOM dsb), nyalakan lagi dalam 30 dtk.
# Dijalankan sekali via: setsid nohup bash supervisor.sh > /dev/null 2>&1 &
while true; do
  pgrep -f 'python3 worker.py' >/dev/null || {
    cd /workspaces/torrent-worker || exit 1
    setsid nohup python3 worker.py --port 8080 >> /tmp/worker.log 2>&1 < /dev/null
    echo "$(date '+%H:%M:%S') supervisor: worker direstart" >> /tmp/worker.log
  }
  sleep 30
done
