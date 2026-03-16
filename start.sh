#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
SERVICE_DIR="$ROOT_DIR/service"

cd "$ROOT_DIR"
git pull

cd "$SERVICE_DIR"

# 1) Stop and remove current containers

docker compose down --remove-orphans

# 2) (Optional) remove old image to force full rebuild
docker rmi akshare-stock-service:latest || true

# 3) Rebuild and start
docker compose up -d --build

echo "Service restarted successfully."
