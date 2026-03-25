#!/usr/bin/env bash
set -euo pipefail
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BRANCH="main"
SERVICE_NAME="rotation-bot"
VENV_DIR="$REPO_DIR/.venv"
ENV_FILE="$REPO_DIR/.env"
DB_FILE="${ROTATION_DB_PATH:-$REPO_DIR/rotation_bot.db}"
cd "$REPO_DIR"

echo "Stopping service..."
sudo systemctl stop "$SERVICE_NAME" || true

echo "Updating repo from origin/$BRANCH..."
git fetch origin "$BRANCH"
git checkout "$BRANCH"
git pull --ff-only origin "$BRANCH"

chmod +x install.sh run.sh rotationctl update.sh || true

if [[ ! -d "$VENV_DIR" ]]; then
  echo "Creating virtualenv..."
  python3 -m venv "$VENV_DIR"
fi

source "$VENV_DIR/bin/activate"
pip install --upgrade pip setuptools wheel
pip install -r "$REPO_DIR/requirements.txt"

if [[ -f "$ENV_FILE" ]]; then
  echo "Preserving existing .env at $ENV_FILE"
else
  echo "Warning: .env not found; service will rely on defaults until setup is run."
fi

if [[ -f "$DB_FILE" ]]; then
  echo "Preserving existing database at $DB_FILE"
else
  echo "Warning: database file not found at $DB_FILE"
fi

sudo systemctl daemon-reload
sudo systemctl restart "$SERVICE_NAME"
sudo systemctl status "$SERVICE_NAME" --no-pager || true
sudo journalctl -u "$SERVICE_NAME" -n 80 --no-pager || true
