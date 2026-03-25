#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$ROOT_DIR/.venv"
if [[ ! -d "$VENV_DIR" ]]; then
  echo "Virtualenv not found. Run ./install.sh first."
  exit 1
fi
source "$VENV_DIR/bin/activate"
if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  source "$ROOT_DIR/.env"
  set +a
fi
export ROTATION_DB_PATH="${ROTATION_DB_PATH:-$ROOT_DIR/rotation_bot.db}"
export ROTATION_NONINTERACTIVE="${ROTATION_NONINTERACTIVE:-1}"
exec python "$ROOT_DIR/rotation_bot.py"
