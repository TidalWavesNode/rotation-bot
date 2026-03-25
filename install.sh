#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$ROOT_DIR/.venv"
SERVICE_NAME="rotation-bot"
SERVICE_PATH="/etc/systemd/system/${SERVICE_NAME}.service"
RUN_SH="$ROOT_DIR/run.sh"
ENV_FILE="$ROOT_DIR/.env"

sudo_maybe() {
  if [[ $EUID -eq 0 ]]; then "$@"; else sudo "$@"; fi
}

echo "========================================================================"
echo "Rotation Bot Installer"
echo "========================================================================"
echo

echo "This installer will:"
echo "  1. Install only the required system packages"
echo "  2. Create the Python virtual environment"
echo "  3. Walk you through wallet + bot setup"
echo "  4. Create the systemd service"
echo "  5. Start the bot automatically"
echo "  6. Open the dashboard"
echo

echo "Installing OS prerequisites..."
sudo_maybe apt-get update
sudo_maybe apt-get install -y python3 python3-venv python3-pip

echo
echo "Creating virtual environment..."
python3 -m venv "$VENV_DIR"
source "$VENV_DIR/bin/activate"
pip install --upgrade pip setuptools wheel
pip install -r "$ROOT_DIR/requirements.txt"

echo
echo "Running guided setup..."
python "$ROOT_DIR/bootstrap.py"

echo
echo "Creating dashboard launcher..."
cat > "$ROOT_DIR/rotationctl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$ROOT_DIR/.venv/bin/activate"
if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  source "$ROOT_DIR/.env"
  set +a
fi
exec python "$ROOT_DIR/rotation_menu.py"
EOF
chmod +x "$ROOT_DIR/rotationctl"

echo
echo "Creating systemd service..."
sudo_maybe bash -c "cat > '$SERVICE_PATH'" <<EOF
[Unit]
Description=Rotation Bot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$ROOT_DIR
Environment=PYTHONUNBUFFERED=1
EnvironmentFile=-$ENV_FILE
ExecStart=$RUN_SH
Restart=on-failure
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF

sudo_maybe systemctl daemon-reload
sudo_maybe systemctl enable "$SERVICE_NAME"
sudo_maybe systemctl restart "$SERVICE_NAME"

echo
echo "Waiting for the service to start..."
sleep 3
sudo_maybe systemctl --no-pager --full status "$SERVICE_NAME" || true

echo
echo "Setup complete. Opening the dashboard..."
exec "$ROOT_DIR/rotationctl"
