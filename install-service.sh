#!/usr/bin/env bash
# install-service.sh — install dmr-decoder as a systemd service
set -euo pipefail

SERVICE="dmr-decoder"
UNIT="/etc/systemd/system/${SERVICE}.service"
REPO="$(cd "$(dirname "$0")" && pwd)"
PYTHON="$(command -v python3)"
RUN_USER="$(whoami)"

echo "[install] Repo:   $REPO"
echo "[install] Python: $PYTHON"
echo "[install] User:   $RUN_USER"

sudo tee "$UNIT" > /dev/null << EOF
[Unit]
Description=DMR Channel Decoder
Documentation=https://github.com/mostlychris/dmr_decoder
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${RUN_USER}
WorkingDirectory=${REPO}
ExecStart=${PYTHON} ${REPO}/dmr_decoder.py
Restart=always
RestartSec=10
StartLimitIntervalSec=60
StartLimitBurst=3
StandardOutput=journal
StandardError=journal
SyslogIdentifier=dmr-decoder

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable "$SERVICE"
sudo systemctl restart "$SERVICE"

echo ""
echo "[install] Done."
echo "  Status : sudo systemctl status $SERVICE"
echo "  Logs   : sudo journalctl -u $SERVICE -f"
echo "  Stop   : sudo systemctl stop $SERVICE"
echo "  Disable: sudo systemctl disable $SERVICE"
