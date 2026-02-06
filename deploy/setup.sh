#!/usr/bin/env bash
# Bootstrap script for deploying derstandard-mod-detector on a Debian/Ubuntu VM.
# Run as root (or with sudo).
set -euo pipefail

INSTALL_DIR="/opt/derstandard-mod-detector"
REPO_URL="https://github.com/citizenkeynes/derstandard-filter-monitor.git"
SERVICE_USER="moddetector"

echo "==> Installing system packages"
apt-get update -qq
apt-get install -y -qq python3 python3-venv git

echo "==> Creating service user"
if ! id "$SERVICE_USER" &>/dev/null; then
    useradd --system --no-create-home --shell /usr/sbin/nologin "$SERVICE_USER"
fi

echo "==> Cloning repository"
if [ -d "$INSTALL_DIR" ]; then
    echo "    $INSTALL_DIR already exists — pulling latest"
    git -C "$INSTALL_DIR" pull --ff-only
else
    git clone "$REPO_URL" "$INSTALL_DIR"
fi

echo "==> Creating Python venv"
python3 -m venv "$INSTALL_DIR/venv"
# No pip install needed — stdlib only

echo "==> Setting up data directory"
mkdir -p "$INSTALL_DIR/data"
chown -R "$SERVICE_USER":"$SERVICE_USER" "$INSTALL_DIR/data"

echo "==> Installing systemd unit"
cp "$INSTALL_DIR/deploy/derstandard-mod-detector.service" /etc/systemd/system/
systemctl daemon-reload

echo "==> Creating config file (if missing)"
CONF="/etc/derstandard-mod-detector.conf"
if [ ! -f "$CONF" ]; then
    cat > "$CONF" <<'CONF_EOF'
# Optional extra URLs to always monitor (space-separated).
EXTRA_URLS=

# Minimum postings to auto-monitor a discovered article.
MIN_POSTS=50

# Drop forums with no new post in this many minutes.
MAX_INACTIVE=60

# Poll interval in seconds.
POLL_INTERVAL=120

# Run RSS discovery every Nth poll cycle.
DISCOVER_INTERVAL=5
CONF_EOF
    echo "    Created $CONF"
else
    echo "    $CONF already exists, skipping."
fi

echo "==> Enabling service (will start on next boot)"
systemctl enable derstandard-mod-detector

echo ""
echo "Done! Next steps:"
echo "  1. Optionally edit /etc/derstandard-mod-detector.conf to tune settings."
echo "  2. sudo systemctl start derstandard-mod-detector"
echo "  3. sudo journalctl -fu derstandard-mod-detector"
