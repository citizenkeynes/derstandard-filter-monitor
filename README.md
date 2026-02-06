# derstandard-mod-detector

Monitors derstandard.at article forums for moderated (removed) postings.

Polls the forum GraphQL API, diffs snapshots between cycles, and logs any postings that disappear to a local SQLite database.

## How it works

1. Fetches all postings for each monitored article forum.
2. Compares the current snapshot to the previous one.
3. Postings present before but missing now are flagged as **moderated** and saved to SQLite with the full posting text, author, and timestamp.

## Local usage

Monitor specific articles:

```bash
python src/derstandard_mod_detector.py \
    "https://www.derstandard.at/story/3000000248089" \
    --interval 120 \
    --db data/moderated_postings.db
```

### Auto-discovery mode

Use `--discover` to automatically find articles with active forums from the RSS feed:

```bash
python src/derstandard_mod_detector.py --discover --interval 120
```

You can combine explicit URLs with discovery:

```bash
python src/derstandard_mod_detector.py \
    "https://www.derstandard.at/story/3000000248089" \
    --discover \
    --min-posts 50 \
    --max-inactive 60 \
    --discover-interval 5
```

| Flag | Default | Description |
|------|---------|-------------|
| `--discover` | off | Enable RSS auto-discovery of articles |
| `--min-posts N` | 50 | Only monitor discovered articles with at least N postings |
| `--max-inactive M` | 60 | Drop forums with no new posting in the last M minutes |
| `--discover-interval K` | 5 | Re-run RSS discovery every Kth poll cycle |
| `--interval S` | 120 | Poll interval in seconds |
| `--db PATH` | `data/moderated_postings.db` | SQLite database path |

Run `--help` for all options:

```bash
python src/derstandard_mod_detector.py --help
```

## Deploy to GCP (e2-micro, free tier)

### 1. Create the VM

```bash
gcloud compute instances create derstandard-mod-detector \
    --zone=us-central1-a \
    --machine-type=e2-micro \
    --image-family=debian-12 \
    --image-project=debian-cloud \
    --boot-disk-size=30GB
```

### 2. SSH in and bootstrap

```bash
gcloud compute ssh derstandard-mod-detector --zone=us-central1-a
```

Then on the VM:

```bash
sudo git clone https://github.com/YOUR_USER/derstandard-mod-detector.git /opt/derstandard-mod-detector
sudo bash /opt/derstandard-mod-detector/deploy/setup.sh
```

### 3. Configure

Edit the config file:

```bash
sudo nano /etc/derstandard-mod-detector.conf
```

```ini
# Optional extra URLs to always monitor (space-separated)
EXTRA_URLS=
MIN_POSTS=50
MAX_INACTIVE=60
POLL_INTERVAL=120
DISCOVER_INTERVAL=5
```

### 4. Start

```bash
sudo systemctl start derstandard-mod-detector
```

### 5. Check logs

```bash
sudo journalctl -fu derstandard-mod-detector
```

## Project structure

```
src/                              Main script
deploy/setup.sh                   VM bootstrap (installs deps, sets up systemd)
deploy/derstandard-mod-detector.service   systemd unit file
data/                             SQLite database lives here at runtime
```
