#!/usr/bin/env python3
"""Detect moderated (removed) forum postings on derstandard.at.

Polls the GraphQL API, diffs snapshots, and logs removed postings to SQLite.

Usage:
    python derstandard_mod_detector.py [url1 ...] [--discover] [--interval 120] [--db moderated_postings.db]
"""

import argparse
import json
import re
import sqlite3
import sys
import threading
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler

API_URL = "https://capi.ds.at/forum-serve-graphql/v1/"
HASHES = {
    "GetForumInfo": "88adea55fbddc38bedd177c9107e457d5cf0f38d2fcd0976c8024dfa31779751",
    "ThreadsByForumQuery": "d5a2376ac61344341ffe4dcd17f814dba7ea80fbacfb5806c8d1f9c2072a3fa6",
}
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "content-type": "application/json",
    "Referer": "https://www.derstandard.at/",
    "Origin": "https://www.derstandard.at",
}

# Shared state for the web dashboard thread to read.
_shared = {"forums": {}, "snapshots": {}, "db_path": ""}


class _DashboardHandler(BaseHTTPRequestHandler):
    """Serves a minimal HTML status page."""

    def _send_html(self, html):
        data = html.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/":
            return self._handle_dashboard()
        if parsed.path == "/sql":
            return self._handle_sql(parsed)
        self.send_error(404)

    def _handle_sql(self, parsed):
        import html as html_mod
        qs = urllib.parse.parse_qs(parsed.query)
        query = qs.get("q", [""])[0].strip()
        db_path = _shared["db_path"]

        result_html = ""
        if query:
            safe_query = html_mod.escape(query)
            # Only allow read-only queries.
            if not query.lstrip().upper().startswith("SELECT"):
                result_html = '<p style="color:red">Only SELECT queries allowed.</p>'
            else:
                try:
                    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
                    cur = conn.execute(query)
                    cols = [d[0] for d in cur.description] if cur.description else []
                    rows = cur.fetchall()
                    conn.close()

                    if cols:
                        header = "".join(f"<th>{html_mod.escape(c)}</th>" for c in cols)
                        body = ""
                        for row in rows:
                            cells = "".join(
                                f"<td>{html_mod.escape(str(v))}</td>" for v in row
                            )
                            body += f"<tr>{cells}</tr>\n"
                        result_html = (
                            f'<p class="meta">{len(rows)} row(s)</p>'
                            f"<table><tr>{header}</tr>\n{body}</table>"
                        )
                    else:
                        result_html = '<p class="meta">No results.</p>'
                except Exception as e:
                    result_html = f'<p style="color:red">Error: {html_mod.escape(str(e))}</p>'
        else:
            safe_query = ""

        page = f"""<!doctype html>
<html><head>
<meta charset="utf-8">
<title>SQL — Mod Detector</title>
<style>
  body {{ font-family: system-ui, sans-serif; margin: 2rem; background: #f8f8f8; }}
  h1 {{ font-size: 1.3rem; }}
  table {{ border-collapse: collapse; width: 100%; background: #fff; }}
  th, td {{ text-align: left; padding: .4rem .6rem; border-bottom: 1px solid #ddd; font-size: .9rem; }}
  th {{ background: #333; color: #fff; }}
  .meta {{ color: #666; font-size: .85rem; }}
  textarea {{ width: 100%; font-family: monospace; font-size: .9rem; padding: .5rem; }}
  button {{ margin-top: .5rem; padding: .4rem 1rem; }}
  a {{ color: #0366d6; }}
</style>
</head><body>
<h1>SQL Query</h1>
<p class="meta"><a href="/">&larr; Dashboard</a></p>
<form method="get" action="/sql">
<textarea name="q" rows="3" placeholder="SELECT * FROM moderated_postings LIMIT 10">{safe_query}</textarea>
<br><button type="submit">Run</button>
</form>
{result_html}
</body></html>"""
        self._send_html(page)

    def _handle_dashboard(self):
        forums = _shared["forums"]
        snapshots = _shared["snapshots"]
        db_path = _shared["db_path"]

        # Query moderated counts and recent moderated postings from SQLite.
        mod_counts = {}
        recent_moderated = []
        try:
            conn = sqlite3.connect(db_path)
            for row in conn.execute(
                "SELECT article_url, COUNT(*) FROM moderated_postings GROUP BY article_url"
            ):
                mod_counts[row[0]] = row[1]
            for row in conn.execute(
                "SELECT article_url, posting_id, author, title, text, created_at, moderated_at"
                " FROM moderated_postings ORDER BY moderated_at DESC LIMIT 100"
            ):
                recent_moderated.append(row)
            conn.close()
        except Exception:
            pass

        import html as html_mod
        now = datetime.now(timezone.utc)
        rows = []
        url_titles = {}
        for forum_id, info in forums.items():
            url = info["url"]
            title = info.get("title", "")
            url_titles[url] = title
            postings = len(snapshots.get(forum_id, {}))
            moderated = mod_counts.get(url, 0)
            last = info.get("last_activity")
            if last is not None:
                delta = int((now - last).total_seconds())
                if delta < 60:
                    age = f"{delta}s ago"
                elif delta < 3600:
                    age = f"{delta // 60}m ago"
                else:
                    age = f"{delta // 3600}h {(delta % 3600) // 60}m ago"
            else:
                age = "\u2014"
            rows.append((url, title, postings, moderated, age))

        rows.sort(key=lambda r: r[3], reverse=True)

        table_rows = ""
        for url, title, postings, moderated, age in rows:
            label = html_mod.escape(title) if title else url
            table_rows += (
                f"<tr><td><a href=\"{url}\">{label}</a></td>"
                f"<td>{postings}</td><td>{moderated}</td><td>{age}</td></tr>\n"
            )

        recent_rows = ""
        for art_url, pid, author, title, text, created, moderated_at in recent_moderated:
            safe_author = html_mod.escape(author or "")
            safe_title = html_mod.escape(title or "")
            safe_text = html_mod.escape(text or "")
            art_label = html_mod.escape(url_titles.get(art_url, "")) or art_url
            # Show moderated_at as relative time
            try:
                mod_dt = datetime.fromisoformat(moderated_at)
                delta = int((now - mod_dt).total_seconds())
                if delta < 60:
                    mod_age = f"{delta}s ago"
                elif delta < 3600:
                    mod_age = f"{delta // 60}m ago"
                else:
                    mod_age = f"{delta // 3600}h {(delta % 3600) // 60}m ago"
            except (ValueError, TypeError):
                mod_age = moderated_at or "\u2014"
            recent_rows += (
                f"<tr><td>{safe_author}</td>"
                f"<td class=\"truncate\" title=\"{safe_title}\">{safe_title}</td>"
                f"<td class=\"truncate\" title=\"{safe_text}\">{safe_text}</td>"
                f"<td><a href=\"{art_url}\">{art_label}</a></td>"
                f"<td>{mod_age}</td></tr>\n"
            )

        html = f"""<!doctype html>
<html><head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="60">
<title>Mod Detector Dashboard</title>
<style>
  body {{ font-family: system-ui, sans-serif; margin: 2rem; background: #f8f8f8; }}
  h1 {{ font-size: 1.3rem; }}
  table {{ border-collapse: collapse; width: 100%; background: #fff; }}
  th, td {{ text-align: left; padding: .5rem .75rem; border-bottom: 1px solid #ddd; }}
  th {{ background: #333; color: #fff; }}
  a {{ color: #0366d6; }}
  .meta {{ color: #666; font-size: .85rem; margin-bottom: 1rem; }}
  h2 {{ font-size: 1.1rem; margin-top: 2rem; }}
  .truncate {{ max-width: 400px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
</style>
</head><body>
<h1>Mod Detector Dashboard</h1>
<p class="meta">Monitoring {len(forums)} forum(s) &middot; page refreshes every 60 s &middot; <a href="/sql">SQL</a></p>
<table>
<tr><th>Article</th><th>Postings</th><th>Moderated</th><th>Last Activity</th></tr>
{table_rows}</table>
<h2>Last 100 Filtered Posts</h2>
<table>
<tr><th>Author</th><th>Title</th><th>Text</th><th>Article</th><th>Filtered</th></tr>
{recent_rows}</table>
</body></html>"""

        self._send_html(html)

    def log_message(self, format, *args):
        # Suppress default stderr logging from BaseHTTPRequestHandler.
        pass


def start_web_server(port):
    """Start the dashboard HTTP server in a daemon thread."""
    server = HTTPServer(("0.0.0.0", port), _DashboardHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    log(f"Web dashboard listening on http://0.0.0.0:{port}/")


def api_call(op_name, variables):
    """Call the DerStandard forum GraphQL API using persisted queries."""
    params = urllib.parse.urlencode({
        "operationName": op_name,
        "variables": json.dumps(variables),
        "extensions": json.dumps({
            "persistedQuery": {"version": 1, "sha256Hash": HASHES[op_name]}
        }),
    })
    url = f"{API_URL}?{params}"
    headers = {**HEADERS, "x-apollo-operation-name": op_name}
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def normalize_url(url):
    """Extract the canonical story URL for the contextUri parameter."""
    # Accept full URLs or just the story path
    m = re.search(r"(https?://www\.derstandard\.at/story/\d+)", url)
    if m:
        return m.group(1)
    # Maybe just a story ID
    m = re.match(r"(\d{10,})", url)
    if m:
        return f"https://www.derstandard.at/story/{m.group(1)}"
    return url


def get_forum_info(article_url):
    """Get forum ID and posting count for an article."""
    result = api_call("GetForumInfo", {"contextUri": article_url})
    forum = result["data"]["getForumByContextUri"]
    if forum is None:
        return None, 0
    return forum["id"], forum["totalPostingCount"]


def collect_postings_from_node(node):
    """Recursively collect a posting and all its nested replies."""
    postings = {}
    postings[node["id"]] = {
        "id": node["id"],
        "author": node["author"]["name"],
        "title": node.get("title") or "",
        "text": node.get("text") or "",
        "created_at": node["history"]["created"],
        "root_posting_id": node.get("rootPostingId", ""),
    }
    for reply in node.get("replies", []):
        postings.update(collect_postings_from_node(reply))
    return postings


def fetch_all_postings(forum_id):
    """Fetch all postings for a forum, paginating through all pages."""
    all_postings = {}
    cursor = ""
    while True:
        result = api_call("ThreadsByForumQuery", {
            "id": forum_id,
            "sortOrder": "ByTime",
            "first": "Max",
            "nextCursor": cursor,
        })
        data = result["data"]["getForumRootPostingsV2"]
        for edge in data["edges"]:
            all_postings.update(collect_postings_from_node(edge["node"]))

        page_info = data["pageInfo"]
        if not page_info["hasNextPage"]:
            break
        cursor = page_info["nextCursor"]

    return all_postings


def fetch_rss_article_urls():
    """Fetch article URLs from the derstandard.at RSS feed.

    Returns a deduplicated list of (normalized_url, title) tuples matching /story/\\d+.
    """
    req = urllib.request.Request(
        "https://www.derstandard.at/rss",
        headers={"User-Agent": HEADERS["User-Agent"]},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        tree = ET.parse(resp)

    seen = set()
    results = []
    for item in tree.iter("item"):
        link_el = item.find("link")
        title_el = item.find("title")
        text = (link_el.text or "").strip() if link_el is not None else ""
        title = (title_el.text or "").strip() if title_el is not None else ""
        if re.search(r"/story/\d+", text):
            normalized = normalize_url(text)
            if normalized not in seen:
                seen.add(normalized)
                results.append((normalized, title))
    return results


def discover_articles(forums, min_posts):
    """Discover new articles from RSS that have active forums.

    Returns new entries {forum_id: {"url": url, "title": title, "last_activity": None}}
    for articles with >= min_posts postings that aren't already monitored.
    """
    try:
        rss_items = fetch_rss_article_urls()
    except Exception as e:
        log(f"  RSS fetch failed: {e}")
        return {}

    log(f"  RSS: found {len(rss_items)} article URLs")

    known_urls = {info["url"] for info in forums.values()}
    new_entries = {}

    for url, title in rss_items:
        if url in known_urls:
            continue
        try:
            forum_id, count = get_forum_info(url)
        except Exception as e:
            log(f"  Error checking {url}: {e}")
            continue

        if forum_id is None:
            log(f"  {url}: no forum, skipping")
            continue

        if forum_id in forums:
            continue

        if count < min_posts:
            log(f"  {url}: {count} postings (< {min_posts}), skipping")
            continue

        log(f"  Discovered: {url} (forum {forum_id}, {count} postings)")
        new_entries[forum_id] = {"url": url, "title": title, "last_activity": None}

    return new_entries


def parse_created_at(ts):
    """Parse an ISO timestamp string to a timezone-aware datetime."""
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def newest_posting_time(postings):
    """Return the newest created_at datetime from a dict of postings, or None."""
    newest = None
    for p in postings.values():
        try:
            dt = parse_created_at(p["created_at"])
        except (ValueError, KeyError):
            continue
        if newest is None or dt > newest:
            newest = dt
    return newest


def init_db(db_path):
    """Initialize the SQLite database."""
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS moderated_postings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            forum_id TEXT NOT NULL,
            article_url TEXT NOT NULL,
            posting_id TEXT NOT NULL,
            author TEXT,
            title TEXT,
            text TEXT,
            created_at TEXT,
            moderated_at TEXT NOT NULL,
            UNIQUE(forum_id, posting_id)
        )
    """)
    conn.commit()
    return conn


def save_moderated(conn, forum_id, article_url, posting):
    """Save a moderated posting to the database."""
    now = datetime.now(timezone.utc).isoformat()
    try:
        conn.execute(
            """INSERT OR IGNORE INTO moderated_postings
               (forum_id, article_url, posting_id, author, title, text, created_at, moderated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (forum_id, article_url, posting["id"], posting["author"],
             posting["title"], posting["text"], posting["created_at"], now),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Detect moderated postings on derstandard.at")
    parser.add_argument("urls", nargs="*", default=[], help="Article URLs to monitor")
    parser.add_argument("--interval", type=int, default=120, help="Poll interval in seconds (default: 120)")
    parser.add_argument("--db", default="data/moderated_postings.db", help="SQLite database path")
    parser.add_argument("--discover", action="store_true", help="Enable RSS auto-discovery of articles")
    parser.add_argument("--min-posts", type=int, default=50, help="Minimum postings to monitor a discovered article (default: 50)")
    parser.add_argument("--max-inactive", type=int, default=60, help="Drop forums with no new post in this many minutes (default: 60)")
    parser.add_argument("--discover-interval", type=int, default=5, help="Run discovery every Nth poll cycle (default: 5)")
    parser.add_argument("--web-port", type=int, default=8080, help="Web dashboard port (default: 8080, 0 to disable)")
    args = parser.parse_args()

    if not args.urls and not args.discover:
        parser.error("must provide at least one URL or use --discover")

    conn = init_db(args.db)

    if args.web_port > 0:
        _shared["db_path"] = args.db
        start_web_server(args.web_port)

    # forums: forum_id -> {"url": article_url, "last_activity": datetime|None}
    forums = {}

    # Resolve forum IDs for CLI URLs
    for url in args.urls:
        article_url = normalize_url(url)
        log(f"Resolving forum for {article_url}")
        try:
            forum_id, count = get_forum_info(article_url)
        except Exception as e:
            log(f"  Error: {e}")
            continue
        if forum_id is None:
            log(f"  No forum found, skipping")
            continue
        forums[forum_id] = {"url": article_url, "title": "", "last_activity": None}
        log(f"  Forum {forum_id} ({count} postings)")

    # Initial discovery
    if args.discover:
        log("Running initial RSS discovery...")
        new = discover_articles(forums, args.min_posts)
        forums.update(new)
        log(f"  {len(new)} new forum(s) discovered")

    if not forums:
        if args.discover:
            log("No forums found yet. Will keep trying via discovery.")
        else:
            print("No valid forums found. Exiting.", file=sys.stderr)
            sys.exit(1)

    # In-memory snapshots: forum_id -> {posting_id -> posting_data}
    snapshots = {}
    # Full posting data cache: posting_id -> posting_data
    posting_cache = {}

    cycle = 0
    while True:
        cycle += 1
        log(f"--- Poll cycle {cycle} ---")

        # Periodic discovery
        if args.discover and cycle > 1 and (cycle % args.discover_interval == 0):
            log("Running RSS discovery...")
            new = discover_articles(forums, args.min_posts)
            forums.update(new)
            if new:
                log(f"  {len(new)} new forum(s) discovered")

        for forum_id in list(forums):
            article_url = forums[forum_id]["url"]
            try:
                current = fetch_all_postings(forum_id)
            except Exception as e:
                log(f"  Error fetching {forum_id}: {e}")
                continue

            current_ids = set(current.keys())
            log(f"  {article_url}: {len(current_ids)} postings")

            # Update last_activity from newest posting
            newest = newest_posting_time(current)
            if newest is not None:
                forums[forum_id]["last_activity"] = newest

            # Update cache with latest data
            posting_cache.update(current)

            if forum_id in snapshots:
                previous_ids = set(snapshots[forum_id].keys())
                removed = previous_ids - current_ids
                added = current_ids - previous_ids

                if added:
                    log(f"  +{len(added)} new postings")

                if removed:
                    log(f"  -{len(removed)} MODERATED postings:")
                    for pid in removed:
                        posting = posting_cache.get(pid, {
                            "id": pid, "author": "?", "title": "?",
                            "text": "?", "created_at": "?",
                        })
                        saved = save_moderated(conn, forum_id, article_url, posting)
                        status = "saved" if saved else "already known"
                        log(f"    {pid} by {posting['author']}: {posting['title'][:50]} ({status})")
                elif not added:
                    log(f"  No changes")

            else:
                log(f"  Initial snapshot captured")

            snapshots[forum_id] = current

        # Update shared state for the web dashboard.
        _shared["forums"] = dict(forums)
        _shared["snapshots"] = dict(snapshots)

        # Cleanup inactive forums
        if args.max_inactive > 0:
            now = datetime.now(timezone.utc)
            stale = []
            for forum_id, info in forums.items():
                if info["last_activity"] is None:
                    continue
                age_minutes = (now - info["last_activity"]).total_seconds() / 60
                if age_minutes > args.max_inactive:
                    stale.append(forum_id)

            for forum_id in stale:
                info = forums.pop(forum_id)
                snapshots.pop(forum_id, None)
                age = int((now - info["last_activity"]).total_seconds() / 60)
                log(f"  Cleanup: dropped {info['url']} (forum {forum_id}, inactive {age}m)")

        if not forums:
            log("No forums being monitored. Waiting for next discovery cycle...")

        if cycle == 1:
            log(f"Monitoring {len(forums)} forum(s). Polling every {args.interval}s. Ctrl+C to stop.")

        try:
            time.sleep(args.interval)
        except KeyboardInterrupt:
            log("Interrupted. Exiting.")
            break

    conn.close()


if __name__ == "__main__":
    main()
