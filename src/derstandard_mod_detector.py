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
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

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

    Returns a deduplicated list of normalized article URLs matching /story/\\d+.
    """
    req = urllib.request.Request(
        "https://www.derstandard.at/rss",
        headers={"User-Agent": HEADERS["User-Agent"]},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        tree = ET.parse(resp)

    seen = set()
    urls = []
    for link_el in tree.iter("link"):
        text = (link_el.text or "").strip()
        if re.search(r"/story/\d+", text):
            normalized = normalize_url(text)
            if normalized not in seen:
                seen.add(normalized)
                urls.append(normalized)
    return urls


def discover_articles(forums, min_posts):
    """Discover new articles from RSS that have active forums.

    Returns new entries {forum_id: {"url": url, "last_activity": None}}
    for articles with >= min_posts postings that aren't already monitored.
    """
    try:
        rss_urls = fetch_rss_article_urls()
    except Exception as e:
        log(f"  RSS fetch failed: {e}")
        return {}

    log(f"  RSS: found {len(rss_urls)} article URLs")

    known_urls = {info["url"] for info in forums.values()}
    new_entries = {}

    for url in rss_urls:
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
        new_entries[forum_id] = {"url": url, "last_activity": None}

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
    args = parser.parse_args()

    if not args.urls and not args.discover:
        parser.error("must provide at least one URL or use --discover")

    conn = init_db(args.db)

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
        forums[forum_id] = {"url": article_url, "last_activity": None}
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
