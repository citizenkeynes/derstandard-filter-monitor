#!/usr/bin/env python3
"""Detect moderated (removed) forum postings on derstandard.at.

Polls the GraphQL API, diffs snapshots, and logs removed postings to SQLite.

Usage:
    python derstandard_mod_detector.py [url1 ...] [--discover] [--interval 120] [--db moderated_postings.db]
"""

import argparse
import base64
import json
import os
import re
import sqlite3
import sys
import threading
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
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

# Track when we last posted to Reddit (persisted in DB to survive restarts).


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
        if parsed.path == "/article":
            return self._handle_article(parsed)
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
  #history {{ margin-top: 1rem; }}
  #history summary {{ cursor: pointer; color: #666; font-size: .85rem; }}
  #history ul {{ list-style: none; padding: 0; margin: .5rem 0; }}
  #history li {{ font-family: monospace; font-size: .85rem; padding: .25rem .4rem; cursor: pointer; border-bottom: 1px solid #eee; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
  #history li:hover {{ background: #e8f0fe; }}
</style>
</head><body>
<h1>SQL Query</h1>
<p class="meta"><a href="/">&larr; Dashboard</a></p>
<form id="sqlform" method="get" action="/sql">
<textarea id="querybox" name="q" rows="3" placeholder="SELECT * FROM moderated_postings LIMIT 10">{safe_query}</textarea>
<br><button type="submit">Run</button>
</form>
<details id="history"><summary>Query history</summary><ul id="hlist"></ul></details>
{result_html}
<script>
(function() {{
  var KEY = "sql_history", MAX = 30;
  var h = JSON.parse(localStorage.getItem(KEY) || "[]");
  var q = document.getElementById("querybox").value.trim();
  if (q) {{
    h = h.filter(function(x) {{ return x !== q; }});
    h.unshift(q);
    if (h.length > MAX) h = h.slice(0, MAX);
    localStorage.setItem(KEY, JSON.stringify(h));
  }}
  var ul = document.getElementById("hlist");
  h.forEach(function(item) {{
    var li = document.createElement("li");
    li.textContent = item;
    li.onclick = function() {{
      document.getElementById("querybox").value = item;
      document.getElementById("sqlform").submit();
    }};
    ul.appendChild(li);
  }});
  if (h.length === 0) document.getElementById("history").style.display = "none";
}})();
</script>
</body></html>"""
        self._send_html(page)

    def _handle_article(self, parsed):
        import html as html_mod
        qs = urllib.parse.parse_qs(parsed.query)
        article_url = qs.get("url", [""])[0]
        if not article_url:
            self.send_error(400, "Missing url parameter")
            return

        db_path = _shared["db_path"]
        now = datetime.now(timezone.utc)

        # Fetch all postings for this article, grouped by thread, threads ordered
        # by max moderated_at desc, postings within thread by created_at asc.
        threads = {}  # thread_id -> {"max_mod": str, "posts": []}
        article_title = ""
        try:
            conn = sqlite3.connect(db_path)
            for row in conn.execute(
                "SELECT posting_id, author, title, text, created_at, moderated_at,"
                " is_reply, upvotes, downvotes, thread_id, article_title,"
                " parent_author, parent_title, parent_text"
                " FROM moderated_postings WHERE article_url = ?"
                " ORDER BY created_at ASC",
                (article_url,),
            ):
                (pid, author, title, text, created, mod_at, is_reply,
                 up, down, tid, art_title, p_author, p_title, p_text) = row
                if art_title:
                    article_title = art_title
                if tid not in threads:
                    threads[tid] = {"max_mod": mod_at, "posts": []}
                if mod_at > threads[tid]["max_mod"]:
                    threads[tid]["max_mod"] = mod_at
                threads[tid]["posts"].append({
                    "pid": pid, "author": author, "title": title, "text": text,
                    "created": created, "mod_at": mod_at, "is_reply": is_reply,
                    "up": up, "down": down, "p_author": p_author,
                    "p_title": p_title, "p_text": p_text,
                })
            conn.close()
        except Exception:
            pass

        # Sort threads by max moderation timestamp descending.
        sorted_threads = sorted(threads.items(), key=lambda t: t[1]["max_mod"], reverse=True)

        total = sum(len(t["posts"]) for _, t in sorted_threads)
        safe_title = html_mod.escape(article_title) if article_title else html_mod.escape(article_url)

        def _relative(ts):
            try:
                dt = datetime.fromisoformat(ts)
                delta = int((now - dt).total_seconds())
                if delta < 60:
                    return f"{delta}s ago"
                if delta < 3600:
                    return f"{delta // 60}m ago"
                if delta < 86400:
                    return f"{delta // 3600}h {(delta % 3600) // 60}m ago"
                return f"{delta // 86400}d ago"
            except (ValueError, TypeError):
                return ts or "\u2014"

        def _abs_time(ts):
            try:
                dt = datetime.fromisoformat(ts)
                return dt.strftime("%Y-%m-%d %H:%M")
            except (ValueError, TypeError):
                return ts or ""

        thread_html = ""
        for tid, tdata in sorted_threads:
            posts = tdata["posts"]
            thread_html += f'<div class="thread">'
            thread_html += f'<div class="thread-hdr">Thread {html_mod.escape(tid)} &mdash; {len(posts)} filtered post(s), latest filtered {_relative(tdata["max_mod"])}</div>'
            for p in posts:
                safe_author = html_mod.escape(p["author"] or "")
                safe_ptitle = html_mod.escape(p["title"] or "")
                safe_text = html_mod.escape(p["text"] or "")
                reply_cls = " reply" if p["is_reply"] else ""
                parent_html = ""
                if p["is_reply"] and (p["p_author"] or p["p_text"]):
                    pa = html_mod.escape(p["p_author"] or "?")
                    pt = html_mod.escape(p["p_text"] or "")
                    parent_html = f'<div class="parent">replying to <b>{pa}</b>: {pt}</div>'
                thread_html += (
                    f'<div class="post{reply_cls}">'
                    f'{parent_html}'
                    f'<div class="post-meta"><b>{safe_author}</b> &middot; '
                    f'{_abs_time(p["created"])} &middot; '
                    f'+{p["up"]}/&minus;{p["down"]} &middot; '
                    f'filtered {_relative(p["mod_at"])}</div>'
                )
                if safe_ptitle:
                    thread_html += f'<div class="post-title">{safe_ptitle}</div>'
                thread_html += f'<div class="post-text">{safe_text}</div></div>'
            thread_html += '</div>'

        page = f"""<!doctype html>
<html><head>
<meta charset="utf-8">
<title>{safe_title} — Mod Detector</title>
<style>
  body {{ font-family: system-ui, sans-serif; margin: 2rem; background: #f8f8f8; }}
  h1 {{ font-size: 1.3rem; }}
  a {{ color: #0366d6; }}
  .meta {{ color: #666; font-size: .85rem; margin-bottom: 1rem; }}
  .thread {{ background: #fff; border: 1px solid #ddd; border-radius: 6px; margin-bottom: 1.2rem; overflow: hidden; }}
  .thread-hdr {{ background: #333; color: #fff; padding: .5rem .75rem; font-size: .85rem; }}
  .post {{ padding: .6rem .75rem; border-bottom: 1px solid #eee; }}
  .post:last-child {{ border-bottom: none; }}
  .post.reply {{ padding-left: 2rem; }}
  .post-meta {{ font-size: .8rem; color: #666; margin-bottom: .25rem; }}
  .post-title {{ font-weight: 600; margin-bottom: .15rem; }}
  .post-text {{ font-size: .95rem; white-space: pre-wrap; }}
  .parent {{ font-size: .8rem; color: #888; border-left: 3px solid #ddd; padding: .25rem .5rem; margin-bottom: .4rem; background: #fafafa; }}
</style>
</head><body>
<h1>{safe_title}</h1>
<p class="meta"><a href="/">&larr; Dashboard</a> &middot; <a href="{html_mod.escape(article_url)}">Open on derstandard.at</a> &middot; {total} filtered post(s) in {len(sorted_threads)} thread(s)</p>
{thread_html}
</body></html>"""
        self._send_html(page)

    def _handle_dashboard(self):
        forums = _shared["forums"]
        snapshots = _shared["snapshots"]
        db_path = _shared["db_path"]

        # Query moderated counts and recent moderated postings from SQLite.
        mod_counts = {}
        mod_last = {}
        recent_moderated = []
        try:
            conn = sqlite3.connect(db_path)
            for row in conn.execute(
                "SELECT article_url, COUNT(*), MAX(moderated_at) FROM moderated_postings GROUP BY article_url"
            ):
                mod_counts[row[0]] = row[1]
                mod_last[row[0]] = row[2] or ""
            for row in conn.execute(
                "SELECT article_url, posting_id, author, title, text, created_at, moderated_at, is_reply, upvotes, downvotes"
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
            last_mod = mod_last.get(url, "")
            rows.append((url, title, postings, moderated, age, last_mod))

        rows.sort(key=lambda r: r[5], reverse=True)

        def _relative_time(iso_str):
            try:
                dt = datetime.fromisoformat(iso_str)
                delta = int((now - dt).total_seconds())
                if delta < 60:
                    return f"{delta}s ago"
                elif delta < 3600:
                    return f"{delta // 60}m ago"
                else:
                    return f"{delta // 3600}h {(delta % 3600) // 60}m ago"
            except (ValueError, TypeError):
                return "\u2014"

        table_rows = ""
        for url, title, postings, moderated, age, last_mod in rows:
            label = html_mod.escape(title) if title else url
            detail_url = f"/article?url={urllib.parse.quote(url, safe='')}"
            last_mod_age = _relative_time(last_mod) if last_mod else "\u2014"
            table_rows += (
                f"<tr><td><a href=\"{detail_url}\">{label}</a></td>"
                f"<td>{postings}</td><td>{moderated}</td>"
                f"<td data-sort=\"{last_mod}\">{last_mod_age}</td>"
                f"<td>{age}</td></tr>\n"
            )

        recent_rows = ""
        for art_url, pid, author, title, text, created, moderated_at, is_reply, upvotes, downvotes in recent_moderated:
            safe_author = html_mod.escape(author or "")
            safe_title = html_mod.escape(title or "")
            safe_text = html_mod.escape(text or "")
            art_label = html_mod.escape(url_titles.get(art_url, "")) or art_url
            mod_age = _relative_time(moderated_at)
            reply_marker = "yes" if is_reply else ""
            recent_rows += (
                f"<tr><td>{safe_author}</td>"
                f"<td class=\"truncate\" title=\"{safe_title}\">{safe_title}</td>"
                f"<td class=\"truncate\" title=\"{safe_text}\">{safe_text}</td>"
                f"<td><a href=\"{art_url}\">{art_label}</a></td>"
                f"<td>{reply_marker}</td>"
                f"<td>+{upvotes}/&minus;{downvotes}</td>"
                f"<td data-sort=\"{moderated_at or ''}\">{mod_age}</td></tr>\n"
            )

        html = f"""<!doctype html>
<html><head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="60">
<title>derStandard Moderation Dashboard</title>
<style>
  *, *::before, *::after {{ box-sizing: border-box; }}
  body {{ font-family: system-ui, -apple-system, sans-serif; margin: 0; padding: 1.5rem 2rem; background: #f5f6f8; color: #1a1a1a; }}
  h1 {{ font-size: 1.3rem; margin: 0 0 .25rem; }}
  h2 {{ font-size: 1.1rem; margin: 2rem 0 .75rem; }}
  .meta {{ color: #666; font-size: .85rem; margin-bottom: 1.25rem; }}
  a {{ color: #0366d6; text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
  .table-wrap {{ overflow-x: auto; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,.08); margin-bottom: .5rem; }}
  table {{ border-collapse: collapse; width: 100%; background: #fff; font-size: .9rem; }}
  th, td {{ text-align: left; padding: .6rem .85rem; border-bottom: 1px solid #eee; white-space: nowrap; }}
  th {{ background: #1e293b; color: #e2e8f0; font-weight: 600; position: sticky; top: 0; cursor: pointer; user-select: none; }}
  th:hover {{ background: #334155; }}
  th .arrow {{ font-size: .7rem; margin-left: .3rem; opacity: .5; }}
  th.sorted .arrow {{ opacity: 1; }}
  tbody tr:nth-child(even) {{ background: #f9fafb; }}
  tbody tr:hover {{ background: #eef2ff; }}
  td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  .truncate {{ max-width: 350px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
</style>
</head><body>
<h1>derStandard Moderation Dashboard</h1>
<p class="meta">Monitoring {len(forums)} forum(s) &middot; refreshes every 60s &middot; <a href="/sql">SQL</a></p>

<div class="table-wrap">
<table class="sortable" id="articles">
<thead><tr>
  <th>Article <span class="arrow">&varr;</span></th>
  <th>Postings <span class="arrow">&varr;</span></th>
  <th>Filtered <span class="arrow">&varr;</span></th>
  <th class="sorted">Last Filtered <span class="arrow">&darr;</span></th>
  <th>Last Activity <span class="arrow">&varr;</span></th>
</tr></thead>
<tbody>{table_rows}</tbody>
</table>
</div>

<h2>Last 100 Filtered Posts</h2>
<div class="table-wrap">
<table class="sortable" id="recent">
<thead><tr>
  <th>Author <span class="arrow">&varr;</span></th>
  <th>Title <span class="arrow">&varr;</span></th>
  <th>Text <span class="arrow">&varr;</span></th>
  <th>Article <span class="arrow">&varr;</span></th>
  <th>Reply <span class="arrow">&varr;</span></th>
  <th>Votes <span class="arrow">&varr;</span></th>
  <th class="sorted">Filtered <span class="arrow">&darr;</span></th>
</tr></thead>
<tbody>{recent_rows}</tbody>
</table>
</div>

<script>
document.querySelectorAll('table.sortable').forEach(table => {{
  const headers = table.querySelectorAll('th');
  let currentCol = -1, ascending = false;
  // Find initially sorted column
  headers.forEach((th, i) => {{ if (th.classList.contains('sorted')) currentCol = i; }});

  headers.forEach((th, colIdx) => {{
    th.addEventListener('click', () => {{
      if (currentCol === colIdx) {{ ascending = !ascending; }}
      else {{ ascending = true; currentCol = colIdx; }}
      headers.forEach(h => {{ h.classList.remove('sorted'); h.querySelector('.arrow').innerHTML = '&varr;'; }});
      th.classList.add('sorted');
      th.querySelector('.arrow').innerHTML = ascending ? '&uarr;' : '&darr;';

      const tbody = table.querySelector('tbody');
      const rows = Array.from(tbody.querySelectorAll('tr'));
      rows.sort((a, b) => {{
        const cellA = a.children[colIdx], cellB = b.children[colIdx];
        let va = cellA.dataset.sort !== undefined ? cellA.dataset.sort : cellA.textContent.trim();
        let vb = cellB.dataset.sort !== undefined ? cellB.dataset.sort : cellB.textContent.trim();
        const isNum = s => /^-?\d+(\.\d+)?$/.test(s);
        if (isNum(va) && isNum(vb)) {{ const d = parseFloat(va) - parseFloat(vb); return ascending ? d : -d; }}
        return ascending ? va.localeCompare(vb) : vb.localeCompare(va);
      }});
      rows.forEach(r => tbody.appendChild(r));
    }});
  }});
}});
</script>
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


def collect_postings_from_node(node, parent_id=""):
    """Recursively collect a posting and all its nested replies."""
    postings = {}
    upvotes = 0
    downvotes = 0
    for r in (node.get("reactions") or {}).get("aggregated", []):
        if r["name"] == "positive":
            upvotes = r["value"]
        elif r["name"] == "negative":
            downvotes = r["value"]
    is_deleted = node.get("lifecycleStatus") == "Deleted"
    postings[node["id"]] = {
        "id": node["id"],
        "author": node["author"]["name"],
        "title": node.get("title") or "",
        "text": node.get("text") or "",
        "created_at": node["history"]["created"],
        "root_posting_id": node.get("rootPostingId", ""),
        "parent_posting_id": parent_id,
        "upvotes": upvotes,
        "downvotes": downvotes,
        "self_deleted": is_deleted,
    }
    for reply in node.get("replies", []):
        postings.update(collect_postings_from_node(reply, parent_id=node["id"]))
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
            is_reply INTEGER NOT NULL DEFAULT 0,
            upvotes INTEGER NOT NULL DEFAULT 0,
            downvotes INTEGER NOT NULL DEFAULT 0,
            thread_id TEXT NOT NULL DEFAULT '',
            article_title TEXT NOT NULL DEFAULT '',
            parent_posting_id TEXT NOT NULL DEFAULT '',
            parent_author TEXT NOT NULL DEFAULT '',
            parent_title TEXT NOT NULL DEFAULT '',
            parent_text TEXT NOT NULL DEFAULT '',
            UNIQUE(forum_id, posting_id)
        )
    """)
    # Migrate existing databases that lack new columns.
    for col, defn in [
        ("is_reply", "INTEGER NOT NULL DEFAULT 0"),
        ("upvotes", "INTEGER NOT NULL DEFAULT 0"),
        ("downvotes", "INTEGER NOT NULL DEFAULT 0"),
        ("thread_id", "TEXT NOT NULL DEFAULT ''"),
        ("article_title", "TEXT NOT NULL DEFAULT ''"),
        ("parent_posting_id", "TEXT NOT NULL DEFAULT ''"),
        ("parent_author", "TEXT NOT NULL DEFAULT ''"),
        ("parent_title", "TEXT NOT NULL DEFAULT ''"),
        ("parent_text", "TEXT NOT NULL DEFAULT ''"),
    ]:
        try:
            conn.execute(f"ALTER TABLE moderated_postings ADD COLUMN {col} {defn}")
        except sqlite3.OperationalError:
            pass  # Column already exists.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    conn.commit()
    return conn


def get_meta(db_path, key):
    """Read a value from the metadata table."""
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT value FROM metadata WHERE key = ?", (key,)).fetchone()
    conn.close()
    return row[0] if row else None


def set_meta(db_path, key, value):
    """Write a value to the metadata table."""
    conn = sqlite3.connect(db_path)
    conn.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)", (key, str(value)))
    conn.commit()
    conn.close()


def save_moderated(conn, forum_id, article_url, article_title, posting):
    """Save a moderated posting to the database."""
    now = datetime.now(timezone.utc).isoformat()
    try:
        root = posting.get("root_posting_id") or ""
        is_reply = 1 if root else 0
        thread_id = root if root else posting["id"]
        parent_posting_id = posting.get("parent_posting_id", "")
        parent_author = posting.get("parent_author", "")
        parent_title = posting.get("parent_title", "")
        parent_text = posting.get("parent_text", "")
        conn.execute(
            """INSERT OR IGNORE INTO moderated_postings
               (forum_id, article_url, posting_id, author, title, text, created_at, moderated_at, is_reply, upvotes, downvotes, thread_id, article_title, parent_posting_id, parent_author, parent_title, parent_text)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (forum_id, article_url, posting["id"], posting["author"],
             posting["title"], posting["text"], posting["created_at"], now, is_reply,
             posting.get("upvotes", 0), posting.get("downvotes", 0), thread_id, article_title, parent_posting_id, parent_author, parent_title, parent_text),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def get_daily_stats(db_path, since_hours=24):
    """Query SQLite for moderation stats over the last `since_hours` hours.

    Returns a formatted string suitable for inclusion in a Gemini prompt.
    Returns None if there were no moderated posts in the period.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=since_hours)).isoformat()
    conn = sqlite3.connect(db_path)

    # Total moderated count
    total = conn.execute(
        "SELECT COUNT(*) FROM moderated_postings WHERE moderated_at >= ?", (cutoff,)
    ).fetchone()[0]

    if total == 0:
        conn.close()
        return None

    # Per-article breakdown (by title, drop URL)
    articles = conn.execute(
        "SELECT article_title, COUNT(*) AS cnt, SUM(upvotes), SUM(downvotes)"
        " FROM moderated_postings WHERE moderated_at >= ?"
        " GROUP BY article_url ORDER BY cnt DESC",
        (cutoff,),
    ).fetchall()

    # Top 10 moderated authors
    authors = conn.execute(
        "SELECT author, COUNT(*) AS cnt"
        " FROM moderated_postings WHERE moderated_at >= ?"
        " GROUP BY author ORDER BY cnt DESC LIMIT 10",
        (cutoff,),
    ).fetchall()

    # Reply vs root post ratio
    reply_count = conn.execute(
        "SELECT COUNT(*) FROM moderated_postings WHERE moderated_at >= ? AND is_reply = 1",
        (cutoff,),
    ).fetchone()[0]

    # Actual posts from the most moderated article (for pattern analysis)
    top_article_posts = []
    if articles:
        top_title = articles[0][0]
        top_article_posts = conn.execute(
            "SELECT author, title, text, is_reply, upvotes, downvotes"
            " FROM moderated_postings WHERE moderated_at >= ? AND article_title = ?"
            " ORDER BY created_at",
            (cutoff, top_title),
        ).fetchall()

    conn.close()

    root_count = total - reply_count

    lines = [f"Total moderated posts: {total}"]
    lines.append(f"Root posts moderated: {root_count}")
    lines.append(f"Replies moderated: {reply_count}")
    lines.append("")
    lines.append("Per-article breakdown:")
    for title, cnt, up, down in articles:
        label = title or "(unknown title)"
        lines.append(f"  {label} — {cnt} moderated (upvotes: {up}, downvotes: {down})")
    lines.append("")
    lines.append("Top moderated authors:")
    for author, cnt in authors:
        lines.append(f"  {author}: {cnt}")

    if top_article_posts:
        top_label = articles[0][0] or "(unknown title)"
        lines.append("")
        lines.append(f"Moderated posts from top article ({top_label}):")
        for author, title, text, is_reply, up, down in top_article_posts:
            kind = "reply" if is_reply else "root"
            lines.append(f"  [{kind}] {author}: {text} (upvotes: {up}, downvotes: {down})")

    return "\n".join(lines)


def gemini_generate(api_key, prompt):
    """Call the Gemini API to generate text from a prompt."""
    url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"
    body = json.dumps({"contents": [{"parts": [{"text": prompt}]}]}).encode()
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "x-goog-api-key": api_key,
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = json.loads(resp.read())
    return data["candidates"][0]["content"]["parts"][0]["text"]


def reddit_get_token(client_id, client_secret, username, password):
    """Obtain a Reddit OAuth access token via the password grant flow."""
    credentials = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    body = urllib.parse.urlencode({
        "grant_type": "password",
        "username": username,
        "password": password,
    }).encode()
    req = urllib.request.Request(
        "https://www.reddit.com/api/v1/access_token",
        data=body,
        headers={
            "Authorization": f"Basic {credentials}",
            "User-Agent": "derstandard-mod-detector/1.0",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read())
    if "access_token" not in data:
        raise RuntimeError(f"Reddit auth failed: {data}")
    return data["access_token"]


def reddit_submit(access_token, subreddit, title, body_text):
    """Submit a self-post to a subreddit."""
    body = urllib.parse.urlencode({
        "sr": subreddit,
        "kind": "self",
        "title": title,
        "text": body_text,
        "api_type": "json",
    }).encode()
    req = urllib.request.Request(
        "https://oauth.reddit.com/api/submit",
        data=body,
        headers={
            "Authorization": f"Bearer {access_token}",
            "User-Agent": "derstandard-mod-detector/1.0",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read())
    errors = data.get("json", {}).get("errors", [])
    if errors:
        raise RuntimeError(f"Reddit submit errors: {errors}")
    return data.get("json", {}).get("data", {}).get("url", "")


def post_daily_summary(args, db_path):
    """Orchestrate: query stats -> Gemini analysis -> Reddit post."""
    stats = get_daily_stats(db_path)
    if stats is None:
        log("Daily summary: no moderated posts in the last 24h, skipping")
        return

    weekly_stats = get_daily_stats(db_path, since_hours=168)

    prompt = (
        "You are summarizing daily moderation activity on derstandard.at, an Austrian news site.\n"
        "Here are the stats for the last 24 hours:\n\n"
        f"{stats}\n\n"
    )
    if weekly_stats:
        prompt += (
            "For context, here are the cumulative stats for the previous 7 days:\n\n"
            f"{weekly_stats}\n\n"
        )
    prompt += (
        "Write a concise Reddit post (2-3 paragraphs) in English that:\n"
        "- Summarizes the key numbers (total moderated, busiest articles by title)\n"
        "- Analyzes patterns in the actual moderated posts from the top article (common themes, tone, why they may have been removed)\n"
        "- Notes any interesting patterns (e.g. which topics got heavy moderation)\n"
        "- Briefly compares today's activity to the 7-day trend if notable\n"
        "- Keeps a neutral, informative tone\n"
        "- Uses Reddit markdown formatting\n\n"
        "Do not include a title — just the body text."
    )

    log("Daily summary: generating analysis via Gemini...")
    body_text = gemini_generate(args.gemini_api_key, prompt)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    title = f"derstandard.at Moderation Summary — {today}"

    log("Daily summary: posting to Reddit...")
    token = reddit_get_token(
        args.reddit_client_id, args.reddit_client_secret,
        args.reddit_username, args.reddit_password,
    )
    post_url = reddit_submit(token, args.reddit_subreddit, title, body_text)
    log(f"Daily summary: posted to Reddit — {post_url}")


def main():
    parser = argparse.ArgumentParser(description="Detect moderated postings on derstandard.at")
    parser.add_argument("urls", nargs="*", default=[], help="Article URLs to monitor")
    parser.add_argument("--interval", type=int, default=240, help="Poll interval in seconds (default: 240)")
    parser.add_argument("--db", default="data/moderated_postings.db", help="SQLite database path")
    parser.add_argument("--discover", action="store_true", help="Enable RSS auto-discovery of articles")
    parser.add_argument("--min-posts", type=int, default=50, help="Minimum postings to monitor a discovered article (default: 50)")
    parser.add_argument("--max-inactive", type=int, default=60, help="Drop forums with no new post in this many minutes (default: 60)")
    parser.add_argument("--discover-interval", type=int, default=5, help="Run discovery every Nth poll cycle (default: 5)")
    parser.add_argument("--web-port", type=int, default=8080, help="Web dashboard port (default: 8080, 0 to disable)")
    parser.add_argument("--reddit-client-id", default=os.environ.get("REDDIT_CLIENT_ID", ""), help="Reddit app client ID (empty = disable posting)")
    parser.add_argument("--reddit-client-secret", default=os.environ.get("REDDIT_CLIENT_SECRET", ""), help="Reddit app client secret")
    parser.add_argument("--reddit-username", default=os.environ.get("REDDIT_USERNAME", ""), help="Reddit account username")
    parser.add_argument("--reddit-password", default=os.environ.get("REDDIT_PASSWORD", ""), help="Reddit account password")
    parser.add_argument("--reddit-subreddit", default=os.environ.get("REDDIT_SUBREDDIT", ""), help="Target subreddit name (no r/ prefix)")
    parser.add_argument("--gemini-api-key", default=os.environ.get("GEMINI_API_KEY", ""), help="Google Gemini API key")
    parser.add_argument("--post-hour", type=int, default=int(os.environ.get("POST_HOUR", "7")), help="UTC hour to post daily summary (default: 7)")
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

        # Daily Reddit summary (persisted in DB to survive restarts)
        now_utc = datetime.now(timezone.utc)
        last_post = get_meta(args.db, "last_post_date")
        if (args.reddit_client_id
                and last_post != str(now_utc.date())
                and now_utc.hour >= args.post_hour):
            try:
                post_daily_summary(args, args.db)
                set_meta(args.db, "last_post_date", now_utc.date())
                log("Daily Reddit post published")
            except Exception as e:
                log(f"Daily Reddit post failed: {e}")

        # Periodic discovery
        if args.discover and cycle > 1 and (cycle % args.discover_interval == 0):
            log("Running RSS discovery...")
            new = discover_articles(forums, args.min_posts)
            forums.update(new)
            if new:
                log(f"  {len(new)} new forum(s) discovered")

        for forum_id in list(forums):
            article_url = forums[forum_id]["url"]
            article_title = forums[forum_id].get("title", "")
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
                    # Filter out self-deleted posts (lifecycleStatus: "Deleted")
                    moderated = [pid for pid in removed
                                 if not posting_cache.get(pid, {}).get("self_deleted")]
                    self_deleted = len(removed) - len(moderated)
                    if self_deleted:
                        log(f"  {self_deleted} self-deleted posting(s) skipped")
                    if moderated:
                        log(f"  -{len(moderated)} MODERATED postings:")
                    for pid in moderated:
                        posting = posting_cache.get(pid, {
                            "id": pid, "author": "?", "title": "?",
                            "text": "?", "created_at": "?",
                        })
                        parent_id = posting.get("parent_posting_id", "")
                        if parent_id:
                            parent = posting_cache.get(parent_id)
                            if parent:
                                posting["parent_author"] = parent.get("author", "")
                                posting["parent_title"] = parent.get("title", "")
                                posting["parent_text"] = parent.get("text", "")
                        saved = save_moderated(conn, forum_id, article_url, article_title, posting)
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
