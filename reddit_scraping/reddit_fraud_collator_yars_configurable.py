"""
reddit_fraud_collator_yars_configurable.py

Scrape Reddit without login using YARS, classify posts/comments into categories
loaded from an external JSON file, and collate all data points by category into
a single JSON output.

Usage:
    python reddit_fraud_collator_yars_configurable.py

Optional args:
    python reddit_fraud_collator_yars_configurable.py --config fraud_categories.json
    python reddit_fraud_collator_yars_configurable.py --output-dir output
    python reddit_fraud_collator_yars_configurable.py --resume
    python reddit_fraud_collator_yars_configurable.py --checkpoint output/reddit_fraud_collated_<ts>.json
    python reddit_fraud_collator_yars_configurable.py --merge-jsons-only --output-dir output

Requirements:
    - YARS must be importable (yars.yars or yars package export).
    - No Reddit login/API credentials required
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
import time
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

try:
    from yars.yars import YARS
except ImportError:
    try:
        from yars import YARS
    except ImportError as e:
        raise ImportError(
            "Could not import YARS. Add YARS `src` to PYTHONPATH "
            "(e.g. export PYTHONPATH=\"/path/to/YARS/src:$PYTHONPATH\")."
        ) from e


DEFAULT_CONFIG: Dict[str, Any] = {
    "backend": "yars",
    "output_dir": "output",
    "sleep_seconds": 3.0,
    "deduplicate_by_permalink": True,
    "include_posts": True,
    "include_comments": True,
    "search_limit_per_query": 40,
    "subreddit_fetch_limit": 30,
    "subreddit_fetch_category": "new",
    "subreddit_time_filter": "week",
    "subreddits": [
        "Scams",
        "personalfinance",
        "legaladvice",
        "CryptoCurrency",
        "jobs",
        "canada",
        "technology"
    ],
}


@dataclass
class MessageRecord:
    record_type: str
    reddit_post_id: Optional[str]
    reddit_comment_id: Optional[str]
    subreddit: Optional[str]
    title: Optional[str]
    text: str
    author: Optional[str]
    score: Optional[int]
    created_utc: Optional[int]
    created_iso: Optional[str]
    permalink: Optional[str]
    parent_permalink: Optional[str]
    source_query: Optional[str]
    source_category: Optional[str]
    matched_categories: List[str]
    matched_keywords: Dict[str, List[str]]


@dataclass
class PostEnvelope:
    post_id: Optional[str]
    subreddit: Optional[str]
    title: Optional[str]
    body: Optional[str]
    author: Optional[str]
    score: Optional[int]
    created_utc: Optional[int]
    created_iso: Optional[str]
    permalink: Optional[str]
    source_query: Optional[str]
    source_category: Optional[str]
    matched_categories: List[str]
    comment_count_extracted: int
    comments: List[Dict[str, Any]]


def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


FLAT_CSV_FIELDS = [
    "bucket_category",
    "record_type",
    "reddit_post_id",
    "reddit_comment_id",
    "subreddit",
    "title",
    "text",
    "author",
    "score",
    "created_utc",
    "created_iso",
    "permalink",
    "parent_permalink",
    "source_query",
    "source_category",
    "matched_categories_json",
    "matched_keywords_json",
]


def _csv_row_for_message(bucket_category: str, m: Dict[str, Any]) -> Dict[str, str]:
    def s(val: Any) -> str:
        if val is None:
            return ""
        if isinstance(val, (dict, list)):
            return json.dumps(val, ensure_ascii=False)
        return str(val)

    return {
        "bucket_category": bucket_category,
        "record_type": s(m.get("record_type")),
        "reddit_post_id": s(m.get("reddit_post_id")),
        "reddit_comment_id": s(m.get("reddit_comment_id")),
        "subreddit": s(m.get("subreddit")),
        "title": s(m.get("title")),
        "text": s(m.get("text")),
        "author": s(m.get("author")),
        "score": s(m.get("score")),
        "created_utc": s(m.get("created_utc")),
        "created_iso": s(m.get("created_iso")),
        "permalink": s(m.get("permalink")),
        "parent_permalink": s(m.get("parent_permalink")),
        "source_query": s(m.get("source_query")),
        "source_category": s(m.get("source_category")),
        "matched_categories_json": json.dumps(m.get("matched_categories") or [], ensure_ascii=False),
        "matched_keywords_json": json.dumps(m.get("matched_keywords") or {}, ensure_ascii=False),
    }


def write_category_bundle_json(
    path: Path,
    *,
    source_file: str,
    messages_by_category: Dict[str, List[Dict[str, Any]]],
    extra_metadata: Dict[str, Any],
) -> None:
    payload = {
        "exported_at_utc": now_utc_iso(),
        "source": source_file,
        "categories": {k: list(messages_by_category[k]) for k in sorted(messages_by_category.keys())},
        "metadata": extra_metadata,
    }
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def write_flat_csv_by_category(
    path: Path,
    messages_by_category: Dict[str, List[Dict[str, Any]]],
) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FLAT_CSV_FIELDS)
        w.writeheader()
        for bucket_category in sorted(messages_by_category.keys()):
            for m in messages_by_category[bucket_category]:
                w.writerow(_csv_row_for_message(bucket_category, m))


def message_dedupe_key(m: Dict[str, Any]) -> str:
    return json.dumps(
        {
            "permalink": m.get("permalink") or "",
            "cid": m.get("reddit_comment_id") or "",
            "rtype": m.get("record_type") or "",
            "text": (m.get("text") or "")[:400],
        },
        sort_keys=True,
    )


def merge_collated_json_files(
    output_dir: Path,
) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Any]]:
    """
    Merge `messages_by_category` from all `reddit_fraud_collated_<ts>.json` files in a directory.
    Deduplicates rows per category bucket across files.
    """
    merged: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    seen: Dict[str, set[str]] = defaultdict(set)
    sources: List[str] = []

    for p in sorted(output_dir.glob("reddit_fraud_collated_*.json")):
        try:
            with p.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError):
            continue
        mbc = data.get("messages_by_category")
        if not isinstance(mbc, dict):
            continue
        sources.append(p.name)
        for cat, msgs in mbc.items():
            if not isinstance(msgs, list):
                continue
            for msg in msgs:
                if not isinstance(msg, dict):
                    continue
                dk = message_dedupe_key(msg)
                if dk in seen[cat]:
                    continue
                seen[cat].add(dk)
                merged[cat].append(msg)

    def sort_key(m: Dict[str, Any]) -> int:
        return int(m.get("created_utc") or 0)

    out = {k: sorted(v, key=sort_key, reverse=True) for k, v in merged.items()}
    meta = {
        "merge_generated_at_utc": now_utc_iso(),
        "source_files": sources,
        "total_categories": len(out),
        "total_rows": sum(len(v) for v in out.values()),
    }
    return out, meta


def export_merged_collated_jsons(output_dir: Path) -> Tuple[Path, Path]:
    """Write merged-by-category JSON + flat CSV from all collated JSONs in output_dir."""
    output_dir = ensure_dir(output_dir)
    merged, meta = merge_collated_json_files(output_dir)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    jpath = output_dir / f"reddit_fraud_merged_all_{ts}.json"
    cpath = output_dir / f"reddit_fraud_merged_all_{ts}.csv"
    write_category_bundle_json(
        jpath,
        source_file="merged:" + ",".join(meta["source_files"]),
        messages_by_category=merged,
        extra_metadata=meta,
    )
    write_flat_csv_by_category(cpath, merged)
    return jpath, cpath


def config_fingerprint(category_config: Dict[str, Any], runtime_config: Dict[str, Any]) -> str:
    """Detect config drift so we do not resume into a mismatched query set."""
    payload = {
        "category_queries": category_config["category_queries"],
        "category_keywords": category_config["category_keywords"],
        "subreddits": runtime_config["subreddits"],
        "search_limit_per_query": runtime_config["search_limit_per_query"],
        "subreddit_fetch_limit": runtime_config["subreddit_fetch_limit"],
        "subreddit_fetch_category": runtime_config["subreddit_fetch_category"],
        "subreddit_time_filter": runtime_config["subreddit_time_filter"],
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:16]


def search_task_key(source_category: str, query: str) -> str:
    return f"{source_category}||{query}"


def _is_resumable_run_json(data: Dict[str, Any]) -> bool:
    """True if this is a run file that can be resumed (not finished)."""
    if "metadata" in data:
        st = data["metadata"].get("checkpoint_status")
        if st is not None and st != "complete":
            return True
    if data.get("checkpoint_status") is not None and data.get("checkpoint_status") != "complete":
        return True  # legacy standalone checkpoint file
    return False


def find_latest_resumable_checkpoint(output_dir: Path) -> Optional[Path]:
    """Most recent reddit_fraud_collated_*.json (or legacy checkpoint) that is not complete."""
    candidates: List[Path] = list(output_dir.glob("reddit_fraud_collated_*.json"))
    candidates += list(output_dir.glob("reddit_fraud_checkpoint_*.json"))
    paths = sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)
    for p in paths:
        try:
            with p.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if _is_resumable_run_json(data):
                return p
        except (OSError, json.JSONDecodeError):
            continue
    return None


def load_resume_blob(rp: Path) -> Dict[str, Any]:
    """
    Load state from a run JSON (reddit_fraud_collated_*.json) or legacy checkpoint JSON.
    Returns dict with keys: stored_fp, ts, run_started, seen_post_keys, completed_search_tasks,
    completed_subreddit_tasks, total_posts_examined, total_posts_kept, total_comments_kept,
    counts_by_category, messages_by_category, records_raw.
    """
    with rp.open("r", encoding="utf-8") as f:
        ck = json.load(f)
    if "metadata" in ck and "resume" in ck.get("metadata", {}):
        md = ck["metadata"]
        r = md["resume"]
        stats = ck.get("stats") or {}
        return {
            "stored_fp": md.get("config_fingerprint"),
            "ts": md.get("run_ts"),
            "run_started": md.get("run_started_at_utc"),
            "seen_post_keys": set(r.get("seen_post_keys") or []),
            "completed_search_tasks": set(r.get("completed_search_tasks") or []),
            "completed_subreddit_tasks": set(r.get("completed_subreddit_tasks") or []),
            "total_posts_examined": int(stats.get("unique_posts_examined") or 0),
            "total_posts_kept": int(stats.get("unique_posts_kept") or 0),
            "total_comments_kept": int(stats.get("comments_kept") or 0),
            "counts_by_category": Counter(ck.get("counts_by_category") or {}),
            "messages_by_category": ck.get("messages_by_category") or {},
            "records_raw": ck.get("records") or [],
        }
    return {
        "stored_fp": ck.get("config_fingerprint"),
        "ts": ck["run_ts"],
        "run_started": ck.get("run_started_at_utc"),
        "seen_post_keys": set(ck.get("seen_post_keys") or []),
        "completed_search_tasks": set(ck.get("completed_search_tasks") or []),
        "completed_subreddit_tasks": set(ck.get("completed_subreddit_tasks") or []),
        "total_posts_examined": int(ck.get("total_posts_examined") or 0),
        "total_posts_kept": int(ck.get("total_posts_kept") or 0),
        "total_comments_kept": int(ck.get("total_comments_kept") or 0),
        "counts_by_category": Counter(ck.get("counts_by_category") or {}),
        "messages_by_category": ck.get("messages_by_category") or {},
        "records_raw": ck.get("records") or [],
    }


def envelope_from_dict(d: Dict[str, Any]) -> PostEnvelope:
    return PostEnvelope(
        post_id=d.get("post_id"),
        subreddit=d.get("subreddit"),
        title=d.get("title"),
        body=d.get("body"),
        author=d.get("author"),
        score=d.get("score"),
        created_utc=d.get("created_utc"),
        created_iso=d.get("created_iso"),
        permalink=d.get("permalink"),
        source_query=d.get("source_query"),
        source_category=d.get("source_category"),
        matched_categories=list(d.get("matched_categories") or []),
        comment_count_extracted=int(d.get("comment_count_extracted") or 0),
        comments=list(d.get("comments") or []),
    )


def normalize_text(text: Any) -> str:
    if text is None:
        return ""
    text = str(text).replace("\r", "\n")
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text


def safe_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def utc_to_iso(ts: Any) -> Optional[str]:
    ts_int = safe_int(ts)
    if ts_int is None:
        return None
    try:
        return datetime.fromtimestamp(ts_int, tz=timezone.utc).isoformat()
    except Exception:
        return None


def compile_category_patterns(category_keywords: Dict[str, List[str]]) -> Dict[str, List[Tuple[str, re.Pattern]]]:
    compiled: Dict[str, List[Tuple[str, re.Pattern]]] = {}
    for category, keywords in category_keywords.items():
        compiled[category] = []
        for keyword in keywords:
            escaped = re.escape(keyword.lower()).replace(r"\ ", r"\s+")
            pattern = re.compile(rf"\b{escaped}\b", re.IGNORECASE)
            compiled[category].append((keyword, pattern))
    return compiled


def classify_text(
    text: str,
    compiled_patterns: Dict[str, List[Tuple[str, re.Pattern]]],
    fallback_category: Optional[str] = None
) -> Tuple[List[str], Dict[str, List[str]]]:
    normalized = normalize_text(text)
    matched_categories: List[str] = []
    matched_keywords: Dict[str, List[str]] = {}

    for category, kw_patterns in compiled_patterns.items():
        hits: List[str] = []
        for keyword, pattern in kw_patterns:
            if pattern.search(normalized):
                hits.append(keyword)
        if hits:
            matched_categories.append(category)
            matched_keywords[category] = sorted(set(hits))

    if not matched_categories and fallback_category:
        matched_categories = [fallback_category]
        matched_keywords = {fallback_category: ["fallback_from_search_context"]}

    return matched_categories, matched_keywords


def flatten_comment_tree(comments: Any) -> List[Dict[str, Any]]:
    flat: List[Dict[str, Any]] = []

    def _walk(node: Any) -> None:
        if node is None:
            return

        if isinstance(node, list):
            for item in node:
                _walk(item)
            return

        if isinstance(node, dict):
            body = (
                node.get("body")
                or node.get("comment")
                or node.get("text")
                or node.get("content")
            )

            comment_id = node.get("id") or node.get("comment_id")

            if body is not None:
                flat.append({
                    "comment_id": comment_id,
                    "body": body,
                    "author": node.get("author"),
                    "score": safe_int(node.get("score")),
                    "created_utc": safe_int(node.get("created_utc") or node.get("timestamp")),
                    "permalink": node.get("permalink"),
                })

            for replies_key in ("replies", "children", "comments"):
                if replies_key in node:
                    _walk(node[replies_key])
            return

    _walk(comments)
    return flat


def pick_first(d: Dict[str, Any], keys: Iterable[str], default: Any = None) -> Any:
    for key in keys:
        if key in d and d[key] not in (None, ""):
            return d[key]
    return default


def reddit_permalink_path(value: Optional[str]) -> Optional[str]:
    """
    YARS scrape_post_details expects a path like /r/sub/comments/id/slug/.
    YARS search_reddit returns full URLs under 'link' only — normalize those here.
    """
    if not value or not isinstance(value, str):
        return None
    v = value.strip()
    if v.startswith(("http://", "https://")):
        path = urlparse(v).path or ""
        return path if path else None
    if v.startswith("/"):
        return v
    return None


def post_id_from_reddit_path(path: Optional[str]) -> Optional[str]:
    if not path:
        return None
    m = re.search(r"/comments/([^/]+)/", path)
    return m.group(1) if m else None


def normalize_post_dict(raw: Dict[str, Any], source_query: str, source_category: str) -> Dict[str, Any]:
    permalink = reddit_permalink_path(
        pick_first(raw, ["permalink", "link", "url", "post_url"])
    )
    title = pick_first(raw, ["title"])
    body = pick_first(
        raw, ["body", "selftext", "text", "content", "description"], ""
    )
    post_id = pick_first(raw, ["id", "post_id"])
    if not post_id and permalink:
        post_id = post_id_from_reddit_path(permalink)
    subreddit = pick_first(raw, ["subreddit", "subreddit_name"])
    author = pick_first(raw, ["author", "username"])
    score = safe_int(pick_first(raw, ["score", "ups"]))
    created_utc = safe_int(pick_first(raw, ["created_utc", "timestamp", "created"]))

    return {
        "post_id": post_id,
        "subreddit": subreddit,
        "title": title,
        "body": body,
        "author": author,
        "score": score,
        "created_utc": created_utc,
        "created_iso": utc_to_iso(created_utc),
        "permalink": permalink,
        "source_query": source_query,
        "source_category": source_category,
    }


def load_category_config(config_path: str | Path) -> Dict[str, Any]:
    path = Path(config_path)
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    required_top_keys = ["category_queries", "category_keywords"]
    for key in required_top_keys:
        if key not in data:
            raise ValueError(f"Missing top-level key in category config: {key}")

    if not isinstance(data["category_queries"], dict) or not data["category_queries"]:
        raise ValueError("category_queries must be a non-empty object")

    if not isinstance(data["category_keywords"], dict) or not data["category_keywords"]:
        raise ValueError("category_keywords must be a non-empty object")

    return data


class YarsBackend:
    def __init__(self) -> None:
        self.client = YARS()

    def search_posts(self, query: str, limit: int) -> List[Dict[str, Any]]:
        results = self.client.search_reddit(query, limit=limit)
        return results if isinstance(results, list) else []

    def fetch_subreddit_posts(
        self,
        subreddit: str,
        limit: int,
        category: str,
        time_filter: str
    ) -> List[Dict[str, Any]]:
        results = self.client.fetch_subreddit_posts(
            subreddit,
            limit=limit,
            category=category,
            time_filter=time_filter
        )
        return results if isinstance(results, list) else []

    def scrape_post_details(self, permalink: str) -> Optional[Dict[str, Any]]:
        return self.client.scrape_post_details(permalink)


def build_dataset(
    runtime_config: Dict[str, Any],
    category_config: Dict[str, Any],
    *,
    resume_checkpoint_path: Optional[Path] = None,
    force_resume: bool = False,
) -> Dict[str, Any]:
    output_dir = ensure_dir(runtime_config["output_dir"])
    compiled_patterns = compile_category_patterns(category_config["category_keywords"])
    backend = YarsBackend()
    fp = config_fingerprint(category_config, runtime_config)

    completed_search_tasks: set[str] = set()
    completed_subreddit_tasks: set[str] = set()

    if resume_checkpoint_path is not None:
        rp = Path(resume_checkpoint_path)
        if not rp.is_file():
            raise FileNotFoundError(f"Run file not found: {rp}")
        blob = load_resume_blob(rp)
        stored_fp = blob["stored_fp"]
        if stored_fp != fp and not force_resume:
            print(
                f"[ERROR] Config fingerprint mismatch (saved={stored_fp}, current={fp}). "
                "Use --force-resume to resume anyway.",
                file=sys.stderr,
            )
            sys.exit(1)
        ts = blob["ts"]
        run_started = blob["run_started"] or now_utc_iso()
        seen_post_keys = blob["seen_post_keys"]
        completed_search_tasks = blob["completed_search_tasks"]
        completed_subreddit_tasks = blob["completed_subreddit_tasks"]
        total_posts_examined = blob["total_posts_examined"]
        total_posts_kept = blob["total_posts_kept"]
        total_comments_kept = blob["total_comments_kept"]
        counts_by_category = blob["counts_by_category"]
        messages_by_category = defaultdict(list)
        for k, v in (blob["messages_by_category"] or {}).items():
            messages_by_category[k] = list(v)
        records = [envelope_from_dict(r) for r in blob["records_raw"]]
        print(
            f"[INFO] Resuming from {rp} | ts={ts} | records={len(records)} | "
            f"seen_keys={len(seen_post_keys)} | searches_done={len(completed_search_tasks)} | "
            f"subreddits_done={len(completed_subreddit_tasks)}"
        )
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_started = now_utc_iso()
        seen_post_keys = set()
        records = []
        messages_by_category = defaultdict(list)
        counts_by_category = Counter()
        total_posts_examined = 0
        total_posts_kept = 0
        total_comments_kept = 0

    jsonl_path = output_dir / f"reddit_fraud_collated_{ts}.jsonl"
    flat_csv_path = output_dir / f"reddit_fraud_flat_{ts}.csv"
    run_json_path = output_dir / f"reddit_fraud_collated_{ts}.json"

    def build_run_payload(checkpoint_status: str) -> Dict[str, Any]:
        return {
            "metadata": {
                "run_started_at_utc": run_started,
                "last_updated_utc": now_utc_iso(),
                "generated_at_utc": now_utc_iso() if checkpoint_status == "complete" else None,
                "run_status": "complete" if checkpoint_status == "complete" else "running",
                "checkpoint_status": checkpoint_status,
                "config_fingerprint": fp,
                "run_ts": ts,
                "backend": runtime_config["backend"],
                "include_posts": runtime_config["include_posts"],
                "include_comments": runtime_config["include_comments"],
                "subreddits": runtime_config["subreddits"],
                "search_limit_per_query": runtime_config["search_limit_per_query"],
                "subreddit_fetch_limit": runtime_config["subreddit_fetch_limit"],
                "subreddit_fetch_category": runtime_config["subreddit_fetch_category"],
                "subreddit_time_filter": runtime_config["subreddit_time_filter"],
                "category_config_categories": list(category_config["category_queries"].keys()),
                "resume": {
                    "completed_search_tasks": sorted(completed_search_tasks),
                    "completed_subreddit_tasks": sorted(completed_subreddit_tasks),
                    "seen_post_keys": sorted(seen_post_keys),
                },
                "output_paths": {
                    "json": str(run_json_path),
                    "jsonl": str(jsonl_path),
                    "flat_csv": str(flat_csv_path),
                },
            },
            "stats": {
                "unique_posts_examined": total_posts_examined,
                "unique_posts_kept": total_posts_kept,
                "comments_kept": total_comments_kept,
                "total_category_buckets": len(messages_by_category),
                "total_messages_across_categories": sum(counts_by_category.values()),
            },
            "counts_by_category": dict(sorted(counts_by_category.items())),
            "messages_by_category": {k: list(v) for k, v in messages_by_category.items()},
            "records": [asdict(r) for r in records],
        }

    def flush_run_json(checkpoint_status: str = "running") -> None:
        """Single JSON file: live progress, checkpoint, and final output share this path."""
        payload = build_run_payload(checkpoint_status)
        with run_json_path.open("w", encoding="utf-8") as rf:
            json.dump(payload, rf, ensure_ascii=False, indent=2)

    jsonl_f = None
    if resume_checkpoint_path is None:
        with jsonl_path.open("w", encoding="utf-8") as jf:
            jf.write(
                json.dumps(
                    {"event": "run_start", "started_at_utc": run_started, "ts": ts},
                    ensure_ascii=False,
                )
                + "\n"
            )
            jf.flush()
    jsonl_f = jsonl_path.open("a", encoding="utf-8")
    if resume_checkpoint_path is not None:
        jsonl_f.write(
            json.dumps({"event": "resume", "at_utc": now_utc_iso(), "ts": ts}, ensure_ascii=False)
            + "\n"
        )
        jsonl_f.flush()

    flush_run_json("running")

    def process_post(raw_post: Dict[str, Any], source_query: str, source_category: str) -> None:
        nonlocal total_posts_examined, total_posts_kept, total_comments_kept

        total_posts_examined += 1
        post = normalize_post_dict(raw_post, source_query, source_category)

        dedup_key = post["permalink"] if runtime_config["deduplicate_by_permalink"] else post["post_id"]
        if dedup_key and dedup_key in seen_post_keys:
            return
        if dedup_key:
            seen_post_keys.add(dedup_key)

        post_text = f"{post.get('title') or ''}\n{post.get('body') or ''}"
        matched_categories, matched_keywords = classify_text(
            post_text,
            compiled_patterns,
            fallback_category=source_category
        )

        detail = None
        if post.get("permalink"):
            try:
                detail = backend.scrape_post_details(post["permalink"])
                time.sleep(runtime_config["sleep_seconds"])
            except Exception as e:
                print(f"[WARN] Failed to scrape details for {post['permalink']}: {e}")

        raw_comments = []
        if isinstance(detail, dict):
            raw_comments = pick_first(detail, ["comments", "replies"], [])
            if not post["title"]:
                post["title"] = pick_first(detail, ["title"])
            if not post["body"]:
                post["body"] = pick_first(detail, ["body", "selftext", "text"], "")
            if not post["author"]:
                post["author"] = pick_first(detail, ["author"])
            if not post["score"]:
                post["score"] = safe_int(pick_first(detail, ["score", "ups"]))
            if not post["created_utc"]:
                post["created_utc"] = safe_int(pick_first(detail, ["created_utc", "timestamp"]))
                post["created_iso"] = utc_to_iso(post["created_utc"])
            if not post["subreddit"]:
                post["subreddit"] = pick_first(detail, ["subreddit", "subreddit_name"])
            if not post["post_id"]:
                post["post_id"] = pick_first(detail, ["id", "post_id"])

        flat_comments = flatten_comment_tree(raw_comments)
        comment_payloads: List[Dict[str, Any]] = []

        if runtime_config["include_posts"]:
            post_message = MessageRecord(
                record_type="post",
                reddit_post_id=post.get("post_id"),
                reddit_comment_id=None,
                subreddit=post.get("subreddit"),
                title=post.get("title"),
                text=(post.get("body") or ""),
                author=post.get("author"),
                score=post.get("score"),
                created_utc=post.get("created_utc"),
                created_iso=post.get("created_iso"),
                permalink=post.get("permalink"),
                parent_permalink=post.get("permalink"),
                source_query=source_query,
                source_category=source_category,
                matched_categories=matched_categories,
                matched_keywords=matched_keywords,
            )
            post_payload = asdict(post_message)
            for cat in matched_categories:
                messages_by_category[cat].append(post_payload)
                counts_by_category[cat] += 1
            total_posts_kept += 1

        if runtime_config["include_comments"]:
            for c in flat_comments:
                comment_text = c.get("body") or ""
                comment_categories, comment_keywords = classify_text(
                    comment_text,
                    compiled_patterns,
                    fallback_category=source_category
                )

                comment_message = MessageRecord(
                    record_type="comment",
                    reddit_post_id=post.get("post_id"),
                    reddit_comment_id=c.get("comment_id"),
                    subreddit=post.get("subreddit"),
                    title=post.get("title"),
                    text=comment_text,
                    author=c.get("author"),
                    score=c.get("score"),
                    created_utc=c.get("created_utc"),
                    created_iso=utc_to_iso(c.get("created_utc")),
                    permalink=c.get("permalink"),
                    parent_permalink=post.get("permalink"),
                    source_query=source_query,
                    source_category=source_category,
                    matched_categories=comment_categories,
                    matched_keywords=comment_keywords,
                )

                comment_payload = asdict(comment_message)
                comment_payloads.append(comment_payload)

                for cat in comment_categories:
                    messages_by_category[cat].append(comment_payload)
                    counts_by_category[cat] += 1

                total_comments_kept += 1

        envelope = PostEnvelope(
            post_id=post.get("post_id"),
            subreddit=post.get("subreddit"),
            title=post.get("title"),
            body=post.get("body"),
            author=post.get("author"),
            score=post.get("score"),
            created_utc=post.get("created_utc"),
            created_iso=post.get("created_iso"),
            permalink=post.get("permalink"),
            source_query=source_query,
            source_category=source_category,
            matched_categories=matched_categories,
            comment_count_extracted=len(comment_payloads),
            comments=comment_payloads,
        )
        records.append(envelope)

        jsonl_f.write(
            json.dumps({"event": "post", "envelope": asdict(envelope)}, ensure_ascii=False)
            + "\n"
        )
        jsonl_f.flush()
        flush_run_json("running")

    normal_exit = False
    try:
        for source_category, queries in category_config["category_queries"].items():
            for query in queries:
                sk = search_task_key(source_category, query)
                if sk in completed_search_tasks:
                    print(f"[SKIP] search already completed | {sk}")
                    continue
                print(f"[INFO] search_reddit | category={source_category} | query={query}")
                try:
                    search_results = backend.search_posts(
                        query=query,
                        limit=runtime_config["search_limit_per_query"]
                    )
                    for raw_post in search_results:
                        process_post(raw_post, source_query=query, source_category=source_category)
                    time.sleep(runtime_config["sleep_seconds"])
                    completed_search_tasks.add(sk)
                    flush_run_json("running")
                except Exception as e:
                    print(f"[WARN] search failed | query={query} | error={e}")
                    flush_run_json("interrupted")

        for subreddit in runtime_config["subreddits"]:
            if subreddit in completed_subreddit_tasks:
                print(f"[SKIP] subreddit already completed | {subreddit}")
                continue
            print(f"[INFO] fetch_subreddit_posts | subreddit={subreddit}")
            try:
                sub_posts = backend.fetch_subreddit_posts(
                    subreddit=subreddit,
                    limit=runtime_config["subreddit_fetch_limit"],
                    category=runtime_config["subreddit_fetch_category"],
                    time_filter=runtime_config["subreddit_time_filter"],
                )
                for raw_post in sub_posts:
                    process_post(
                        raw_post,
                        source_query=f"subreddit:{subreddit}",
                        source_category="subreddit_browse"
                    )
                time.sleep(runtime_config["sleep_seconds"])
                completed_subreddit_tasks.add(subreddit)
                flush_run_json("running")
            except Exception as e:
                print(f"[WARN] subreddit fetch failed | subreddit={subreddit} | error={e}")
                flush_run_json("interrupted")
        normal_exit = True
    except BaseException as exc:
        flush_run_json("interrupted")
        print(f"[ERROR] Run stopped: {exc}", file=sys.stderr)
        raise
    finally:
        if jsonl_f is not None:
            if normal_exit:
                jsonl_f.write(
                    json.dumps({"event": "run_end", "ended_at_utc": now_utc_iso()}, ensure_ascii=False)
                    + "\n"
                )
            else:
                jsonl_f.write(
                    json.dumps(
                        {"event": "run_interrupted", "at_utc": now_utc_iso()},
                        ensure_ascii=False,
                    )
                    + "\n"
                )
            jsonl_f.flush()
            jsonl_f.close()

    def sort_key(msg: Dict[str, Any]) -> int:
        return msg.get("created_utc") or 0

    for cat in list(messages_by_category.keys()):
        messages_by_category[cat] = sorted(messages_by_category[cat], key=sort_key, reverse=True)

    output = build_run_payload("complete")
    with run_json_path.open("w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    write_flat_csv_by_category(flat_csv_path, dict(messages_by_category))

    print(f"\n[INFO] Done")
    print(f"[INFO] Run JSON (single file): {run_json_path}")
    print(f"[INFO] JSONL stream: {jsonl_path}")
    print(f"[INFO] Flat CSV: {flat_csv_path}")
    print(json.dumps(output["stats"], indent=2))
    return output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Reddit with YARS and collate messages by category.")
    parser.add_argument(
        "--config",
        default="fraud_categories.json",
        help="Path to the external category JSON file."
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_CONFIG["output_dir"],
        help="Directory to write output JSON files."
    )
    parser.add_argument(
        "--search-limit",
        type=int,
        default=DEFAULT_CONFIG["search_limit_per_query"],
        help="Posts to fetch per search query."
    )
    parser.add_argument(
        "--subreddit-limit",
        type=int,
        default=DEFAULT_CONFIG["subreddit_fetch_limit"],
        help="Posts to fetch per subreddit browse."
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from the latest non-complete checkpoint in --output-dir.",
    )
    parser.add_argument(
        "--checkpoint",
        default=None,
        help="Explicit path to reddit_fraud_collated_<ts>.json (or legacy reddit_fraud_checkpoint_<ts>.json).",
    )
    parser.add_argument(
        "--force-resume",
        action="store_true",
        help="Resume even if fraud_categories.json fingerprint differs from the checkpoint.",
    )
    parser.add_argument(
        "--merge-jsons-only",
        action="store_true",
        help="Do not scrape; merge all reddit_fraud_collated_*.json in --output-dir into one by-category JSON + CSV.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.merge_jsons_only:
        jpath, cpath = export_merged_collated_jsons(Path(args.output_dir))
        print(f"[INFO] Merged by-category JSON: {jpath}")
        print(f"[INFO] Merged flat CSV: {cpath}")
        return

    runtime_config = dict(DEFAULT_CONFIG)
    runtime_config["output_dir"] = args.output_dir
    runtime_config["search_limit_per_query"] = args.search_limit
    runtime_config["subreddit_fetch_limit"] = args.subreddit_limit

    category_config = load_category_config(args.config)

    resume_path: Optional[Path] = None
    if args.checkpoint:
        resume_path = Path(args.checkpoint)
    elif args.resume:
        resume_path = find_latest_resumable_checkpoint(Path(args.output_dir))
        if resume_path is None:
            print(
                "[ERROR] No resumable run JSON found in output dir "
                "(need reddit_fraud_collated_*.json with metadata.checkpoint_status != complete, "
                "or a legacy reddit_fraud_checkpoint_*.json).",
                file=sys.stderr,
            )
            sys.exit(1)
        print(f"[INFO] Resuming from checkpoint file: {resume_path}")

    build_dataset(
        runtime_config,
        category_config,
        resume_checkpoint_path=resume_path,
        force_resume=args.force_resume,
    )


if __name__ == "__main__":
    main()
