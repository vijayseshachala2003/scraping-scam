# Reddit fraud collator (YARS)

Scrapes Reddit **without API keys** using [YARS](../YARS) (public `.json` endpoints), matches text against configurable fraud-related categories, and writes **one JSON file per run** with all collected material.

## Setup

1. Install YARS so `from yars.yars import YARS` works (upstream `yars/__init__.py` is often empty). Add the YARS **`src`** folder to `PYTHONPATH`, or install from that tree.
2. From this directory (or with paths adjusted), run:

```bash
python reddit_fraud_collator_yars_configurable.py
```

Options:

| Argument | Default | Meaning |
|----------|---------|---------|
| `--config` | `fraud_categories.json` | Category queries + keyword lists |
| `--output-dir` | `output` | Where the collated JSON is written |
| `--search-limit` | `40` | Max posts per search query |
| `--subreddit-limit` | `30` | Max posts per subreddit browse |
| `--resume` | off | Resume using the **newest** run JSON in `--output-dir` that is not `complete` |
| `--checkpoint` | — | Explicit path to `reddit_fraud_collated_<ts>.json` (or legacy `reddit_fraud_checkpoint_<ts>.json`) |
| `--force-resume` | off | Allow resume when `fraud_categories.json` (queries/keywords/subreddits/limits) no longer matches the checkpoint fingerprint |
| `--merge-jsons-only` | off | Merge all `reddit_fraud_collated_*.json` in `--output-dir` into one by-category JSON + CSV (no scraping) |

Runtime defaults (subreddit list, sleep, sort filters, etc.) live in `DEFAULT_CONFIG` inside `reddit_fraud_collator_yars_configurable.py`.

## Configuration (`fraud_categories.json`)

Two top-level objects are **required**:

- **`category_queries`** — Maps a **category id** (e.g. `phishing_cyber`) to a list of **Reddit search strings**. Each query is run through `search_reddit`; hits inherit that category as **`source_category`** when keyword matching would otherwise assign nothing (fallback).
- **`category_keywords`** — Same category ids → keyword **phrases**. Each phrase becomes a word-boundary regex; if any phrase in a category matches the normalized post or comment text, that category is recorded in **`matched_categories`** (and the hits in **`matched_keywords`**).

Classification is **lexical** (regex), not ML.

## How data is stored (output files)

Each run shares one timestamp `YYYYMMDD_HHMMSS` and writes **three** artifacts under `{output_dir}` (default `output/`):

| File | Purpose |
|------|---------|
| `reddit_fraud_collated_<ts>.json` | **Single JSON for everything**: full dataset (`stats`, `counts_by_category`, `messages_by_category`, `records`) plus **`metadata.resume`** (seen permalinks, completed search/subreddit tasks, `config_fingerprint`) and **`metadata.checkpoint_status`**. This file is **rewritten after every processed post** while running, then **`complete`** once finished. Use it for `--resume` and live progress. |
| `reddit_fraud_collated_<ts>.jsonl` | **Append-only JSON Lines**: `run_start`, `resume` (if applicable), one line per post (`post`), then `run_end` or `run_interrupted`. |
| `reddit_fraud_flat_<ts>.csv` | **Flat table**: one row per message in each category bucket (`bucket_category`, `text`, permalinks, etc.). |

**Legacy** files from older runs (`reddit_fraud_checkpoint_<ts>.json`, `reddit_fraud_live_<ts>.json`, …) are no longer produced; `--resume` still accepts an old **`reddit_fraud_checkpoint_*.json`** path if needed.

### Merging multiple `reddit_fraud_collated_*.json` files

After several runs you may have multiple final JSONs in the same folder. To combine them into **one** by-category JSON and **one** CSV (deduplicating rows per category across files):

```bash
python reddit_fraud_collator_yars_configurable.py --merge-jsons-only --output-dir output
```

This writes `reddit_fraud_merged_all_<merge_ts>.json` and `reddit_fraud_merged_all_<merge_ts>.csv`.

### Checkpoint and resume

- **Fingerprint**: The script hashes `category_queries`, `category_keywords`, `subreddits`, and fetch limits. If you change the config, **`--resume` refuses** unless you pass **`--force-resume`** (use with care).
- **After a failure**: Fix the issue (network, 429, etc.), then run:
  - `python reddit_fraud_collator_yars_configurable.py --resume --output-dir output`  
  or point to a specific file:  
  - `python reddit_fraud_collator_yars_configurable.py --checkpoint output/reddit_fraud_collated_20260327_120000.json`
- **Behavior**: Already-finished **search queries** and **subreddits** are skipped. Already-seen **post permalinks** are skipped, so partially processed search results are safe to re-fetch. New rows are appended to the same **`jsonl`** for that run.

Default delay between detail fetches is **`sleep_seconds`: 3.0** (helps with Reddit **429** rate limits). Increase further if you still see throttling.

The main JSON document has these **top-level sections**:

### 1. `metadata`

Run context: `generated_at_utc`, `run_started_at_utc`, `run_status`, **`checkpoint_status`** (`running` / `interrupted` / `complete`), **`resume`** (dedupe keys + completed tasks), **`config_fingerprint`**, backend name, limits, and **`output_paths`** (`json`, `jsonl`, `flat_csv`).

### 2. `stats`

Aggregates: posts examined, posts kept, comments kept, number of category buckets, total message rows across all buckets.

### 3. `counts_by_category`

Per category id, how many **message rows** were appended (a post body counts as one row; each matching comment counts as another). Useful for quick distribution checks.

### 4. `messages_by_category`

The **primary bucketed store**: an object whose keys are **category ids**. Each value is a **list of message records** (see below), sorted by `created_utc` descending (missing timestamps sort as 0).

The same post or comment can appear under **multiple** categories if multiple keyword groups matched.

### 5. `records`

A **flat list of per-post envelopes** in processing order. Each item is one Reddit thread we touched:

| Field | Purpose |
|-------|---------|
| Post metadata | `post_id`, `subreddit`, `title`, `body`, `author`, `score`, `created_utc`, `created_iso`, `permalink` |
| Provenance | `source_query` (search string or `subreddit:name`), `source_category` (from config or `subreddit_browse`) |
| Classification | `matched_categories` for the **post** title+body |
| `comment_count_extracted` | How many comment rows we flattened |
| `comments` | List of **comment message records** (same shape as in `messages_by_category`, but scoped to this post) |

Use **`messages_by_category`** for analysis by fraud theme; use **`records`** when you need full thread context or to trace a post end-to-end.

### Message record shape (posts and comments)

Each object in `messages_by_category[...]` and each entry in `records[].comments` shares the same structure:

| Field | Description |
|-------|-------------|
| `record_type` | `"post"` or `"comment"` |
| `reddit_post_id` / `reddit_comment_id` | Reddit ids when known |
| `subreddit`, `title` | Subreddit and post title (title repeated on comments for context) |
| `text` | Post body or comment body |
| `author`, `score`, `created_utc`, `created_iso` | When provided by YARS |
| `permalink` | Post or comment permalink path when available |
| `parent_permalink` | For comments, the **post** permalink |
| `source_query`, `source_category` | How this row was discovered |
| `matched_categories` | Category ids that matched this text |
| `matched_keywords` | Map category id → list of keyword phrases that fired |

## Search results and permalinks

YARS `search_reddit` returns posts with a **`link`** (full URL) rather than a bare **`permalink`**. This script converts any `http(s)` `link` into a **path** (`/r/.../comments/.../`) so `scrape_post_details` runs and deduplication by permalink works. Search snippets also use the **`description`** field as initial body text until the full post is fetched.

## Ethics and limits

Scraping is subject to Reddit’s terms and rate limits. The script sleeps between requests; use modest limits and consider proxies for large runs (see YARS README).
