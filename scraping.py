"""
One-shot scrape + dataset ingest → XML first, then JSON.

Flow: collect (rich extraction) → output/scam_data.xml → output/scam_data_normalized.json

Scope (only these sources):
  • UCI SMS Spam Collection (dataset page: archive.ics.uci.edu/dataset/228)
  • Kaggle NUS SMS Corpus — requires manual CSV export → data/nus_sms_corpus.csv
  • Scamwatch (AU), ScamShield (SG), NCSC (UK), Canadian Anti-Fraud Centre

Failed URLs/sources are listed in output/fetch_failures.json; the run continues for the rest.

If you hit blocks (403, geo, bot protection, empty CAPTCHA pages):
  • Set SCRAPE_DO_TOKEN from https://scrape.do — failed direct requests retry via scrape.do.
  • Optional: SCRAPE_DO_ALWAYS=1 to send every URL through scrape.do (skip direct first).
  • Alias env: SCRAPE_DO_API_TOKEN (same as SCRAPE_DO_TOKEN).
"""

import hashlib
import io
import json
import os
import re
import xml.etree.ElementTree as ET
import zipfile
from urllib.parse import quote, urljoin
from datetime import datetime, timezone

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────

OUTPUT_DIR = "output"
FAILURES_JSON = os.path.join(OUTPUT_DIR, "fetch_failures.json")
XML_OUTPUT = os.path.join(OUTPUT_DIR, "scam_data.xml")
MAX_RAW_HTML_CHARS = 512 * 1024  # cap per page for XML size
MAX_LINKS_PER_PAGE = 300
os.makedirs(OUTPUT_DIR, exist_ok=True)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; ScamDataBot/1.0; +research)"}

# Official UCI download (matches dataset 228 page).
UCI_ZIP_URL = "https://archive.ics.uci.edu/static/public/228/sms+spam+collection.zip"
# Inside the ZIP the messages file is named SMSSpamCollection (tab-separated label + message).

SOURCES = {
    "datasets": [
        {
            "name": "UCI SMS Spam Collection",
            "dataset_page": "https://archive.ics.uci.edu/dataset/228/sms+spam+collection",
            "url": UCI_ZIP_URL,
            "format": "uci_zip",
            "country": "Global",
        },
        {
            "name": "NUS SMS Corpus (Kaggle export)",
            "dataset_page": "https://www.kaggle.com/datasets/rtatman/the-national-university-of-singapore-sms-corpus",
            "url": None,
            "local_path": "data/nus_sms_corpus.csv",
            "format": "csv",
            "country": "Singapore",
        },
    ],
    "websites": [
        {
            "name": "Scamwatch",
            "url": "https://www.scamwatch.gov.au/",
            "country": "Australia",
        },
        {
            "name": "ScamShield",
            "url": "https://www.scamshield.gov.sg/",
            "country": "Singapore",
        },
        {
            "name": "NCSC UK",
            "url": "https://www.ncsc.gov.uk/",
            "country": "UK",
        },
        {
            "name": "Canadian Anti-Fraud Centre",
            "url": "https://www.antifraudcentre-centreantifraude.ca/",
            "country": "Canada",
        },
    ],
}

# ──────────────────────────────────────────────
# COLLECT
# ──────────────────────────────────────────────


def _failure_record(kind, source_name, url, error, stage="fetch"):
    return {
        "kind": kind,
        "source_name": source_name,
        "url": url or "",
        "stage": stage,
        "error": str(error),
        "at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def _scrape_do_token():
    return os.environ.get("SCRAPE_DO_TOKEN") or os.environ.get("SCRAPE_DO_API_TOKEN")


def _fetch_via_scrape_do(url, timeout):
    token = _scrape_do_token()
    if not token:
        return None
    api_url = f"https://api.scrape.do/?token={token}&url={quote(url, safe='')}"
    return requests.get(api_url, timeout=timeout)


def fetch_url(url, timeout=30):
    """GET url (direct first). Returns (response, None) or (None, error_message)."""
    token = _scrape_do_token()
    always = os.environ.get("SCRAPE_DO_ALWAYS", "").lower() in ("1", "true", "yes")

    if token and always:
        try:
            response = _fetch_via_scrape_do(url, timeout)
            if response is None:
                return None, "scrape.do: no token path (internal)"
            response.raise_for_status()
            return response, None
        except Exception as e:
            return None, f"scrape.do (SCRAPE_DO_ALWAYS): {e}"

    err_direct = None
    try:
        response = requests.get(url, headers=HEADERS, timeout=timeout)
        response.raise_for_status()
        return response, None
    except Exception as e:
        err_direct = e

    if not token:
        return None, f"direct: {err_direct}"

    try:
        print("  [RETRY] Direct failed; trying scrape.do …")
        response = _fetch_via_scrape_do(url, timeout)
        if response is None:
            return None, f"direct: {err_direct}; scrape.do: no response"
        response.raise_for_status()
        return response, None
    except Exception as e2:
        return None, f"direct: {err_direct}; scrape.do: {e2}"


def _xml_safe(text):
    """Strip characters illegal in XML 1.0 text nodes."""
    if text is None:
        return ""
    out = []
    for c in str(text):
        o = ord(c)
        if o in (9, 10, 13) or o >= 32:
            if o <= 0xD7FF or (0xE000 <= o <= 0xFFFD):
                out.append(c)
    return "".join(out)


def _build_rich_page_record(soup, source, resp):
    """One record per page with headings, links, meta, HTML snapshot, and text blocks."""
    raw_text = resp.text or ""
    truncated = len(raw_text) > MAX_RAW_HTML_CHARS
    raw_html = raw_text[:MAX_RAW_HTML_CHARS] if truncated else raw_text

    meta_description = ""
    md = soup.find("meta", attrs={"name": re.compile(r"^description$", re.I)})
    if md and md.get("content"):
        meta_description = (md.get("content") or "")[:8000]

    meta_title = ""
    og = soup.find("meta", property="og:title")
    if og and og.get("content"):
        meta_title = (og.get("content") or "").strip()
    if not meta_title:
        tt = soup.find("title")
        if tt:
            meta_title = tt.get_text(strip=True)[:2000]

    headings = []
    for hx in range(1, 7):
        for h in soup.find_all(f"h{hx}"):
            t = h.get_text(strip=True)
            if t:
                headings.append({"level": hx, "text": t[:4000]})

    links = []
    for a in soup.find_all("a", href=True):
        if len(links) >= MAX_LINKS_PER_PAGE:
            break
        href = (a.get("href") or "").strip()
        if not href or href.startswith("#") or href.lower().startswith("javascript:"):
            continue
        full = urljoin(source["url"], href)
        links.append({"href": full, "text": a.get_text(strip=True)[:500]})

    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    articles = soup.find_all(
        ["article", "section", "div"],
        class_=re.compile(r"(news|article|post|content|alert|hero|banner|intro|main)", re.I),
    )
    if not articles:
        articles = [soup.find("main") or soup.find("body") or soup]

    blocks = []
    seen = set()
    for article in articles[:40]:
        title_tag = article.find(["h1", "h2", "h3"])
        title = title_tag.get_text(strip=True) if title_tag else ""
        paragraphs = article.find_all("p")
        content = " ".join(p.get_text(strip=True) for p in paragraphs)
        link_tag = article.find("a", href=True)
        link = link_tag["href"] if link_tag else source["url"]
        if link and not link.startswith("http"):
            link = urljoin(source["url"], link)

        key = (title[:200], content[:500])
        if key in seen:
            continue
        seen.add(key)
        if title or content:
            blocks.append(
                {
                    "title": title or source["name"],
                    "content": content,
                    "link": link or source["url"],
                }
            )

    content_parts = [b["content"] for b in blocks if b.get("content")]
    full_content = "\n\n".join(content_parts)
    if not full_content.strip():
        body = soup.find("body")
        blob = body.get_text(" ", strip=True) if body else soup.get_text(" ", strip=True)
        full_content = blob[:100000]

    lists_text = []
    for ul in soup.find_all(["ul", "ol"])[:80]:
        for li in ul.find_all("li", recursive=False):
            t = li.get_text(" ", strip=True)
            if t:
                lists_text.append(t)
    if lists_text:
        full_content = (full_content + "\n\n" + "\n".join(lists_text[:500]))[:120000]

    return {
        "title": meta_title or source["name"],
        "content": full_content[:120000],
        "category": "scam-alert",
        "source_url": source["url"],
        "publish_date": None,
        "country": source["country"],
        "tags": ["scraped", "website", "rich"],
        "source_name": source["name"],
        "kind": "website",
        "extracted": {
            "meta_title": meta_title,
            "meta_description": meta_description,
            "headings": headings,
            "links": links,
            "blocks": blocks,
            "raw_html": _xml_safe(raw_html),
            "raw_html_truncated": truncated,
            "http_status": resp.status_code,
            "content_type": (resp.headers.get("Content-Type") or "")[:200],
        },
    }


def _parse_uci_smsspam_lines(text):
    """Each line: <label>\\t<message> (ham/spam + SMS text)."""
    records = []
    for line in text.strip().split("\n"):
        parts = line.split("\t", 1)
        if len(parts) < 2:
            continue
        label, body = parts[0].strip(), parts[1].strip()
        records.append(
            {
                "title": label,
                "content": body,
                "category": label,
                "source_url": UCI_ZIP_URL,
                "publish_date": None,
                "country": "Global",
                "tags": ["dataset", "sms"],
                "source_name": "UCI SMS Spam Collection",
                "kind": "dataset",
            }
        )
    return records


def collect_uci_zip(source, failures):
    records = []
    print(f"  [DATASET] Downloading ZIP: {source['url']}")
    resp, err = fetch_url(source["url"])
    if err:
        failures.append(_failure_record("dataset", source["name"], source["url"], err))
        print(f"  [FAIL] {err}")
        return records
    try:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            inner = None
            for name in zf.namelist():
                if name.rstrip("/").endswith("SMSSpamCollection"):
                    inner = name
                    break
            if not inner:
                msg = "SMSSpamCollection not found inside ZIP"
                failures.append(
                    _failure_record("dataset", source["name"], source["url"], msg, stage="parse")
                )
                print(f"  [FAIL] {msg}")
                return records
            with zf.open(inner) as f:
                text = f.read().decode("utf-8", errors="replace")
        records = _parse_uci_smsspam_lines(text)
        print(f"    → {len(records)} SMS rows from UCI")
    except Exception as e:
        failures.append(_failure_record("dataset", source["name"], source["url"], e, stage="parse"))
        print(f"  [FAIL] UCI ZIP parse: {e}")
    return records


def collect_local_csv(source, failures):
    """Kaggle export: place CSV at local_path (export from Kaggle UI)."""
    records = []
    local = source["local_path"]
    if not os.path.exists(local):
        print(f"  [SKIP] Place Kaggle export CSV at {local} (see dataset page in SOURCES)")
        return records
    print(f"  [DATASET] Reading: {local}")
    try:
        df = pd.read_csv(local, encoding="utf-8", on_bad_lines="skip")
        cols = {c.lower(): c for c in df.columns}
        text_col = None
        for key in ("text", "message", "sms", "content", "body"):
            if key in cols:
                text_col = cols[key]
                break
        if text_col is None:
            text_col = df.columns[-1]

        label_col = None
        for key in ("label", "class", "category", "spam", "type"):
            if key in cols:
                label_col = cols[key]
                break

        for _, row in df.iterrows():
            msg = str(row.get(text_col, "") or "").strip()
            if not msg:
                continue
            label = str(row.get(label_col, "unknown") if label_col else "unknown")
            row_dict = {str(c): row.get(c) for c in df.columns}
            records.append(
                {
                    "title": label[:80] if label else "sms",
                    "content": msg,
                    "category": label,
                    "source_url": local,
                    "publish_date": None,
                    "country": source["country"],
                    "tags": ["dataset", "sms", "nus"],
                    "source_name": source["name"],
                    "kind": "dataset",
                    "csv_row": row_dict,
                }
            )
        print(f"    → {len(records)} rows from NUS CSV")
    except Exception as e:
        failures.append(_failure_record("dataset", source["name"], local, e, stage="read"))
        print(f"  [FAIL] Could not read {local}: {e}")
    return records


def collect_dataset(source, failures):
    fmt = source.get("format", "csv")
    if fmt == "uci_zip":
        return collect_uci_zip(source, failures)
    if fmt == "csv":
        return collect_local_csv(source, failures)
    return []


def collect_website(source, failures):
    """Scrape homepage once with rich extraction (meta, headings, links, HTML snapshot, blocks)."""
    records = []
    print(f"  [SCRAPE] {source['url']}")
    resp, err = fetch_url(source["url"])
    if err:
        failures.append(_failure_record("website", source["name"], source["url"], err))
        print(f"  [FAIL] {err}")
        return records

    try:
        soup = BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        failures.append(_failure_record("website", source["name"], source["url"], e, stage="parse"))
        print(f"  [FAIL] HTML parse: {e}")
        return records
    try:
        rec = _build_rich_page_record(soup, source, resp)
        records.append(rec)
    except Exception as e:
        failures.append(_failure_record("website", source["name"], source["url"], e, stage="extract"))
        print(f"  [FAIL] Extract: {e}")
        return records

    ex = rec.get("extracted") or {}
    nblocks = len(ex.get("blocks") or [])
    nlinks = len(ex.get("links") or [])
    print(f"    → 1 rich page ({nblocks} blocks, {nlinks} links, {len(ex.get('headings') or [])} headings)")
    return records


# ──────────────────────────────────────────────
# SCHEMA & NORMALIZE
# ──────────────────────────────────────────────

REQUIRED_FIELDS = [
    "title",
    "content",
    "category",
    "source_url",
    "publish_date",
    "country",
    "tags",
    "source_name",
]


def extract_fields(record):
    out = {}
    for field in REQUIRED_FIELDS:
        if field == "tags":
            t = record.get("tags", [])
            out[field] = t if isinstance(t, list) else []
        else:
            out[field] = record.get(field, "")
    return out


CATEGORY_MAP = {
    "spam": "spam",
    "scam": "scam",
    "phish": "phishing",
    "phishing": "phishing",
    "fraud": "fraud",
    "malware": "malware",
    "ham": "legitimate",
    "legitimate": "legitimate",
    "scam-alert": "scam",
    "unknown": "unknown",
}


def normalize_category(cat):
    cat = str(cat).lower().strip()
    for key, value in CATEGORY_MAP.items():
        if key in cat:
            return value
    return "unknown"


def clean_text(text):
    text = str(text).strip()
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^\x00-\x7F]+", "", text)
    text = re.sub(r"<[^>]+>", "", text)
    return text.strip()


def normalize_date(date_str):
    if not date_str:
        return None
    for fmt in (
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%B %d, %Y",
        "%a, %d %b %Y %H:%M:%S %z",
        "%Y-%m-%dT%H:%M:%SZ",
    ):
        try:
            return datetime.strptime(str(date_str).strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return str(date_str).strip() or None


def normalize_url(url):
    url = str(url).strip()
    if url and not url.startswith("http"):
        url = "https://" + url
    return url


def clean_and_normalize(record):
    out = {
        "title": clean_text(record.get("title", "")),
        "content": clean_text(record.get("content", "")),
        "category": normalize_category(record.get("category", "")),
        "source_url": normalize_url(record.get("source_url", "")),
        "publish_date": normalize_date(record.get("publish_date")),
        "country": str(record.get("country", "")).strip(),
        "tags": record.get("tags", []),
        "source_name": str(record.get("source_name", "")).strip(),
        "ingested_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    if record.get("kind"):
        out["kind"] = str(record["kind"]).strip()
    if record.get("csv_row") is not None:
        out["csv_row"] = record["csv_row"]
    if record.get("extracted"):
        ex = record["extracted"]
        out["extracted"] = {
            "meta_title": ex.get("meta_title", ""),
            "meta_description": ex.get("meta_description", ""),
            "headings": ex.get("headings") or [],
            "links": ex.get("links") or [],
            "blocks": ex.get("blocks") or [],
            "raw_html_truncated": bool(ex.get("raw_html_truncated")),
            "http_status": ex.get("http_status"),
            "content_type": ex.get("content_type", ""),
            "raw_html": ex.get("raw_html", ""),
        }
    return out


def generate_id(record):
    key = (record.get("source_url", "") + record.get("title", "")).encode()
    return hashlib.md5(key).hexdigest()


def _el_text(el, tag):
    c = el.find(tag) if el is not None else None
    if c is None:
        return ""
    return c.text if c.text is not None else ""


def write_records_xml(all_records, failures, path):
    root = ET.Element("ingest")
    root.set("version", "1")
    meta = ET.SubElement(root, "metadata")
    m = ET.SubElement(meta, "generated_at")
    m.text = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    ET.SubElement(meta, "format").text = "scam_ingest_v1"

    froot = ET.SubElement(root, "failures")
    for fail in failures:
        fe = ET.SubElement(froot, "failure")
        for k, v in fail.items():
            n = ET.SubElement(fe, k)
            n.text = _xml_safe(v)

    recs = ET.SubElement(root, "records")
    for i, rec in enumerate(all_records):
        r = ET.SubElement(recs, "record")
        r.set("index", str(i))
        for key in (
            "kind",
            "title",
            "content",
            "category",
            "source_url",
            "country",
            "source_name",
        ):
            n = ET.SubElement(r, key)
            n.text = _xml_safe(rec.get(key, ""))
        pd = ET.SubElement(r, "publish_date")
        pd.text = _xml_safe(rec.get("publish_date") or "")

        tags = rec.get("tags") or []
        tg = ET.SubElement(r, "tags")
        for t in tags:
            te = ET.SubElement(tg, "tag")
            te.text = _xml_safe(t)

        if rec.get("csv_row") is not None:
            cr = ET.SubElement(r, "csv_row_json")
            cr.text = _xml_safe(json.dumps(rec["csv_row"], default=str))

        ex = rec.get("extracted")
        if ex:
            ex_el = ET.SubElement(r, "extracted")
            for k in ("meta_title", "meta_description", "content_type"):
                n = ET.SubElement(ex_el, k)
                n.text = _xml_safe(ex.get(k, ""))
            hs = ET.SubElement(ex_el, "http_status")
            hs.text = str(ex.get("http_status", ""))

            rh = ET.SubElement(ex_el, "raw_html")
            if ex.get("raw_html_truncated"):
                rh.set("truncated", "true")
            rh.text = _xml_safe(ex.get("raw_html", ""))

            he = ET.SubElement(ex_el, "headings")
            for h in ex.get("headings") or []:
                hh = ET.SubElement(he, "heading")
                hh.set("level", str(h.get("level", 1)))
                hh.text = _xml_safe(h.get("text", ""))

            le = ET.SubElement(ex_el, "links")
            for lk in ex.get("links") or []:
                ln = ET.SubElement(le, "link")
                ln.set("href", _xml_safe(lk.get("href", "")))
                ln.set("text", _xml_safe(lk.get("text", "")))

            be = ET.SubElement(ex_el, "blocks")
            for b in ex.get("blocks") or []:
                blk = ET.SubElement(be, "block")
                for bk in ("title", "content", "link"):
                    bn = ET.SubElement(blk, bk)
                    bn.text = _xml_safe(b.get(bk, ""))

    tree = ET.ElementTree(root)
    try:
        ET.indent(tree, space="  ")
    except AttributeError:
        pass
    tree.write(path, encoding="utf-8", xml_declaration=True)


def read_records_xml(path):
    tree = ET.parse(path)
    root = tree.getroot()
    recs_el = root.find("records")
    if recs_el is None:
        return []
    out = []
    for r in recs_el.findall("record"):
        rec = {
            "kind": _el_text(r, "kind"),
            "title": _el_text(r, "title"),
            "content": _el_text(r, "content"),
            "category": _el_text(r, "category"),
            "source_url": _el_text(r, "source_url"),
            "publish_date": _el_text(r, "publish_date") or None,
            "country": _el_text(r, "country"),
            "source_name": _el_text(r, "source_name"),
        }
        tg = r.find("tags")
        if tg is not None:
            rec["tags"] = [t.text or "" for t in tg.findall("tag")]
        else:
            rec["tags"] = []

        cr = r.find("csv_row_json")
        if cr is not None and (cr.text or "").strip():
            try:
                rec["csv_row"] = json.loads(cr.text)
            except json.JSONDecodeError:
                rec["csv_row"] = {}

        ex = r.find("extracted")
        if ex is not None:
            extracted = {
                "meta_title": _el_text(ex, "meta_title"),
                "meta_description": _el_text(ex, "meta_description"),
                "content_type": _el_text(ex, "content_type"),
            }
            hs = ex.find("http_status")
            try:
                extracted["http_status"] = int((hs.text or "0").strip() or 0)
            except ValueError:
                extracted["http_status"] = 0
            rh = ex.find("raw_html")
            extracted["raw_html"] = rh.text if rh is not None and rh.text else ""
            extracted["raw_html_truncated"] = rh.get("truncated", "").lower() == "true" if rh is not None else False

            headings = []
            he = ex.find("headings")
            if he is not None:
                for hh in he.findall("heading"):
                    try:
                        lev = int(hh.get("level", "1"))
                    except ValueError:
                        lev = 1
                    headings.append({"level": lev, "text": hh.text or ""})
            extracted["headings"] = headings

            links = []
            le = ex.find("links")
            if le is not None:
                for ln in le.findall("link"):
                    links.append({"href": ln.get("href", ""), "text": ln.get("text", "")})
            extracted["links"] = links

            blocks = []
            be = ex.find("blocks")
            if be is not None:
                for blk in be.findall("block"):
                    blocks.append(
                        {
                            "title": _el_text(blk, "title"),
                            "content": _el_text(blk, "content"),
                            "link": _el_text(blk, "link"),
                        }
                    )
            extracted["blocks"] = blocks
            rec["extracted"] = extracted

        out.append(rec)
    return out


def run_pipeline():
    failures = []
    all_records = []

    print("\n=== Collect (one run, continue on errors) ===")

    for source in SOURCES["datasets"]:
        print(f"\n[DATASET] {source['name']}")
        all_records.extend(collect_dataset(source, failures))

    for source in SOURCES["websites"]:
        print(f"\n[WEBSITE] {source['name']}")
        all_records.extend(collect_website(source, failures))

    print(f"\n=== Write XML ({len(all_records)} records) ===")
    write_records_xml(all_records, failures, XML_OUTPUT)
    print(f"    → {XML_OUTPUT}")

    print("\n=== Read XML → normalize → JSON ===")
    from_xml = read_records_xml(XML_OUTPUT)
    normalized = []
    for rec in from_xml:
        merged = {**rec, **extract_fields(rec)}
        cleaned = clean_and_normalize(merged)
        cleaned["id"] = generate_id(cleaned)
        normalized.append(cleaned)

    output_path = os.path.join(OUTPUT_DIR, "scam_data_normalized.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(normalized, f, indent=2, ensure_ascii=False, default=str)

    with open(FAILURES_JSON, "w", encoding="utf-8") as f:
        json.dump(failures, f, indent=2, ensure_ascii=False)

    print(f"\nDone. {len(normalized)} records → {output_path} (from XML)")
    print(f"     XML → {XML_OUTPUT}")
    print(f"     Failures log → {FAILURES_JSON} ({len(failures)} entr{'y' if len(failures) == 1 else 'ies'})")
    if failures and not _scrape_do_token():
        print(
            "  [HINT] Some fetches failed. For bot/geo blocks, set SCRAPE_DO_TOKEN from https://scrape.do and re-run."
        )
    return normalized


if __name__ == "__main__":
    run_pipeline()
