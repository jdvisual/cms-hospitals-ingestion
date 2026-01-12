#!/usr/bin/env python3
"""
CMS Provider Data (Hospitals theme) incremental downloader + header normalizer.

- Lists all datasets from:
  https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items
- Filters theme == "Hospitals"
- Downloads CSV distributions in parallel
- Rewrites CSV headers to snake_case while streaming rows (memory-safe for large files)
- Tracks last successful run in a local SQLite DB (state.db by default)

Important threading note:
- Worker threads do NOT touch SQLite.
- Main thread handles all DB reads/writes to avoid SQLite thread restrictions on Windows.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import logging
import re
import sqlite3
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests


METASTORE_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
THEME_NAME = "Hospitals"


# ----------------------------
# Helpers: header normalization
# ----------------------------
_SMART_APOSTROPHES = {
    "\u2019": "'",  # right single quote
    "\u2018": "'",  # left single quote
    "\u02bc": "'",  # modifier letter apostrophe
}


def to_snake_case(name: str) -> str:
    """
    Convert a column name to snake_case:
      - normalizes unicode
      - replaces smart quotes
      - drops punctuation/special chars
      - collapses whitespace/underscores
      - strips leading/trailing underscores

    Example:
      "Patients’ rating of the facility linear mean score"
        -> "patients_rating_of_the_facility_linear_mean_score"
    """
    if name is None:
        return ""

    s = unicodedata.normalize("NFKC", str(name))
    for k, v in _SMART_APOSTROPHES.items():
        s = s.replace(k, v)

    s = s.lower()
    s = s.replace("/", " ").replace("-", " ").replace("\u00a0", " ")

    # Keep alnum + whitespace + underscore only
    s = re.sub(r"[^a-z0-9_\s]+", "", s)

    # Whitespace -> underscore, collapse underscores
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"_+", "_", s)

    return s.strip("_")


# ----------------------------
# State management (SQLite)
# ----------------------------
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  started_at_utc TEXT NOT NULL,
  finished_at_utc TEXT,
  status TEXT NOT NULL,         -- RUNNING, SUCCESS, FAILED
  theme TEXT NOT NULL,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS dataset_state (
  identifier TEXT PRIMARY KEY,
  title TEXT,
  theme TEXT,
  modified TEXT,                -- dataset-level modified date string from metastore
  last_downloaded_at_utc TEXT,
  last_output_path TEXT,
  last_error TEXT
);
"""


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def parse_metastore_modified(s: str) -> dt.datetime:
    """
    Metastore 'modified' is typically YYYY-MM-DD.
    Treat as UTC midnight. If parsing fails, return epoch.
    """
    try:
        if isinstance(s, str) and len(s) == 10 and s[4] == "-" and s[7] == "-":
            d = dt.datetime.strptime(s, "%Y-%m-%d").date()
            return dt.datetime(d.year, d.month, d.day, tzinfo=dt.timezone.utc)
        return dt.datetime.fromisoformat(str(s).replace("Z", "+00:00")).astimezone(dt.timezone.utc)
    except Exception:
        return dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)


class StateDB:
    """
    SQLite state DB used ONLY from the main thread.
    """

    def __init__(self, path: Path):
        self.path = path
        self.conn = sqlite3.connect(str(path))
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.executescript(SCHEMA_SQL)
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def start_run(self, theme: str) -> int:
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO runs(started_at_utc, status, theme) VALUES (?, 'RUNNING', ?)",
            (utc_now_iso(), theme),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def finish_run(self, run_id: int, status: str, notes: Optional[str] = None) -> None:
        self.conn.execute(
            "UPDATE runs SET finished_at_utc = ?, status = ?, notes = ? WHERE id = ?",
            (utc_now_iso(), status, notes, run_id),
        )
        self.conn.commit()

    def last_success_time(self, theme: str) -> Optional[dt.datetime]:
        cur = self.conn.cursor()
        cur.execute(
            "SELECT finished_at_utc FROM runs WHERE theme = ? AND status = 'SUCCESS' ORDER BY id DESC LIMIT 1",
            (theme,),
        )
        row = cur.fetchone()
        if not row or not row[0]:
            return None
        return dt.datetime.fromisoformat(row[0]).astimezone(dt.timezone.utc)

    def upsert_dataset(
        self,
        identifier: str,
        title: str,
        theme: str,
        modified: str,
        last_downloaded_at_utc: Optional[str],
        last_output_path: Optional[str],
        last_error: Optional[str],
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO dataset_state(identifier, title, theme, modified, last_downloaded_at_utc, last_output_path, last_error)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(identifier) DO UPDATE SET
              title = excluded.title,
              theme = excluded.theme,
              modified = excluded.modified,
              last_downloaded_at_utc = excluded.last_downloaded_at_utc,
              last_output_path = excluded.last_output_path,
              last_error = excluded.last_error
            """,
            (identifier, title, theme, modified, last_downloaded_at_utc, last_output_path, last_error),
        )
        self.conn.commit()


# ----------------------------
# CMS download + processing
# ----------------------------
@dataclass(frozen=True)
class DatasetDownloadTask:
    identifier: str
    title: str
    modified: str
    download_url: str


def iter_hospital_csv_tasks(items: Iterable[Dict[str, Any]]) -> List[DatasetDownloadTask]:
    tasks: List[DatasetDownloadTask] = []
    for it in items:
        themes = it.get("theme") or []
        if THEME_NAME not in themes:
            continue

        identifier = it.get("identifier")
        title = it.get("title") or ""
        modified = it.get("modified") or ""

        for dist in it.get("distribution") or []:
            if (dist.get("mediaType") or "").lower() != "text/csv":
                continue
            url = dist.get("downloadURL")
            if not url:
                continue

            tasks.append(
                DatasetDownloadTask(
                    identifier=str(identifier),
                    title=str(title),
                    modified=str(modified),
                    download_url=str(url),
                )
            )
    return tasks


def safe_filename(s: str, max_len: int = 160) -> str:
    s = unicodedata.normalize("NFKC", str(s))
    s = re.sub(r"[^\w\s\-]+", "", s).strip()
    s = re.sub(r"\s+", "_", s).strip("_")
    return (s or "dataset")[:max_len]


def download_file(session: requests.Session, url: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with session.get(url, stream=True, timeout=180) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def transform_csv_headers_streaming(in_path: Path, out_path: Path) -> Tuple[List[str], List[str], int]:
    """
    Stream input CSV to output CSV:
      - rewrite header row to snake_case
      - copy all remaining rows unchanged
    Returns: (original_headers, new_headers, row_count_written)
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with open(in_path, "r", newline="", encoding="utf-8-sig") as fin:
        reader = csv.reader(fin)
        try:
            original_headers = next(reader)
        except StopIteration:
            original_headers = []

        new_headers = [to_snake_case(h) for h in original_headers]

        with open(out_path, "w", newline="", encoding="utf-8") as fout:
            writer = csv.writer(fout)
            writer.writerow(new_headers)

            row_count = 0
            for row in reader:
                writer.writerow(row)
                row_count += 1

    return original_headers, new_headers, row_count


def should_download(task: DatasetDownloadTask, last_success: Optional[dt.datetime]) -> bool:
    if last_success is None:
        return True
    m = parse_metastore_modified(task.modified)
    return m > last_success


def process_one_task(
    session: requests.Session,
    task: DatasetDownloadTask,
    raw_dir: Path,
    processed_dir: Path,
) -> Dict[str, Any]:
    """
    Worker-thread function: downloads + transforms only.
    NO SQLITE CALLS in this function.
    """
    title_slug = safe_filename(task.title)
    raw_path = raw_dir / f"{task.identifier}__{title_slug}.csv"
    out_path = processed_dir / f"{task.identifier}__{title_slug}__snake.csv"
    map_path = processed_dir / f"{task.identifier}__{title_slug}__header_map.json"

    try:
        logging.info("Downloading %s | %s", task.identifier, task.title)
        download_file(session, task.download_url, raw_path)

        logging.info("Transforming headers: %s", raw_path.name)
        original_headers, new_headers, rows = transform_csv_headers_streaming(raw_path, out_path)

        header_map = dict(zip(original_headers, new_headers))
        with open(map_path, "w", encoding="utf-8") as f:
            json.dump(header_map, f, indent=2, ensure_ascii=False)

        return {
            "identifier": task.identifier,
            "title": task.title,
            "modified": task.modified,
            "raw_path": str(raw_path),
            "out_path": str(out_path),
            "map_path": str(map_path),
            "rows_written": rows,
            "original_headers_sample": original_headers[:8],
            "new_headers_sample": new_headers[:8],
            "status": "OK",
        }

    except Exception as e:
        return {
            "identifier": task.identifier,
            "title": task.title,
            "modified": task.modified,
            "status": "ERROR",
            "error": str(e),
        }


def main() -> int:
    parser = argparse.ArgumentParser(description="Incremental CMS Hospitals downloader + snake_case CSV header processor.")
    parser.add_argument("--out", default="cms_hospitals/output", help="Output directory (default: cms_hospitals/output)")
    parser.add_argument("--db", default="state.db", help="SQLite state DB path (default: state.db)")
    parser.add_argument("--max-workers", type=int, default=8, help="Parallel worker count (default: 8)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    out_dir = Path(args.out)
    raw_dir = out_dir / "raw"
    processed_dir = out_dir / "processed"
    report_dir = out_dir / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)

    state_db = StateDB(Path(args.db))
    run_id = state_db.start_run(THEME_NAME)

    try:
        last_success = state_db.last_success_time(THEME_NAME)
        logging.info("Last successful run (UTC): %s", last_success.isoformat() if last_success else "None (first run)")

        with requests.Session() as session:
            resp = session.get(METASTORE_URL, timeout=60)
            resp.raise_for_status()
            items = resp.json()

            tasks_all = iter_hospital_csv_tasks(items)
            tasks = [t for t in tasks_all if should_download(t, last_success)]

            logging.info("Found %d Hospitals CSV distributions; %d need download this run.", len(tasks_all), len(tasks))

            results: List[Dict[str, Any]] = []

            if tasks:
                with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
                    futures = [ex.submit(process_one_task, session, t, raw_dir, processed_dir) for t in tasks]

                    for f in as_completed(futures):
                        r = f.result()
                        results.append(r)

                        # DB writes happen ONLY here (main thread)
                        if r.get("status") == "OK":
                            state_db.upsert_dataset(
                                identifier=r["identifier"],
                                title=r.get("title", ""),
                                theme=THEME_NAME,
                                modified=r.get("modified", ""),
                                last_downloaded_at_utc=utc_now_iso(),
                                last_output_path=r.get("out_path"),
                                last_error=None,
                            )
                        else:
                            state_db.upsert_dataset(
                                identifier=r.get("identifier", ""),
                                title=r.get("title", ""),
                                theme=THEME_NAME,
                                modified=r.get("modified", ""),
                                last_downloaded_at_utc=None,
                                last_output_path=None,
                                last_error=r.get("error", "unknown error"),
                            )

            report_path = report_dir / f"run_{run_id}_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(report_path, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "run_id": run_id,
                        "theme": THEME_NAME,
                        "last_success_utc": last_success.isoformat() if last_success else None,
                        "tasks_attempted": len(tasks),
                        "results": results,
                    },
                    f,
                    indent=2,
                    ensure_ascii=False,
                )

            ok = sum(1 for r in results if r.get("status") == "OK")
            err = sum(1 for r in results if r.get("status") == "ERROR")
            logging.info("Run summary: OK=%d ERROR=%d report=%s", ok, err, report_path)

        state_db.finish_run(run_id, "SUCCESS", notes=f"Downloaded={len(tasks)} OK={ok} ERROR={err}")
        return 0

    except Exception as e:
        logging.exception("Run failed: %s", e)
        state_db.finish_run(run_id, "FAILED", notes=str(e))
        return 2

    finally:
        state_db.close()


if __name__ == "__main__":
    raise SystemExit(main())
