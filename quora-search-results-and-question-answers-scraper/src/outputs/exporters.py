thonfrom __future__ import annotations

import json
import logging
import os
from typing import List, Dict, Any

import pandas as pd

from extractors.utils_time import now_utc, timestamp_for_filename

logger = logging.getLogger("quora_scraper.exporters")

def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def export_data(
    records: List[Dict[str, Any]],
    output_dir: str,
    base_filename: str = "quora_results",
    fmt: str = "json",
) -> str:
    """
    Export records to the requested format and return the file path.

    Supported formats: json, csv, excel, html
    """
    if fmt not in {"json", "csv", "excel", "html"}:
        raise ValueError(f"Unsupported format: {fmt}")

    _ensure_dir(output_dir)

    ts = timestamp_for_filename(now_utc())
    ext = {
        "json": "json",
        "csv": "csv",
        "excel": "xlsx",
        "html": "html",
    }[fmt]

    filename = f"{base_filename}_{ts}.{ext}"
    path = os.path.join(output_dir, filename)

    logger.info("Exporting %d records as %s to %s", len(records), fmt, path)

    if fmt == "json":
        _export_json(records, path)
    else:
        df = pd.DataFrame(records)
        if fmt == "csv":
            _export_csv(df, path)
        elif fmt == "excel":
            _export_excel(df, path)
        elif fmt == "html":
            _export_html(df, path)

    return path

def _export_json(records: List[Dict[str, Any]], path: str) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)
    except Exception as exc:
        logger.error("Failed to export JSON to %s: %s", path, exc)
        raise

def _export_csv(df, path: str) -> None:
    try:
        df.to_csv(path, index=False)
    except Exception as exc:
        logger.error("Failed to export CSV to %s: %s", path, exc)
        raise

def _export_excel(df, path: str) -> None:
    try:
        df.to_excel(path, index=False)
    except Exception as exc:
        logger.error("Failed to export Excel to %s: %s", path, exc)
        raise

def _export_html(df, path: str) -> None:
    try:
        df.to_html(path, index=False)
    except Exception as exc:
        logger.error("Failed to export HTML to %s: %s", path, exc)
        raise