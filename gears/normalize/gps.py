from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import pandas as pd
import yaml

from .schema import pick_first_matching, ensure_range_lat_lon


def _load_mapping(yaml_path: str) -> Dict[str, Any]:
    with open(yaml_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _extract_header_kv(path: str) -> Dict[str, str]:
    """
    Read initial comment lines like: "# StartTime = 06/29/2016 07:23:50.2827 AM"
    Returns dict of key->value. Stops when the first non-# line is encountered.
    BOM at the start of the file is tolerated.
    """
    kv: Dict[str, str] = {}
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for _, line in enumerate(f):
            # tolerate UTF-8 BOM, then trim whitespace
            line_stripped = line.lstrip("\ufeff").strip()
            if not line_stripped.startswith("#"):
                break
            # remove leading '#', split on '=' if present
            body = line_stripped.lstrip("#").strip()
            if "=" in body:
                k, v = body.split("=", 1)
                kv[k.strip()] = v.strip()
    return kv


def _read_table(path: str) -> pd.DataFrame:
    p = Path(path)
    if p.suffix.lower() in (".parquet", ".pq"):
        return pd.read_parquet(p)
    # For CSV with header comments, pandas can skip them using `comment="#"`
    return pd.read_csv(p, comment="#")


def _parse_iso_or_local_to_utc(
    series: pd.Series, is_iso_utc: bool, input_tz: str
) -> pd.Series:
    """
    Parse timestamps:
      - if already ISO UTC -> parse with utc=True
      - else parse naive/local, localize to input_tz, then convert to UTC
    Return as ISO 8601 UTC strings (Z suffix).
    """
    if is_iso_utc:
        dt = pd.to_datetime(series, utc=True, errors="coerce")
    else:
        dt = pd.to_datetime(series, errors="coerce", utc=False)
        # If tz-aware already, pandas keeps tz; otherwise localize.
        try:
            dt = dt.dt.tz_localize(
                input_tz, nonexistent="shift_forward", ambiguous="NaT"
            )
        except TypeError:
            # already tz-aware
            pass
        dt = dt.dt.tz_convert("UTC")
    return dt.dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _build_ts_from_start_plus_seconds(
    seconds: pd.Series, start_str: str, input_tz: str
) -> pd.Series:
    """
    Build timestamps from a header StartTime and a per-row seconds column.
    Returns ISO 8601 UTC strings (Z suffix).
    """
    # start_str like "06/29/2016 07:23:50.2827 AM"
    start = pd.to_datetime(start_str, errors="coerce")
    if start.tzinfo is None:
        start = start.tz_localize(
            input_tz, nonexistent="shift_forward", ambiguous="NaT"
        )
    # add seconds (can be float)
    seconds = pd.to_numeric(seconds, errors="coerce")
    dt = start + pd.to_timedelta(seconds, unit="s")
    dt = dt.dt.tz_convert("UTC")
    return dt.dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_gps(path_in: str, mapping_yaml: str) -> tuple[pd.DataFrame, dict]:
    """
    Normalize raw GPS into GEARS GPS schema:
      vehicle_id (string), ts (UTC ISO8601), lat (float), lon (float),
      speed_mps (Float64 nullable), heading_deg (Float64 nullable)
    Returns (DataFrame, report_dict).
    """
    mapping = _load_mapping(mapping_yaml)
    time_cfg = mapping.get("time", {}) or {}
    units_cfg = mapping.get("units", {}) or {}
    colmap = mapping.get("columns", {}) or {}

    header_kv: Dict[str, str] = {}
    p = Path(path_in)
    if p.suffix.lower() not in (".parquet", ".pq"):
        header_kv = _extract_header_kv(path_in)

    raw = _read_table(path_in)

    # Resolve source columns for targets via aliases
    resolved: Dict[str, str] = {}
    for target, aliases in colmap.items():
        alias_list = aliases if isinstance(aliases, list) else [aliases]
        src = pick_first_matching(list(raw.columns), alias_list)
        if src:
            resolved[target] = src

    # Vehicle id: if missing, synthesize from filename (single-vehicle file)
    if "vehicle_id" not in resolved:
        resolved["vehicle_id"] = None

    # Timestamp handling:
    # - either direct ts column (iso/local)
    # - OR stopwatch seconds + StartTime in header
    use_stopwatch = "start_time_header" in time_cfg and "seconds_column" in time_cfg

    need = ["lat", "lon"]
    if not use_stopwatch:
        need += ["ts"]
    missing = [k for k in need if k not in resolved and k not in ("ts",)]
    if missing:
        raise ValueError(
            f"Missing required column mappings: {missing}. Have columns: {list(raw.columns)}"
        )

    # Build the normalized frame
    df = pd.DataFrame(index=raw.index)
    if resolved.get("vehicle_id") is not None:
        df["vehicle_id"] = raw[resolved["vehicle_id"]].astype("string").str.strip()
    else:
        df["vehicle_id"] = str(p.stem)

    df["lat"] = pd.to_numeric(raw[resolved["lat"]], errors="coerce")
    df["lon"] = pd.to_numeric(raw[resolved["lon"]], errors="coerce")

    # Speed (normalize to m/s)
    if "speed" in resolved:
        speed = pd.to_numeric(raw[resolved["speed"]], errors="coerce")
        if units_cfg.get("speed_is_kmh", False):
            speed = speed / 3.6
        df["speed_mps"] = speed.astype("Float64")
    else:
        df["speed_mps"] = pd.Series([pd.NA] * len(df), dtype="Float64")

    # Heading (deg)
    if "heading" in resolved:
        df["heading_deg"] = pd.to_numeric(
            raw[resolved["heading"]], errors="coerce"
        ).astype("Float64")
    else:
        df["heading_deg"] = pd.Series([pd.NA] * len(df), dtype="Float64")

    # Build timestamp column
    input_tz = time_cfg.get("input_tz", "UTC")
    if use_stopwatch:
        start_key = str(time_cfg["start_time_header"])
        seconds_col = str(time_cfg["seconds_column"])
        if seconds_col not in raw.columns:
            raise ValueError(
                f"seconds_column '{seconds_col}' not found in data columns: {list(raw.columns)}"
            )
        if start_key not in header_kv or not header_kv[start_key]:
            raise ValueError(
                f"Header '{start_key}' not found at top of file; available: {header_kv}"
            )
        df["ts"] = _build_ts_from_start_plus_seconds(
            raw[seconds_col], header_kv[start_key], input_tz
        )
    else:
        if "ts" not in resolved:
            raise ValueError(
                "No timestamp column mapped and no stopwatch config provided."
            )
        df["ts"] = _parse_iso_or_local_to_utc(
            raw[resolved["ts"]],
            is_iso_utc=bool(time_cfg.get("is_iso_utc", False)),
            input_tz=input_tz,
        )

    # Clean: drop invalid rows, sort, dedupe
    before = len(df)
    df = df[df.apply(lambda r: ensure_range_lat_lon(r["lat"], r["lon"]), axis=1)]
    df = df.dropna(subset=["vehicle_id", "ts", "lat", "lon"])
    df = df.sort_values(["vehicle_id", "ts"], kind="stable")
    df = df.drop_duplicates(subset=["vehicle_id", "ts", "lat", "lon"], keep="first")

    # Ingest report
    report: Dict[str, Any] = {
        "rows_in": int(before),
        "rows_out": int(len(df)),
        "dropped_rows": int(before - len(df)),
        "pct_missing_speed": float(df["speed_mps"].isna().mean() * 100.0),
    }

    # p95 inter-ping seconds per vehicle, then median across vehicles
    def _p95_dt(s: pd.Series) -> float:
        dt = pd.to_datetime(s, utc=True, errors="coerce")
        d = dt.diff().dt.total_seconds().dropna()
        return float(d.quantile(0.95)) if not d.empty else 0.0

    p95s = df.groupby("vehicle_id")["ts"].apply(_p95_dt)
    report["p95_inter_ping_seconds_median_across_vehicles"] = float(
        p95s.median() if not p95s.empty else 0.0
    )

    # Ensure dtypes/column order
    df["vehicle_id"] = df["vehicle_id"].astype("string")
    df["lat"] = df["lat"].astype("float64")
    df["lon"] = df["lon"].astype("float64")
    df["speed_mps"] = df["speed_mps"].astype("Float64")
    df["heading_deg"] = df["heading_deg"].astype("Float64")

    df = df[["vehicle_id", "ts", "lat", "lon", "speed_mps", "heading_deg"]].reset_index(
        drop=True
    )
    return df, report
