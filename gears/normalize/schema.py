from typing import List


GPS_REQUIRED = ["vehicle_id", "ts", "lat", "lon"]
GPS_OPTIONAL = ["speed_mps", "heading_deg"]


def pick_first_matching(src_cols: List[str], candidates: List[str]) -> str | None:
    """
    Case-insensitive match: return the first existing source column that matches any alias.
    """
    lowered = {c.lower(): c for c in src_cols}
    for alias in candidates:
        if alias.lower() in lowered:
            return lowered[alias.lower()]
    return None


def ensure_range_lat_lon(lat: float, lon: float) -> bool:
    """
    True if lat/lon are within valid geographic ranges.
    """
    try:
        return -90.0 <= float(lat) <= 90.0 and -180.0 <= float(lon) <= 180.0
    except Exception:
        return False
