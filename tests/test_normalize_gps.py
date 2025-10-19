import pandas as pd
from gears.normalize.gps import normalize_gps


def test_vendor_a_basic():
    df, report = normalize_gps(
        "tests/fixtures/gps_vendor_a.csv",
        "tests/fixtures/config/gps_vendor_a.yaml",
    )
    assert list(df.columns) == [
        "vehicle_id",
        "ts",
        "lat",
        "lon",
        "speed_mps",
        "heading_deg",
    ]
    # km/h -> m/s conversion: 36 km/h == 10 m/s
    assert abs(df.loc[2, "speed_mps"] - 10.0) < 1e-6
    # all UTC strings
    assert df["ts"].str.endswith("Z").all()
    assert report["rows_in"] == 3 and report["rows_out"] == 3


def test_vendor_b_stopwatch_header_time():
    df, report = normalize_gps(
        "tests/fixtures/gps_vendor_b.csv",
        "tests/fixtures/config/gps_vendor_b.yaml",
    )
    assert len(df) == 3
    # First timestamp should be StartTime in UTC format (suffix Z)
    assert df.loc[0, "ts"].endswith("Z")
    # Heading carried through
    assert not pd.isna(df.loc[0, "heading_deg"])
