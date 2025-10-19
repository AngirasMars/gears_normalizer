import argparse
import json
from pathlib import Path
import sys

import pandas as pd

from .normalize.gps import normalize_gps


def _write_table(df: pd.DataFrame, out_path: str) -> None:
    p = Path(out_path)
    if p.suffix.lower() in (".parquet", ".pq"):
        df.to_parquet(p, index=False)
    else:
        df.to_csv(p, index=False)


def main(argv=None):
    parser = argparse.ArgumentParser(prog="gears")
    sub = parser.add_subparsers(dest="cmd", required=True)

    ng = sub.add_parser("normalize-gps", help="Normalize GPS table")
    ng.add_argument("--in", dest="path_in", required=True)
    ng.add_argument("--map", dest="mapping_yaml", required=True)
    ng.add_argument("--out", dest="path_out", required=True)
    ng.add_argument("--report", dest="path_report", required=False)

    # (POIs subcommand will be added later)

    args = parser.parse_args(argv or sys.argv[1:])

    if args.cmd == "normalize-gps":
        df, report = normalize_gps(args.path_in, args.mapping_yaml)
        _write_table(df, args.path_out)
        if args.path_report:
            Path(args.path_report).write_text(
                json.dumps(report, indent=2), encoding="utf-8"
            )
        print(f"Wrote {len(df)} rows to {args.path_out}")
        return
