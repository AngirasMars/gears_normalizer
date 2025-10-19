import argparse
import sys


def main(argv=None):
    parser = argparse.ArgumentParser(prog="gears")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("normalize-gps", help="Normalize GPS table (stub)")
    sub.add_parser("normalize-pois", help="Normalize POIs (stub)")
    args = parser.parse_args(argv or sys.argv[1:])
    print(f"stub: {args.cmd}")
