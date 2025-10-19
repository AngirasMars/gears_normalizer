# GEARS Normalizer — GPS → Standard Schema

**Goal:** Turn messy vendor GPS tables into a tiny, strict schema so the GEARS detector can later emit **ARRIVE / DEPART / DWELL / ANOMALY**.

This repo currently includes the **GPS normalizer** (POIs coming next on `feat/normalize-pois`).

---

## TL;DR (quick start)

```bash
# clone
git clone https://github.com/AngirasMars/gears_normalizer.git
cd gears_normalizer

# create & activate venv (Windows)
python -m venv .venv
.\.venv\Scripts\activate

# install package + dev hooks
pip install -e .
pre-commit install

# run tests
pytest -q    # should pass
```

Normalize a file:

```bash
gears normalize-gps \
  --in raw/vendor.csv \
  --map config/vendor.yaml \
  --out data/gps_clean.parquet \
  --report out/gps_ingest.json
```

Open `out/gps_ingest.json` for an ingest summary (rows in/out, % missing speed, cadence metric).

> Tip: If you want to eyeball in Excel, use `--out data/file.csv` instead of Parquet.

---

## What the GPS normalizer expects & produces

### Input (any vendor CSV/Parquet)

Provide a small **YAML mapping** that tells the tool which source columns mean:

* **Required:**

  * `vehicle_id` (or omit and we’ll derive from filename)
  * either `ts` **or** (`start_time_header` + `seconds_column`)
  * `lat`, `lon`
* **Optional:**

  * `speed` (km/h or m/s)
  * `heading`

### Output schema

A tidy table with:

* `vehicle_id` (string)
* `ts` (UTC ISO8601, e.g. `2025-02-01T14:00:10Z`)
* `lat`, `lon` (float64)
* `speed_mps` (nullable Float64)  ← auto-converted from km/h if you set it in YAML
* `heading_deg` (nullable Float64)

Plus a JSON **ingest report** with:

* `rows_in`, `rows_out`, `dropped_rows`
* `pct_missing_speed`
* `p95_inter_ping_seconds_median_across_vehicles`

---

## YAML mapping templates

### A) ISO-UTC timestamps; speed in km/h (→ converted to m/s)

```yaml
columns:
  vehicle_id: ["veh_id"]
  ts: ["timestamp"]
  lat: ["lat", "latitude"]
  lon: ["lon", "lng", "longitude"]
  speed: ["speed_kmh"]

time:
  is_iso_utc: true
  input_tz: "UTC"

units:
  speed_is_kmh: true
```

### B) Stopwatch + `# StartTime = ...` header; speed already in m/s (OBD-style)

```yaml
columns:
  lat: ["Latitude"]
  lon: ["Longitude"]
  speed: ["GPS Speed (m/s)"]
  heading: ["Bearing (deg)"]

time:
  input_tz: "America/Chicago"   # set to the local zone of StartTime
  start_time_header: "StartTime"
  seconds_column: "Time (sec)"

units:
  speed_is_kmh: false
```

> **Note:** If your CSV headers contain **leading spaces** (common from Excel), either:
>
> * include the spaces in the aliases (e.g. `" Latitude"`), **or**
> * enable tolerant matching (see *Dev notes* below), so we ignore extra spaces/case.

---

## Examples (included fixtures)

```bash
# ISO UTC + km/h -> m/s
gears normalize-gps \
  --in tests/fixtures/gps_vendor_a.csv \
  --map tests/fixtures/config/gps_vendor_a.yaml \
  --out data/gps_a.parquet --report out/gps_a_report.json

# StartTime + seconds + m/s
gears normalize-gps \
  --in tests/fixtures/gps_vendor_b.csv \
  --map tests/fixtures/config/gps_vendor_b.yaml \
  --out data/gps_b.parquet --report out/gps_b_report.json
```

---

## Using your own dataset

1. **Save file** to `raw/` (CSV or Parquet).
   If using StartTime+seconds, make sure the very first line is:

   ```
   # StartTime = 06/29/2016 07:23:50.2827 AM
   ```
2. **Create mapping** in `config/your_vendor.yaml` (copy a template above).

   * If timestamps are not UTC, set `time.input_tz` to the local timezone.
   * If speed is in km/h, set `units.speed_is_kmh: true`.
3. **Run:**

   ```bash
   gears normalize-gps --in raw/your_vendor.csv \
     --map config/your_vendor.yaml \
     --out data/your_vendor.parquet \
     --report out/your_vendor_report.json
   ```
4. **Inspect output:** open the Parquet in Python, or output CSV for Excel, and check `out/your_vendor_report.json`.

---

## Troubleshooting

* **“Missing required column mappings”**
  Check YAML aliases exactly match headers (including any spaces), or enable tolerant matching (below).

* **“Header 'StartTime' not found”**
  The first non-empty line must literally start with `# StartTime = ...`.

* **Timestamps off by hours**
  Set `time.input_tz` to the correct local timezone of StartTime/ts.

* **Weird Excel spaces/BOM**
  BOM is handled; for spaces, see tolerant matching.

* **(0,0) coordinates**
  Valid but often “no fix”; those rows aren’t dropped by range alone.

---

## Dev notes

### Tolerant header matching (recommended)

Make the normalizer ignore extra spaces and case in CSV headers by replacing `pick_first_matching` in `gears/normalize/schema.py` with:

```python
def pick_first_matching(src_cols, candidates):
    def _norm(s: str) -> str:
        return " ".join(str(s).split()).lower()  # collapse spaces + lowercase
    lowered = {_norm(c): c for c in src_cols}
    for alias in candidates:
        key = _norm(alias)
        if key in lowered:
            return lowered[key]
    return None
```

Commit the change:

```bash
git add gears/normalize/schema.py
git commit -m "feat: tolerant header matching (ignore extra spaces/case)"
```

### Dev setup

```bash
# Windows
.\.venv\Scripts\activate
pip install -e .
pre-commit install
pytest -q
```

### Branching

* `main` = stable
* feature work via `feat/<topic>` → PR to `main`

---

## Roadmap

* `gears normalize-pois` (CSV/GeoJSON → standardized POI Points)
* `gears detect` (GPS + POIs → ARRIVE/DEPART/DWELL/ANOMALY)
* Confidence scoring, streaming wrapper, and auditor app.

---

## License

MIT (to be added)
