# evaluate.py - Execution Guide

## Overview

Processes a master CSV file exported from DB, groups records by `RUNID`, and for each run:
- Creates a target directory structure
- Writes a `metadata.json` file with common fields and nested design steps
- Copies attachment files from source to target
- Logs mismatches and batch timing

Supports **parallel batch processing** and **resume on failure** via a checkpoint file.

---

## Quick Start

```bash
# Default run (uses built-in paths and defaults)
python evaluate.py

# Custom paths and parallelism
python evaluate.py \
  --csv-path /path/to/master-file.csv \
  --source-root-dir /path/to/source \
  --target-root-dir /path/to/target \
  --batch-size 500 \
  --max-workers 8

# Reprocess everything (ignore previous checkpoint)
python evaluate.py --no-resume

# Custom checkpoint location
python evaluate.py --checkpoint-file /path/to/processed_runs.json
```

---

## Configuration Variables

### Paths

| Variable | Default | Description |
|---|---|---|
| `TARGET_ROOT_DIR` | `/Users/adnankhan/learning/target` | Root directory where output is written |
| `SOURCE_ROOT_DIR` | `/Users/adnankhan/learning/source` | Root directory where source attachments are read from |
| `MASTER_CSV_PATH` | `/Users/adnankhan/learning/master-file.csv` | Input CSV file path |

All three can be overridden via CLI: `--target-root-dir`, `--source-root-dir`, `--csv-path`.

### Processing

| Variable | Default | Description |
|---|---|---|
| `DEFAULT_BATCH_SIZE` | `1000` | Number of **unique run IDs** per batch (not CSV rows). Override with `--batch-size`. |
| `DEFAULT_MAX_WORKERS` | `10` | Max parallel threads processing batches simultaneously. Override with `--max-workers`. |
| `CHECKPOINT_FLUSH_INTERVAL` | `500` | Write checkpoint JSON to disk every N processed/failed run IDs. Also flushes on shutdown. |

### Path Sanitization

| Variable | Default | Description |
|---|---|---|
| `MAX_PATH_COMPONENT_LENGTH` | `200` | Max characters per directory/file name segment. Longer names get truncated with a hash suffix. |
| `MAX_RELATIVE_PATH_LENGTH` | `4000` | Max total relative path length. Longest middle segments are shortened first. |

---

## Column Lists

These lists drive what gets extracted from each CSV row. Add or remove column names to change behavior without touching logic code.

### `COMMON_METADATA_COLUMNS`

Columns extracted from the **first row** of each run ID to build `metadata.json`:

```
RUNID, TEST_PROJECT_NAME, TEST_ENTITY, ENTITY, DOMAIN, SUBDOMAIN,
PROJECT_NAME, PROJECT_DESCRIPTION, PROJECT_STATUS, PROJECT_START_DATE,
PROJECT_END_DATE, PROJECT_OWNER, PROJECT_OWNER_EMAIL, PROJECT_OWNER_PHONE
```

### `DESIGN_STEP_COLUMNS`

Columns extracted from each **DESSTEPS row** and nested under `"design_steps"` in metadata:

```
STEPNUMBER, STEPNAME, STEPDESCRIPTION
```

### Path Column Lists

These control how source and target directory paths are assembled. Each column value becomes a path segment in order.

| List | Columns | Produces (example) |
|---|---|---|
| `SOURCE_PATH_COLUMNS` | `DOMAIN / SUBDOMAIN / ENTITY / RUNID` | `RETAIL/PAYMENTS/SCREENSHOT/100042` |
| `TARGET_PATH_COLUMNS` | `DOMAIN / SUBDOMAIN / TEST_PROJECT_NAME / RUNID / ENTITY` | `retail/payments/checkout_flow/100042/screenshot` |
| `TARGET_METADATA_PATH_COLUMNS` | `DOMAIN / SUBDOMAIN / TEST_PROJECT_NAME / RUNID` | `retail/payments/checkout_flow/100042` |
| `SOURCE_DESIGN_STEP_PATH_COLUMNS` | `DOMAIN / SUBDOMAIN / TEST_ENTITY / RUNID` | `RETAIL/PAYMENTS/DESSTEPS/100042` |
| `TARGET_DESIGN_STEP_PATH_COLUMNS` | `DOMAIN / SUBDOMAIN / TEST_PROJECT_NAME / RUNID / TEST_ENTITY` | `retail/payments/checkout_flow/100042/dessteps` |

Design-step paths are only built when the run has rows with `TEST_ENTITY = DESSTEPS`.

### `PATH_RAW_COLUMNS`

Columns in this set are used **as-is** in paths (no case transform, no reformat). Default: `{"RUNID"}`.

### `SOURCE_PATH_VALUE_OVERRIDES`

Maps column values to different values **only for source paths**. Used when the source filesystem uses a different naming convention than the CSV.

```python
SOURCE_PATH_VALUE_OVERRIDES = {
    "ENTITY": {
        "TESTPLAN": "TEST",   # ENTITY=TESTPLAN -> source path uses TEST
    }
}
```

So a row with `ENTITY=TESTPLAN` produces:
- Source path: `.../TEST/100042`
- Target path: `.../testplan/100042`

---

## Key Components

### 1. CSV Loading (`load_master_data`)

- Reads the entire CSV once into memory using pandas
- Groups rows by `RUNID` into a dictionary
- All subsequent processing works from this in-memory grouped data (no re-reads)

### 2. Path Generation (`generate_paths`)

- Takes the full DataFrame for one run ID
- Builds source/target relative paths from column lists
- Checks all rows for `TEST_ENTITY = DESSTEPS` before building design-step paths
- Source paths are **UPPERCASE**, target paths are **lowercase**
- Returns `copy_pairs` (source, target tuples) and metadata path

### 3. Path Sanitization (`sanitize_target_paths`)

Applied **only to target paths**, right before directory creation:
- Strips invalid filesystem characters (`<>:"|?*` and control chars)
- Collapses repeated underscores
- Truncates long segments with a hash suffix for uniqueness
- Blocks path traversal (`..`) outside target root via `resolve_target_path`

Source paths are never sanitized (they must match existing filesystem structure).

### 4. Metadata Generation (`get_metadata_for_run_id`)

Produces a JSON object per run ID:

```json
{
  "RUNID": "100042",
  "DOMAIN": "retail",
  "SUBDOMAIN": "payments",
  "design_steps": [
    {"STEPNUMBER": 1, "STEPNAME": "Open application", "STEPDESCRIPTION": "..."},
    {"STEPNUMBER": 2, "STEPNAME": "Login with valid user", "STEPDESCRIPTION": "..."}
  ]
}
```

- Common fields from `COMMON_METADATA_COLUMNS` (first row)
- Nested `design_steps` array from `DESIGN_STEP_COLUMNS` (all DESSTEPS rows)
- Empty array if no design steps exist

### 5. Parallel Batch Processing

```
CSV rows -> group by RUNID -> chunk into batches -> ThreadPoolExecutor
```

- Each batch is a list of run IDs processed sequentially within one thread
- Multiple batches run in parallel (up to `max_workers` threads)
- File I/O (mismatch report, checkpoint) is thread-safe via locks

### 6. Checkpoint / Resume (`ProcessedRunTracker`)

- Backed by a JSON file at `<target>/processed_runs.json`
- Tracks `success` and `failed` run ID sets in memory
- Flushes to disk every `CHECKPOINT_FLUSH_INTERVAL` marks + on shutdown
- On restart with `--resume` (default), skips already-successful run IDs
- Prints progress after every run ID:

```
[success] run_id=100042 | processed=43, failed=0, remaining=1961, total=2004
```

JSON structure:

```json
{
  "total_count": 2004,
  "processed_count": 2004,
  "failed_count": 0,
  "remaining_count": 0,
  "success": ["100000", "100001", "..."],
  "failed": []
}
```

### 7. File Copy (`copy_files_to_target`)

- Copies all files from source directory to target directory
- Always returns actual source/target paths and file counts (even when source is missing)
- Results written to mismatch report for auditing

---

## Output Files

All written under `TARGET_ROOT_DIR`:

| File | Description |
|---|---|
| `processed_runs.json` | Checkpoint with success/failed run IDs and summary counts |
| `mismatch_report.csv` | Per-copy-pair log: `run_id, source_path, target_path, source_file_count, source_file_path_size, target_file_count, target_file_path_size` |
| `batch_timing_report.csv` | Per-batch timing: `batch_number, run_id_count, processed, skipped, failed, start_time, end_time, duration_seconds` |
| `<domain>/<subdomain>/<project>/<runid>/metadata.json` | Metadata per run ID |

---

## CLI Arguments

| Argument | Default | Description |
|---|---|---|
| `--csv-path` | `MASTER_CSV_PATH` | Path to input CSV |
| `--source-root-dir` | `SOURCE_ROOT_DIR` | Source attachments root |
| `--target-root-dir` | `TARGET_ROOT_DIR` | Output root |
| `--batch-size` | `1000` | Run IDs per batch |
| `--max-workers` | `10` | Parallel batch threads |
| `--checkpoint-file` | `<target>/processed_runs.json` | Checkpoint file path |
| `--resume` / `--no-resume` | `--resume` | Skip already-processed run IDs |
