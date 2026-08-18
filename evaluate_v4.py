import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from pandas import isna, read_csv

TARGET_ROOT_DIR = "/Users/adnankhan/learning/target"
SOURCE_ROOT_DIR = "//Users/adnankhan/learning/source"
MASTER_CSV_PATH = "/Users/adnankhan/learning/master-file.csv"
DEFAULT_BATCH_SIZE = 1000
DEFAULT_MAX_WORKERS = 10
CHECKPOINT_FLUSH_INTERVAL = 500
MAX_PATH_COMPONENT_LENGTH = 200
MAX_RELATIVE_PATH_LENGTH = 4000

COMMON_METADATA_COLUMNS = [
    "RUNID",
    "TEST_PROJECT_NAME",
    "TEST_ENTITY",
    "ENTITY",
    "DOMAIN",
    "SUBDOMAIN",
    "PROJECT_NAME",
    "PROJECT_DESCRIPTION",
    "PROJECT_STATUS",
    "PROJECT_START_DATE",
    "PROJECT_END_DATE",
    "PROJECT_OWNER",
    "PROJECT_OWNER_EMAIL",
    "PROJECT_OWNER_PHONE",
]

DESIGN_STEP_COLUMNS = [
    "STEPNUMBER",
    "STEPNAME",
    "STEPDESCRIPTION",

]

SOURCE_PATH_COLUMNS = ["DOMAIN", "SUBDOMAIN", "ENTITY", "RUNID"]
TARGET_PATH_COLUMNS = ["DOMAIN", "SUBDOMAIN", "TEST_PROJECT_NAME", "RUNID", "ENTITY"]
TARGET_METADATA_PATH_COLUMNS = ["DOMAIN", "SUBDOMAIN", "TEST_PROJECT_NAME", "RUNID"]

# Optional extra copy when every column has a value (skipped if TEST_ENTITY is empty).
SOURCE_DESIGN_STEP_PATH_COLUMNS = ["DOMAIN", "SUBDOMAIN", "TEST_ENTITY", "RUNID"]
TARGET_DESIGN_STEP_PATH_COLUMNS = [
    "DOMAIN",
    "SUBDOMAIN",
    "TEST_PROJECT_NAME",
    "RUNID",
    "TEST_ENTITY",
]

PATH_RAW_COLUMNS = {"RUNID"}
SOURCE_PATH_VALUE_OVERRIDES = {
    "ENTITY": {
        "TESTPLAN": "TEST",
    }
}

INVALID_PATH_CHARS = re.compile(r'[<>:"|?*\\/\x00-\x1f]')
MULTI_UNDERSCORE = re.compile(r"_+")
WINDOWS_RESERVED_NAMES = {
    "CON", "PRN", "AUX", "NUL",
    "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
    "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
}

_mismatch_report_lock = threading.Lock()
_batch_timing_report_lock = threading.Lock()


def _truncate_with_hash(value: str, max_length: int) -> str:
    if len(value) <= max_length:
        return value
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:8]
    keep = max_length - len(digest) - 1
    if keep < 1:
        return digest[:max_length]
    return f"{value[:keep]}_{digest}"


def sanitize_path_component(name: str, max_length: int = MAX_PATH_COMPONENT_LENGTH) -> str:
    cleaned = INVALID_PATH_CHARS.sub("_", name.strip())
    cleaned = MULTI_UNDERSCORE.sub("_", cleaned)
    cleaned = cleaned.strip("._ ")
    if not cleaned:
        cleaned = "unnamed"
    if cleaned.upper() in WINDOWS_RESERVED_NAMES:
        cleaned = f"_{cleaned}"
    return _truncate_with_hash(cleaned, max_length)


def sanitize_relative_path(relative_path: str) -> str:
    if not relative_path:
        return relative_path

    parts = [sanitize_path_component(part) for part in relative_path.split(os.sep) if part]
    if not parts:
        return "unnamed"

    sanitized = os.path.join(*parts)
    if len(sanitized) <= MAX_RELATIVE_PATH_LENGTH:
        return sanitized

    # Shorten longest segments first while preserving the final segment (usually run_id).
    parts = list(parts)
    while len(os.path.join(*parts)) > MAX_RELATIVE_PATH_LENGTH and len(parts) > 1:
        longest_index = max(range(len(parts) - 1), key=lambda i: len(parts[i]))
        shortened = _truncate_with_hash(parts[longest_index], MAX_PATH_COMPONENT_LENGTH // 2)
        if shortened == parts[longest_index]:
            parts.pop(longest_index)
        else:
            parts[longest_index] = shortened

    return os.path.join(*parts) if parts else "unnamed"


def reformat_string(input_string):
    segments = input_string.split(">")

    cleaned_segments = []
    for segment in segments:
        s = segment.strip()
        s = s.replace(".", "")
        s = re.sub(r"\s+", "_", s)
        if s:
            cleaned_segments.append(s)

    return "/".join(cleaned_segments)


def _cell_value(value):
    if isna(value):
        return None
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped or stripped.upper() == "NULL":
            return None
        return stripped
    return value


def _row_to_fields(row, columns):
    return {column: _cell_value(row.get(column)) for column in columns}


def _path_segment(row, column, lowercase=True, value_overrides=None):
    value = _cell_value(row.get(column))
    if value is None:
        return None
    value = str(value)
    if value_overrides:
        value = value_overrides.get(column, {}).get(value.upper(), value)
    if column in PATH_RAW_COLUMNS:
        return value
    transformed = value.lower() if lowercase else value.upper()
    return reformat_string(transformed)


def build_relative_path(row, columns, lowercase=True, value_overrides=None):
    if not columns:
        return None
    segments = []
    for column in columns:
        segment = _path_segment(
            row,
            column,
            lowercase=lowercase,
            value_overrides=value_overrides,
        )
        if segment is None:
            return None
        segments.extend(part for part in segment.split("/") if part)
    if not segments:
        return None
    return os.path.join(*segments)


def sanitize_target_paths(paths):
    """Apply filesystem-safe sanitization to target paths only."""
    sanitized_copy_pairs = [
        (source_path, sanitize_relative_path(target_path))
        for source_path, target_path in paths["copy_pairs"]
    ]
    sanitized_attachment_paths = [
        sanitize_relative_path(path) for path in paths["target_attachment_paths"]
    ]
    metadata_path = paths["target_metadata_path"]
    return {
        "copy_pairs": sanitized_copy_pairs,
        "target_attachment_paths": sanitized_attachment_paths,
        "target_metadata_path": sanitize_relative_path(metadata_path)
        if metadata_path
        else None,
    }


def _has_dessteps(df):
    return df["TEST_ENTITY"].notna().any() and (
        df["TEST_ENTITY"].astype(str).str.strip().str.upper() == "DESSTEPS"
    ).any()


def generate_paths(df):
    row = df.iloc[0]
    copy_pairs = []
    target_attachment_paths = []

    if _has_dessteps(df):
        desstep_row = df.loc[
            df["TEST_ENTITY"].astype(str).str.strip().str.upper() == "DESSTEPS"
        ].iloc[0]
        source_design_step_path = build_relative_path(
            desstep_row,
            SOURCE_DESIGN_STEP_PATH_COLUMNS,
            lowercase=False,
            value_overrides=SOURCE_PATH_VALUE_OVERRIDES,
        )
        target_design_step_path = build_relative_path(desstep_row, TARGET_DESIGN_STEP_PATH_COLUMNS)
        if source_design_step_path and target_design_step_path:
            copy_pairs.append((source_design_step_path, target_design_step_path))
            target_attachment_paths.append(target_design_step_path)

    source_path = build_relative_path(
        row,
        SOURCE_PATH_COLUMNS,
        lowercase=False,
        value_overrides=SOURCE_PATH_VALUE_OVERRIDES,
    )
    target_path = build_relative_path(row, TARGET_PATH_COLUMNS)
    if source_path and target_path:
        copy_pairs.append((source_path, target_path))
        target_attachment_paths.append(target_path)

    return {
        "copy_pairs": copy_pairs,
        "target_attachment_paths": target_attachment_paths,
        "target_metadata_path": build_relative_path(row, TARGET_METADATA_PATH_COLUMNS),
    }


def create_target_directory(path):
    os.makedirs(path, exist_ok=True)


def resolve_target_path(relative_path):
    if not relative_path:
        return None
    absolute_path = os.path.join(TARGET_ROOT_DIR, relative_path)
    root = os.path.abspath(TARGET_ROOT_DIR)
    resolved = os.path.abspath(absolute_path)
    if not resolved.startswith(root + os.sep) and resolved != root:
        raise ValueError(f"Unsafe target path escapes root directory: {relative_path}")
    return resolved


def create_target_paths(target_attachment_paths, target_metadata_path):
    for path in target_attachment_paths:
        create_target_directory(resolve_target_path(path))
    if target_metadata_path:
        create_target_directory(resolve_target_path(target_metadata_path))


def create_metadata_file(path, metadata):
    metadata_path = os.path.join(path, "metadata.json")
    with open(metadata_path, "w") as f:
        json.dump(metadata, f)
    return metadata_path


def get_metadata_for_run_id(run_id, df):
    if df.empty:
        return {}

    metadata = _row_to_fields(df.iloc[0], COMMON_METADATA_COLUMNS)
    metadata["RUNID"] = str(run_id)

    desstep_mask = df["TEST_ENTITY"].notna() & (
        df["TEST_ENTITY"].astype(str).str.strip().str.upper() == "DESSTEPS"
    )
    metadata["design_steps"] = [
        _row_to_fields(row, DESIGN_STEP_COLUMNS)
        for _, row in df[desstep_mask].iterrows()
    ]
    return metadata


def copy_files_to_target(source_attachment_path, target_attachment_path):
    source_root = os.path.join(SOURCE_ROOT_DIR, source_attachment_path)
    target_root = resolve_target_path(target_attachment_path)
    result = {
        "source_path": source_root,
        "target_path": target_root,
        "source_file_count": 0,
        "source_file_path_size": 0,
        "target_file_count": 0,
        "target_file_path_size": 0,
    }

    if not os.path.exists(source_root):
        if os.path.exists(target_root):
            result["target_file_count"] = len(os.listdir(target_root))
            result["target_file_path_size"] = sum(
                os.path.getsize(os.path.join(target_root, file))
                for file in os.listdir(target_root)
            )
        return result

    result["source_file_count"] = len(os.listdir(source_root))
    result["source_file_path_size"] = sum(
        os.path.getsize(os.path.join(source_root, file)) for file in os.listdir(source_root)
    )

    for file in os.listdir(source_root):
        shutil.copy(
            os.path.join(source_root, file),
            os.path.join(target_root, file),
        )

    result["target_file_count"] = len(os.listdir(target_root))
    result["target_file_path_size"] = sum(
        os.path.getsize(os.path.join(target_root, file)) for file in os.listdir(target_root)
    )

    return result


def write_mismatch_report(
    run_id,
    source_path,
    target_path,
    source_file_count,
    source_file_path_size,
    target_file_count,
    target_file_path_size,
    mismatch_report_path,
):
    with _mismatch_report_lock:
        with open(mismatch_report_path, "a", newline="") as f:
            writer = csv.writer(f, delimiter=",")
            writer.writerow(
                [
                    run_id,
                    source_path,
                    target_path,
                    source_file_count,
                    source_file_path_size,
                    target_file_count,
                    target_file_path_size,
                ]
            )


class ProcessedRunTracker:
    """Tracks processed run IDs using an in-memory set backed by a JSON file."""

    def __init__(self, checkpoint_path: str, total_count: int = 0):
        self._path = checkpoint_path
        self._lock = threading.Lock()
        self._dirty_count = 0
        self._total_count = total_count

        if os.path.exists(checkpoint_path):
            with open(checkpoint_path) as f:
                data = json.load(f)
            self._success: set[str] = set(data.get("success", []))
            self._failed: set[str] = set(data.get("failed", []))
        else:
            self._success: set[str] = set()
            self._failed: set[str] = set()

    def set_total_count(self, total_count: int) -> None:
        with self._lock:
            self._total_count = total_count

    def get_processed_run_ids(self) -> set[str]:
        return self._success

    @property
    def processed_count(self) -> int:
        return len(self._success)

    @property
    def failed_count(self) -> int:
        return len(self._failed)

    @property
    def remaining_count(self) -> int:
        return max(0, self._total_count - len(self._success))

    def is_processed(self, run_id) -> bool:
        return str(run_id) in self._success

    def mark_success(self, run_id) -> None:
        with self._lock:
            self._success.add(str(run_id))
            self._failed.discard(str(run_id))
            self._dirty_count += 1
            self._print_progress(run_id, "success")
            if self._dirty_count >= CHECKPOINT_FLUSH_INTERVAL:
                self._flush_unlocked()

    def mark_failure(self, run_id, error_message: str = "") -> None:
        with self._lock:
            self._failed.add(str(run_id))
            self._dirty_count += 1
            self._print_progress(run_id, "failed")
            if self._dirty_count >= CHECKPOINT_FLUSH_INTERVAL:
                self._flush_unlocked()

    def _print_progress(self, run_id, status: str) -> None:
        print(
            f"[{status}] run_id={run_id} | "
            f"processed={len(self._success)}, "
            f"failed={len(self._failed)}, "
            f"remaining={max(0, self._total_count - len(self._success))}, "
            f"total={self._total_count}"
        )

    def _flush_unlocked(self) -> None:
        with open(self._path, "w") as f:
            json.dump(
                {
                    "total_count": self._total_count,
                    "processed_count": len(self._success),
                    "failed_count": len(self._failed),
                    "remaining_count": max(0, self._total_count - len(self._success)),
                    "success": sorted(self._success),
                    "failed": sorted(self._failed),
                },
                f,
            )
        self._dirty_count = 0

    def flush(self) -> None:
        with self._lock:
            self._flush_unlocked()

    def close(self) -> None:
        self.flush()


def load_master_data(csv_path: str):
    print(f"Loading master CSV from {csv_path} ...")
    df = read_csv(csv_path)
    grouped = {run_id: group for run_id, group in df.groupby("RUNID", sort=False)}
    print(f"Loaded {len(df):,} records across {len(grouped):,} run IDs.")
    return grouped


def chunk_run_ids(run_ids: list, batch_size: int):
    for index in range(0, len(run_ids), batch_size):
        yield run_ids[index : index + batch_size]


def process_run_id(run_id, run_df, mismatch_report_path):
    paths = sanitize_target_paths(generate_paths(run_df))
    create_target_paths(paths["target_attachment_paths"], paths["target_metadata_path"])
    metadata = get_metadata_for_run_id(run_id, run_df)
    create_metadata_file(
        resolve_target_path(paths["target_metadata_path"]),
        metadata,
    )

    for source_path, target_path in paths["copy_pairs"]:
        copy_result = copy_files_to_target(source_path, target_path)
        write_mismatch_report(
            run_id,
            copy_result["source_path"],
            copy_result["target_path"],
            copy_result["source_file_count"],
            copy_result["source_file_path_size"],
            copy_result["target_file_count"],
            copy_result["target_file_path_size"],
            mismatch_report_path,
        )

    return run_id


def process_batch(batch_number, run_ids, grouped_runs, tracker, mismatch_report_path):
    start_time = datetime.now(timezone.utc)
    start_perf = time.perf_counter()
    processed = 0
    skipped = 0
    failed = 0

    for run_id in run_ids:
        if tracker.is_processed(run_id):
            skipped += 1
            continue

        try:
            process_run_id(run_id, grouped_runs[run_id], mismatch_report_path)
            tracker.mark_success(run_id)
            processed += 1
        except Exception as exc:
            tracker.mark_failure(run_id, str(exc))
            failed += 1
            print(f"Failed run ID {run_id} in batch {batch_number}: {exc}")

    end_time = datetime.now(timezone.utc)
    duration_seconds = time.perf_counter() - start_perf

    return {
        "batch_number": batch_number,
        "run_id_count": len(run_ids),
        "processed": processed,
        "skipped": skipped,
        "failed": failed,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_seconds": round(duration_seconds, 3),
    }


def init_mismatch_report(mismatch_report_path: str):
    if not os.path.exists(mismatch_report_path):
        with open(mismatch_report_path, "w", newline="") as f:
            writer = csv.writer(f, delimiter=",")
            writer.writerow(
                [
                    "run_id",
                    "source_path",
                    "target_path",
                    "source_file_count",
                    "source_file_path_size",
                    "target_file_count",
                    "target_file_path_size",
                ]
            )


def init_batch_timing_report(batch_timing_report_path: str):
    if not os.path.exists(batch_timing_report_path):
        with open(batch_timing_report_path, "w", newline="") as f:
            writer = csv.writer(f, delimiter=",")
            writer.writerow(
                [
                    "batch_number",
                    "run_id_count",
                    "processed",
                    "skipped",
                    "failed",
                    "start_time",
                    "end_time",
                    "duration_seconds",
                ]
            )


def write_batch_timing_report(batch_timing_report_path, result):
    with _batch_timing_report_lock:
        with open(batch_timing_report_path, "a", newline="") as f:
            writer = csv.writer(f, delimiter=",")
            writer.writerow(
                [
                    result["batch_number"],
                    result["run_id_count"],
                    result["processed"],
                    result["skipped"],
                    result["failed"],
                    result["start_time"],
                    result["end_time"],
                    result["duration_seconds"],
                ]
            )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Process exported CSV run records in parallel with resume support."
    )
    parser.add_argument("--csv-path", default=MASTER_CSV_PATH)
    parser.add_argument("--target-root-dir", default=TARGET_ROOT_DIR)
    parser.add_argument("--source-root-dir", default=SOURCE_ROOT_DIR)
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Number of run IDs per batch (default: 10000).",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help="Number of batches to process in parallel (default: 10).",
    )
    parser.add_argument(
        "--checkpoint-file",
        default=None,
        help="JSON file for processed run tracking. Defaults to <target>/processed_runs.json",
    )
    parser.add_argument(
        "--resume",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip run IDs already marked successful in the checkpoint DB.",
    )
    return parser.parse_args()


def main():
    global TARGET_ROOT_DIR, SOURCE_ROOT_DIR

    args = parse_args()
    TARGET_ROOT_DIR = args.target_root_dir
    SOURCE_ROOT_DIR = args.source_root_dir

    checkpoint_file = args.checkpoint_file or os.path.join(
        TARGET_ROOT_DIR, "processed_runs.json"
    )
    mismatch_report_path = os.path.join(TARGET_ROOT_DIR, "mismatch_report.csv")
    batch_timing_report_path = os.path.join(TARGET_ROOT_DIR, "batch_timing_report.csv")

    os.makedirs(TARGET_ROOT_DIR, exist_ok=True)
    init_mismatch_report(mismatch_report_path)
    init_batch_timing_report(batch_timing_report_path)

    grouped_runs = load_master_data(args.csv_path)
    all_run_ids = list(grouped_runs.keys())
    tracker = ProcessedRunTracker(checkpoint_file, total_count=len(all_run_ids))
    if args.resume:
        already_processed = tracker.get_processed_run_ids()
        pending_run_ids = [run_id for run_id in all_run_ids if str(run_id) not in already_processed]
        print(
            f"Resume enabled: {len(already_processed):,} already processed, "
            f"{len(pending_run_ids):,} pending."
        )
    else:
        pending_run_ids = all_run_ids

    batches = list(chunk_run_ids(pending_run_ids, args.batch_size))
    if not batches:
        print("No run IDs to process.")
        tracker.close()
        return

    print(
        f"Processing {len(pending_run_ids):,} run IDs in {len(batches)} batch(es) "
        f"with up to {args.max_workers} parallel workers."
    )

    totals = {"processed": 0, "skipped": 0, "failed": 0}
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = [
            executor.submit(
                process_batch,
                batch_number,
                batch_run_ids,
                grouped_runs,
                tracker,
                mismatch_report_path,
            )
            for batch_number, batch_run_ids in enumerate(batches, start=1)
        ]

        for future in as_completed(futures):
            result = future.result()
            totals["processed"] += result["processed"]
            totals["skipped"] += result["skipped"]
            totals["failed"] += result["failed"]
            write_batch_timing_report(batch_timing_report_path, result)
            print(
                f"Batch {result['batch_number']} complete: "
                f"processed={result['processed']}, "
                f"skipped={result['skipped']}, "
                f"failed={result['failed']}, "
                f"start={result['start_time']}, "
                f"end={result['end_time']}, "
                f"duration={result['duration_seconds']}s"
            )

    tracker.close()
    print(
        "Done. "
        f"processed={totals['processed']:,}, "
        f"skipped={totals['skipped']:,}, "
        f"failed={totals['failed']:,}. "
        f"Checkpoint: {checkpoint_file}"
    )


if __name__ == "__main__":
    main()
