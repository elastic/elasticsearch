#!/usr/bin/env python3
"""
Generate ground-truth JSON files from apache/parquet-testing for ESQL integration tests.

Downloads parquet files from a pinned commit of https://github.com/apache/parquet-testing,
reads them with pyarrow, and outputs a canonical JSON file per parquet file.

Usage:
    pip install pyarrow
    python generate_parquet_ground_truth.py [--output-dir parquet-testing-ground-truth]

The output JSON files are checked into the repository and loaded by ParquetTestingIT.java
to compare against ESQL EXTERNAL query results.
"""

import argparse
import io
import json
import math
import os
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq

HASH = "fa255dfacf58c8bab428b5d0117d188acc8ad03f"
BASE_URL = f"https://raw.githubusercontent.com/apache/parquet-testing/{HASH}"

# Files from data/ to process (supported or partially supported by ESQL)
DATA_FILES = [
    "alltypes_dictionary.parquet",
    "alltypes_plain.parquet",
    "alltypes_plain.snappy.parquet",
    "alltypes_tiny_pages.parquet",
    "alltypes_tiny_pages_plain.parquet",
    "binary.parquet",
    "binary_truncated_min_max.parquet",
    "byte_array_decimal.parquet",
    "byte_stream_split.zstd.parquet",
    "byte_stream_split_extended.gzip.parquet",
    "column_chunk_key_value_metadata.parquet",
    "concatenated_gzip_members.parquet",
    "data_index_bloom_encoding_stats.parquet",
    "data_index_bloom_encoding_with_length.parquet",
    "datapage_v1-snappy-compressed-checksum.parquet",
    "datapage_v1-uncompressed-checksum.parquet",
    "datapage_v2.snappy.parquet",
    "datapage_v2_empty_datapage.snappy.parquet",
    "delta_binary_packed.parquet",
    "delta_byte_array.parquet",
    "delta_encoding_optional_column.parquet",
    "delta_encoding_required_column.parquet",
    "delta_length_byte_array.parquet",
    "dict-page-offset-zero.parquet",
    "fixed_length_byte_array.parquet",
    "fixed_length_decimal.parquet",
    "fixed_length_decimal_legacy.parquet",
    "float16_nonzeros_and_nans.parquet",
    "float16_zeros_and_nans.parquet",
    "int32_decimal.parquet",
    "int32_with_null_pages.parquet",
    "int64_decimal.parquet",
    "int96_from_spark.parquet",
    "lz4_raw_compressed.parquet",
    "lz4_raw_compressed_larger.parquet",
    "nan_in_stats.parquet",
    "nulls.snappy.parquet",
    "page_v2_empty_compressed.parquet",
    "plain-dict-uncompressed-checksum.parquet",
    "rle-dict-snappy-checksum.parquet",
    "rle_boolean_encoding.parquet",
    "single_nan.parquet",
    "sort_columns.parquet",
]

# Bad data files -- used to verify clean error handling
BAD_DATA_FILES = [
    "ARROW-GH-41317.parquet",
    "ARROW-GH-41321.parquet",
    "ARROW-GH-43605.parquet",
    "ARROW-GH-45185.parquet",
    "ARROW-GH-47662.parquet",
    "ARROW-RS-GH-6229-DICTHEADER.parquet",
    "ARROW-RS-GH-6229-LEVELS.parquet",
    "PARQUET-1481.parquet",
]

# PyArrow types that map to ESQL UNSUPPORTED
UNSUPPORTED_TYPE_PREFIXES = ("struct", "map", "list<struct", "list<map")


def is_unsupported_type(pa_type_str):
    """Check if a pyarrow type string represents an ESQL-unsupported type."""
    return pa_type_str.startswith(UNSUPPORTED_TYPE_PREFIXES)


def pyarrow_type_to_string(pa_type):
    """Convert a pyarrow type to a canonical string."""
    return str(pa_type)


def is_string_type(pa_type):
    """Check if a pyarrow type represents string data (BINARY with STRING logical annotation)."""
    pa_type_str = str(pa_type)
    return pa_type_str in ("string", "large_string", "utf8", "large_utf8")


def is_raw_binary_type(pa_type):
    """Check if a pyarrow type is raw binary without string annotation."""
    pa_type_str = str(pa_type)
    return pa_type_str in ("binary", "large_binary") or pa_type_str.startswith("fixed_size_binary")


def serialize_value(value, pa_type):
    """Serialize a pyarrow scalar value to a JSON-compatible Python object."""
    if value is None:
        return None

    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        # ESQL converts binary data to UTF-8 strings when possible, falling back to
        # Base64 for non-UTF-8 data. Match that behavior: try UTF-8, then hex.
        if is_raw_binary_type(pa_type):
            try:
                return value.decode("utf-8")
            except UnicodeDecodeError:
                return "0x" + value.hex()
        return "0x" + value.hex()
    if isinstance(value, str):
        return value
    if isinstance(value, datetime):
        # PyArrow returns naive datetimes (no tzinfo) for timestamps without timezone.
        # Treat them as UTC to match ESQL's interpretation.
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        ts_millis = int(value.timestamp() * 1000)
        return {"epoch_millis": ts_millis}
    if isinstance(value, list):
        element_type = pa_type.value_type if hasattr(pa_type, "value_type") else pa_type
        return [serialize_value(v, element_type) for v in value]

    return str(value)


def safe_as_py(scalar, pa_type):
    """Extract a Python value from a pyarrow scalar, handling overflow for INT96 timestamps."""
    try:
        return scalar.as_py()
    except (ValueError, OverflowError):
        # INT96 timestamps can have values outside representable datetime range.
        # Fall back to extracting the raw integer value and converting to millis.
        pa_type_str = str(pa_type)
        if "timestamp" in pa_type_str:
            raw = scalar.value
            if raw is None:
                return None
            # Convert based on resolution
            if "ns" in pa_type_str:
                millis = raw // 1_000_000
            elif "us" in pa_type_str:
                millis = raw // 1_000
            elif "ms" in pa_type_str:
                millis = raw
            else:
                millis = raw * 1000
            try:
                return datetime.fromtimestamp(millis / 1000, tz=timezone.utc).replace(tzinfo=None)
            except (ValueError, OverflowError, OSError):
                return {"epoch_millis": millis, "_overflow": True}
        raise


def process_file(file_path, url):
    """Download and process a single parquet file, returning the ground-truth dict."""
    print(f"  Downloading {url} ...", end=" ", flush=True)
    try:
        data = urllib.request.urlopen(url).read()
    except Exception as e:
        print(f"FAILED: {e}")
        return None
    print(f"({len(data)} bytes)", end=" ", flush=True)

    try:
        table = pq.read_table(io.BytesIO(data))
    except Exception as e:
        print(f"READ FAILED: {e}")
        return None

    columns = []
    supported_columns = []
    for field in table.schema:
        pa_type_str = pyarrow_type_to_string(field.type)
        col_info = {"name": field.name, "pyarrow_type": pa_type_str}
        if is_unsupported_type(pa_type_str):
            col_info["esql_supported"] = False
        else:
            col_info["esql_supported"] = True
            supported_columns.append(field.name)
        columns.append(col_info)

    rows = []
    for i in range(table.num_rows):
        row = {}
        for col_name in supported_columns:
            col_idx = table.schema.get_field_index(col_name)
            pa_type = table.schema.field(col_idx).type
            scalar = table.column(col_name)[i]
            value = safe_as_py(scalar, pa_type)
            row[col_name] = serialize_value(value, pa_type)
        rows.append(row)

    print(f"OK ({table.num_rows} rows, {len(supported_columns)}/{len(columns)} supported cols)")

    return {
        "file": file_path,
        "num_rows": table.num_rows,
        "columns": columns,
        "rows": rows,
    }


def main():
    parser = argparse.ArgumentParser(description="Generate parquet-testing ground truth JSON")
    parser.add_argument(
        "--output-dir",
        default=os.path.join(os.path.dirname(__file__), "parquet-testing-ground-truth"),
        help="Output directory for JSON files",
    )
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    print(f"Parquet-testing commit: {HASH}")
    print(f"Output directory: {args.output_dir}")
    print()

    # Process data/ files
    print(f"Processing {len(DATA_FILES)} data files...")
    skipped = []
    for filename in DATA_FILES:
        url = f"{BASE_URL}/data/{filename}"
        result = process_file(f"data/{filename}", url)
        if result is None:
            skipped.append(filename)
            continue

        # Skip files where ALL columns are unsupported
        supported = [c for c in result["columns"] if c["esql_supported"]]
        if not supported:
            print(f"  SKIPPED {filename}: all columns unsupported")
            skipped.append(filename)
            continue

        json_filename = filename.replace(".parquet", ".json")
        output_path = os.path.join(args.output_dir, json_filename)
        with open(output_path, "w") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
            f.write("\n")

    # Write bad_data manifest (just the file list -- no ground truth data)
    bad_data_manifest = {
        "bad_data_files": [f"bad_data/{f}" for f in BAD_DATA_FILES],
    }
    manifest_path = os.path.join(args.output_dir, "_bad_data_manifest.json")
    with open(manifest_path, "w") as f:
        json.dump(bad_data_manifest, f, indent=2)
        f.write("\n")
    print(f"\nWrote bad_data manifest with {len(BAD_DATA_FILES)} files")

    if skipped:
        print(f"\nSkipped {len(skipped)} files: {', '.join(skipped)}")

    print("\nDone.")


if __name__ == "__main__":
    main()
