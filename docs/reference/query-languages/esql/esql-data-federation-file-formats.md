---
navigation_title: "File formats"
description: "Compare the file formats, extensions, compression codecs, and schema sources supported by ES|QL Data Federation datasets."
applies_to:
  stack: experimental 9.5+
  serverless: unavailable
products:
  - id: elasticsearch
---

# File formats for {{esql}} Data Federation

{{esql}} Data Federation reads Parquet, newline-delimited JSON (NDJSON), comma-separated values (CSV), and tab-separated values (TSV) files from external storage. Use the file extension, schema source, and compression support in this reference when defining a [dataset](esql-data-federation-datasets.md).

:::{include} _snippets/data-federation/experimental-warning.md
:::

The following table compares the supported file formats:

| Format | Recognized extensions | Schema source | Compression |
|---|---|---|---|
| Parquet | `.parquet`<br>{applies_to}`stack: experimental 9.6+` `.parq` | File metadata | Internal per column chunk |
| NDJSON | `.ndjson`, `.jsonl`, `.json` | Sampled rows | Uncompressed, gzip, or zstd |
| CSV | `.csv` | Sampled rows | Uncompressed, gzip, or zstd |
| TSV | `.tsv` | Sampled rows | Uncompressed, gzip, or zstd |

## Select one format per dataset

Scope each dataset to one file format. {{es}} infers the format when the resource pattern implies exactly one registered format. For example, `**/*.parquet`, `_schema.parquet,events/**/*.parquet`, and `a.csv,b.csv.gz` each imply one format. Compression suffixes don't count as a second format.

Set [`format`](esql-data-federation-dataset-settings.md#format) explicitly for extensionless resources such as `hits/*` and for mixed patterns such as `*.{parquet,csv}`. Alternatively, use a [resource pattern](esql-data-federation-patterns.md) that selects one format, and create another dataset for files in a different format.

:::{important}
An explicit `format` selects the reader for every file that the resource pattern matches, including files with unrecognized extensions such as `.log.gz`. A file whose extension maps to a different registered format is rejected rather than skipped. For details, refer to [`format`](esql-data-federation-dataset-settings.md#format).
:::

## Compression

A text file can be uncompressed or compressed with a codec identified by its final extension. For example, `clicks.csv`, `clicks.csv.gz`, and `clicks.csv.zst` are all CSV files.

| Codec | Extensions |
|---|---|
| Uncompressed | None |
| gzip | `.gz`, `.gzip` |
| zstd | `.zst`, `.zstd` |

Parquet declares compression internally for each column chunk and does not support whole-file compression. The supported internal codecs are `UNCOMPRESSED`, `SNAPPY`, `ZSTD`, `GZIP`, `LZ4_RAW`, and legacy Hadoop-framed `LZ4` for reading.

## Schema handling

Parquet files include their schema in file metadata. {{es}} infers schemas for CSV, TSV, and NDJSON by sampling rows. Refer to [schema inference](esql-data-federation-schema.md) to learn how {{es}} reconciles schemas across multiple files.
