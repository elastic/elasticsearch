| Format | Schema source | Compression |
|---|---|---|
| Parquet | Read from file headers | Internal per column chunk: UNCOMPRESSED, SNAPPY, ZSTD, GZIP |
| NDJSON | Inferred by sampling rows | gzip, zstd |
| CSV and TSV | Inferred by sampling rows | gzip, zstd |
