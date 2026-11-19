| Format | Schema source | Compression |
|---|---|---|
| Parquet | Read from file headers | Internal per column chunk: uncompressed, snappy, zstd, gzip |
| NDJSON | Inferred by sampling rows | gzip, zstd |
| CSV and TSV | Inferred by sampling rows | gzip, zstd |
