# ESQL NDJSON Data Source Plugin

Provides NDJSON (newline-delimited JSON) format support for ESQL external data sources.

## Features

- Schema inference from the first 100 non-empty lines
- Type conflict resolution to KEYWORD
- Ignores blank lines; logs warnings for malformed lines (does not fail the file)
- Supports `.ndjson` and `.jsonl` extensions

## Usage

Once installed, ESQL will use this plugin for files ending in `.ndjson` or `.jsonl`:

```sql
FROM "https://example.com/data/events.ndjson"
| WHERE status = "ok"
| LIMIT 100
```

## Limitations / TODO

Pages only contain blocks, not attributes. There's no way to know what
these blocks represent (name, type, nullability, etc.)
`SourceMetadata` should be provided to `FormatReader.read()`

1. Avoid rediscovering the schema
2. Ensure data is interpreted as expected by the schema
3. Blocks are laid out in the page in the order they appear in the metadata

## Misc

- `employees.ndjson` created by running `CsvTestsDataLoader` and extracted using
  `curl 'http://localhost:9200/employees/_search?size=1000' | jq -c '.hits.hits[] | ._source'`
  and manually convert "string booleans" to actual booleans.
