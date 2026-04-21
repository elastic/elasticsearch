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

- QA runs the shared [`external-basic.csv-spec`](../esql/qa/testFixtures/src/main/resources/external-basic.csv-spec) (same standalone scenarios as other external-format datasource QAs).
- Under `iceberg-fixtures/standalone/`, `employees.ndjson`, `web_logs.ndjson`, and `books.ndjson` are generated at Gradle build time from the canonical CSVs in [`esql/qa/testFixtures/.../data/`](../esql/qa/testFixtures/src/main/resources/data/) via
  `NdJsonFixtureGenerator` in `esql:qa:server` (`generateStandaloneNdjsonFixtures` on the `esql-datasource-ndjson:qa` project).
