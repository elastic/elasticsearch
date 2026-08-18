```yaml {applies_to}
stack: preview 9.6+
serverless: preview
```

`DEDUP` removes duplicate rows from a result set, keeping only one row per unique combination of values across all columns.

## Syntax

```esql
DEDUP
```

## Description

`DEDUP` takes no arguments. It compares all columns currently in scope and discards any row that is an exact duplicate of a previously seen row. Null values are treated as equal for the purpose of deduplication.

`DEDUP` is equivalent to `LIMIT 1 BY <all columns>`.

## Limitations

`DEDUP` cannot be used when any column in scope has one of the following types: `aggregate_metric_double`, counter types (`counter_long`, `counter_integer`, `counter_double`), or `date_range`. Attempting to do so results in a validation error.

Full-text search functions (such as `MATCH` or `KQL`) cannot appear after `DEDUP` in the same pipeline.

## Examples

Remove duplicate values from a single column:

:::{include} ../../generated/x-pack-esql/commands/examples/dedup.csv-spec/dedupBasicForDocs.md
:::

Remove rows that are duplicates across multiple columns:

:::{include} ../../generated/x-pack-esql/commands/examples/dedup.csv-spec/dedupMultipleColumnsForDocs.md
:::
