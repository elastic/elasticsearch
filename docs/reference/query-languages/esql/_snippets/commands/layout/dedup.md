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

### When to use `LIMIT ... BY` instead

`DEDUP` always keeps exactly one row per unique combination of *all* columns in
scope. Use [`LIMIT ... BY`](/reference/query-languages/esql/commands/limit.md)
directly for the following two cases:

- **Keeping more than one copy of each duplicate.** To retain up to `N` rows per
  unique combination instead of just one, use `LIMIT N BY <all columns>`. Note
  that `LIMIT ... BY` does not support wildcards, so you must list every column
  explicitly:

  :::{include} ../../generated/x-pack-esql/commands/examples/dedup.csv-spec/dedupKeepSeveralCopiesForDocs.md
  :::

- **Deduplicating on a subset of the columns.** To treat rows as duplicates
  based on only some of the columns, while still returning the remaining
  columns, list just those columns in the `BY` clause. Precede it with a
  [`SORT`](/reference/query-languages/esql/commands/sort.md) to control which
  row is kept for each group:

  :::{include} ../../generated/x-pack-esql/commands/examples/dedup.csv-spec/dedupSubsetOfColumnsForDocs.md
  :::

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
