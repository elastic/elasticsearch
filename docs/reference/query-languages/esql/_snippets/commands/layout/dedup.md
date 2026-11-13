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

A multivalued column is compared as a whole: two values are equal only if they hold the same values in the same order. `[a, b]`, `[b, a]` and `[a, a, b]` are three different values, so `DEDUP` keeps a row for each of them. This differs from [`STATS ... BY`](/reference/query-languages/esql/commands/stats-by.md), which puts a row with a multivalued grouping key into one group for each of its values.

After a [`SORT`](/reference/query-languages/esql/commands/sort.md), the rows that `DEDUP` keeps stay in sort order, each at the position of its first occurrence. Without a `SORT`, the order of the output rows is not defined.

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

### Performance

Every column in scope is part of the comparison, so `DEDUP` reads every column of every row before it can discard any. When nothing before `DEDUP` selects columns, as in `FROM my-index | DEDUP`, that means every field in the mapping, which can be expensive for indices with many fields. To reduce the work:

- Use [`KEEP`](/reference/query-languages/esql/commands/keep.md) before `DEDUP` to select only the columns you need.
- If rows are duplicates when a few key columns match, use `LIMIT 1 BY <key columns>` instead. It compares only the key columns, so the other columns can be loaded later, only for the rows it keeps.

## Limitations

`DEDUP` cannot be used when any column in scope has one of the following types: `aggregate_metric_double`, counter fields (`counter_long`, `counter_integer`, `counter_double`), `date_range`, `exponential_histogram`, `tdigest`, `date_period`, or `time_duration`. Attempting to do so results in a validation error. Use `KEEP` or `DROP` before `DEDUP` to remove such columns.

Columns of an `unsupported` type are ignored when comparing rows, and `DEDUP` does not fail because of them. This includes fields mapped to incompatible types across the indices of a `FROM` (a union type): such a field is returned as `null` until it is converted, so it cannot tell rows apart. For example, if `emp_no` is `integer` in one index and `long` in another, `FROM employees, employees_incompatible | KEEP emp_no | DEDUP` returns a single row. A later conversion such as `EVAL e = emp_no::long` does not bring the removed rows back. To deduplicate on the value of such a field, convert it first and then `DEDUP`: `EVAL e = emp_no::long | DROP emp_no | DEDUP`.

Full-text searches on indexed fields (such as `MATCH` or `KQL`) can't appear after `DEDUP` in the same pipeline.

## Examples

Remove duplicate values from a single column:

:::{include} ../../generated/x-pack-esql/commands/examples/dedup.csv-spec/dedupBasicForDocs.md
:::

Remove rows that are duplicates across multiple columns:

:::{include} ../../generated/x-pack-esql/commands/examples/dedup.csv-spec/dedupMultipleColumnsForDocs.md
:::
