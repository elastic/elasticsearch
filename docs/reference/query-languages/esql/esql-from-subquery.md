---
navigation_title: "Subqueries with FROM"
applies_to:
  serverless: ga
  stack: preview 9.4, ga 9.5+
products:
  - id: elasticsearch
---

# Use {{esql}} subqueries in a `FROM` command [esql-from-subquery]

A [subquery](/reference/query-languages/esql/esql-subquery.md) can be used
in place of an index pattern in the [`FROM`](/reference/query-languages/esql/commands/from.md) command.
Each subquery is executed independently. The final output combines all these
results into a single list, including any duplicate rows.

## Syntax

```esql
FROM index_pattern [, subquery]* [METADATA fields] <1>
FROM subquery [, subquery]* [METADATA fields] <2>
```

1. When an index pattern is present, zero or more subqueries can follow.
2. Without an index pattern, one or more subqueries are required.

Multiple subqueries and regular index patterns can be combined in a single
`FROM` clause, separated by commas.

## Description

Much like [views](/reference/query-languages/esql/esql-views.md),
subqueries in a `FROM` command enable you to combine results from multiple independently processed
data sources within a single query. Each subquery runs its own pipeline of
processing commands (such as `WHERE`, `EVAL`, `STATS`, or `SORT`) and the
results are combined together with results from other index patterns, views or subqueries
in the `FROM` clause.

Fields that exist in one source but not another are filled with `null` values.

The [`METADATA` directive](/reference/query-languages/esql/esql-metadata-fields.md)
is also supported on either the subquery or the outer `FROM`.

## Examples

The following examples show how to use subqueries within the `FROM` command.

### Combine data from multiple indices

Use a subquery alongside a regular index pattern to combine results from
different indices:

:::{include} _snippets/generated/x-pack-esql/commands/examples/subquery.csv-spec/basic_subquery.md
:::

Rows from `employees` have `null` for `client_ip`, while rows from `sample_data`
have `null` for `emp_no` and `languages`, because each index has different fields.

### Use only subqueries (no main index pattern)

You can use one or more subqueries without specifying a regular index pattern:

:::{include} _snippets/generated/x-pack-esql/commands/examples/subquery.csv-spec/subquery_only.md
:::

The `FROM` clause contains only a subquery with no regular index pattern. The
subquery wraps the `employees` index, and the outer query filters, sorts, and
projects the results.

### Filter data inside a subquery

Apply a `WHERE` clause inside the subquery to pre-filter data before combining:

:::{include} _snippets/generated/x-pack-esql/commands/examples/subquery.csv-spec/subquery_with_filter.md
:::

The `WHERE` inside the subquery filters `sample_data` to only rows where
`client_ip` is `172.21.3.15` before combining with `employees`. The `_index`
metadata field shows which index each row originated from.

### Aggregate data inside a subquery

Use `STATS` inside a subquery to aggregate data before combining with other sources:

:::{include} _snippets/generated/x-pack-esql/commands/examples/subquery.csv-spec/subquery_with_aggregation.md
:::

The `STATS` inside the subquery aggregates `sample_data` by counting rows per
`client_ip` before combining with `employees`. The `cnt` column is `null` for
`employees` rows since that field only exists in the subquery output.

### Combine multiple subqueries

Multiple subqueries can be combined in a single `FROM` clause:

:::{include} _snippets/generated/x-pack-esql/commands/examples/subquery.csv-spec/multiple_subqueries.md
:::

Two subqueries aggregate `sample_data` and `sample_data_str` separately, each
counting rows by `client_ip`. The results are combined and then filtered to only
show rows where `client_ip` is `172.21.3.15`. The `_index` field confirms each
row's source.

### Use LOOKUP JOIN inside a subquery

Enrich subquery results with a lookup join before combining:

:::{include} _snippets/generated/x-pack-esql/commands/examples/subquery.csv-spec/subquery_with_lookup_join.md
:::

The `LOOKUP JOIN` inside the subquery joins each `sample_data` row with the
`env` field from `clientips_lookup` based on `client_ip`. The `env` column is
`null` for `employees` rows since the lookup only applies within the subquery.

### Sort and limit inside a subquery

Use `SORT` and `LIMIT` inside a subquery to return only top results:

:::{include} _snippets/generated/x-pack-esql/commands/examples/subquery.csv-spec/subquery_with_sort.md
:::

The subquery aggregates `sample_data` by `client_ip`, sorts by count in
descending order, and limits to the top result. Only the `client_ip` with the
highest count (`172.21.3.15` with 4 occurrences) is included when combined with
`employees`.

### Combine time series data with a TS subquery
```{applies_to}
stack: ga 9.5+
```

Use a [`TS`](/reference/query-languages/esql/commands/ts.md) subquery to read
from a time series index and combine the results with a regular index:

:::{include} _snippets/generated/x-pack-esql/commands/examples/subquery_with_ts_source.csv-spec/ts_subquery.md
:::

The `TS` subquery reads the `k8s` time series index and uses the
[`RATE`](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions/rate.md)
function to compute the maximum per-second rate of the `network.total_bytes_in`
counter per `cluster`. The outer query combines these aggregates with
`sample_data`, derives `rate_per_minute` from `max_rate`, and keeps the time
series rows plus the `sample_data` rows for client IP `172.21.3.15`. Because each
branch exposes different fields, `sample_data` rows have `null` for `cluster`,
`max_rate`, and `rate_per_minute`, while the time series rows have `null` for
`client_ip`.

### Use only TS subqueries
```{applies_to}
stack: ga 9.5+
```

Combine several `TS` subqueries in a single `FROM` clause, without a regular
index pattern:

:::{include} _snippets/generated/x-pack-esql/commands/examples/subquery_with_ts_source.csv-spec/multiple_ts_subqueries.md
:::

Each `TS` subquery aggregates a different time series index (`k8s` by `cluster`
and `k8s-downsampled` by `pod`). Because the two branches expose different
fields, each row carries `null` for the columns that come from the other
subquery.

### Inline dataset with a ROW subquery
```{applies_to}
stack: ga 9.5+
```

Use a [`ROW`](/reference/query-languages/esql/commands/row.md) subquery to
introduce rows with literal values alongside data read from an index:

:::{include} _snippets/generated/x-pack-esql/commands/examples/subquery.csv-spec/row_subquery.md
:::

The `ROW` subquery contributes a single synthesized `(emp_no, languages)` row
that is combined with the matching rows from `employees`.

### Use only ROW subqueries
```{applies_to}
stack: ga 9.5+
```

Multiple `ROW` subqueries can be combined without a regular index pattern:

:::{include} _snippets/generated/x-pack-esql/commands/examples/subquery.csv-spec/row_subquery_only.md
:::

Each `ROW` subquery contributes one row, and the outer query sorts and projects
the combined results.

### Combine FROM, TS, and ROW subqueries
```{applies_to}
stack: ga 9.5+
```

Different source commands can be mixed in a single `FROM` clause. This example
combines a regular index pattern with a `FROM` subquery, a `TS` subquery, and a
`ROW` subquery:

:::{include} _snippets/generated/x-pack-esql/commands/examples/subquery_with_ts_source.csv-spec/mixed_source_subqueries.md
:::

Each branch contributes its own rows to the combined result: `sample_data` and
the `FROM` subquery provide the `client_ip` rows, the `TS` subquery provides the
per-`cluster` aggregates, and the `ROW` subquery provides the synthesized
`synthetic` row. Fields that don't exist in a given branch are filled with
`null`.

### Subqueries with METADATA [subqueries-with-metadata]

The [`METADATA` directive](/reference/query-languages/esql/esql-metadata-fields.md) is supported both inside and outside a subquery.
If the directive is used only outside the subquery, it will report `null` for the values within the subquery:

:::{include} _snippets/generated/x-pack-esql/commands/examples/subquery.csv-spec/subquery_with_metadata_outer.md
:::

To see the combined values from within the subquery include the directive inside as well:

:::{include} _snippets/generated/x-pack-esql/commands/examples/subquery.csv-spec/subquery_with_metadata.md
:::

If you only have the directive within the subquery, null values will be returned for the indices outside the subquery:

:::{include} _snippets/generated/x-pack-esql/commands/examples/subquery.csv-spec/subquery_with_metadata_inner.md
:::

## Limitations [esql-from-subquery-limitations]

:::{include} _snippets/common/subquery_limitations.md
:::

## Related pages

* [ES|QL subqueries](/reference/query-languages/esql/esql-subquery.md): canonical definition, supported commands, and links to both subquery usage contexts.
* [Use subqueries in a `WHERE` command](/reference/query-languages/esql/esql-in-subquery.md): filter rows using subqueries with `IN` and `NOT IN`.
* [Define virtual indices using ES|QL views](/reference/query-languages/esql/esql-views.md): the closest alternative to subqueries in `FROM`, with a persisted, named definition.
* [`FROM` command](/reference/query-languages/esql/commands/from.md): full reference for index expressions.
* [`FORK` command](/reference/query-languages/esql/commands/fork.md): the other branching construct in ES|QL, which shares the same branching limits.
