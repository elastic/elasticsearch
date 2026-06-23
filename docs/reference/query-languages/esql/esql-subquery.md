---
navigation_title: "Combine result sets with subqueries"
applies_to:
  serverless: preview
  stack: preview 9.4.0
products:
  - id: elasticsearch
---

# {{esql}} subquery [esql-subquery]

A subquery is a complete ES|QL query wrapped in parentheses that can be used
in place of an index pattern in the [`FROM`](/reference/query-languages/esql/commands/from.md) command.
Each subquery is executed independently. The final output combines all these
results into a single list, including any duplicate rows.

## Syntax

```esql
FROM index_pattern [, (FROM index_pattern [METADATA fields] [| processing_commands])]* [METADATA fields] <1>
FROM (FROM index_pattern [METADATA fields] [| processing_commands]) [, (FROM index_pattern [METADATA fields] [| processing_commands])]* [METADATA fields] <2>
```

A subquery starts with a `FROM` source command followed by zero or more piped
processing commands, all enclosed in parentheses. Multiple subqueries and regular
index patterns can be combined in a single `FROM` clause, separated by commas.

1. When an index pattern is present, zero or more subqueries can follow.
2. Without an index pattern, one or more subqueries are required.

## Description

Much like [views](/reference/query-languages/esql/esql-views.md),
subqueries enable you to combine results from multiple independently processed
data sources within a single query. Each subquery runs its own pipeline of
processing commands (such as `WHERE`, `EVAL`, `STATS`, or `SORT`) and the
results are combined together with results from other index patterns, views or subqueries
in the `FROM` clause.

Fields that exist in one source but not another are filled with `null` values.

The subquery pipeline can include commands such as the following:

Source commands:

- [`FROM`](/reference/query-languages/esql/commands/from.md)

Processing commands:

- [`CHANGE_POINT`](/reference/query-languages/esql/commands/change-point.md)
- [`COMPLETION`](/reference/query-languages/esql/commands/completion.md)
- [`DISSECT`](/reference/query-languages/esql/commands/dissect.md)
- [`DROP`](/reference/query-languages/esql/commands/drop.md)
- [`ENRICH`](/reference/query-languages/esql/commands/enrich.md)
- [`EVAL`](/reference/query-languages/esql/commands/eval.md)
- [`GROK`](/reference/query-languages/esql/commands/grok.md)
- [`INLINE STATS`](/reference/query-languages/esql/commands/inlinestats-by.md)
- [`KEEP`](/reference/query-languages/esql/commands/keep.md)
- [`LIMIT`](/reference/query-languages/esql/commands/limit.md)
- [`LOOKUP JOIN`](/reference/query-languages/esql/commands/lookup-join.md)
- [`MV_EXPAND`](/reference/query-languages/esql/commands/mv_expand.md)
- [`RENAME`](/reference/query-languages/esql/commands/rename.md)
- [`RERANK`](/reference/query-languages/esql/commands/rerank.md)
- [`SAMPLE`](/reference/query-languages/esql/commands/sample.md)
- [`SORT`](/reference/query-languages/esql/commands/sort.md)
- [`STATS`](/reference/query-languages/esql/commands/stats-by.md)
- [`WHERE`](/reference/query-languages/esql/commands/where.md)

The [`METADATA` directive](/reference/query-languages/esql/esql-subquery.md#subqueries-with-metadata)
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

### Subqueries with METADATA

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

## Limitations [esql-subquery-limitations]

:::{include} _snippets/common/subquery_limitations.md
:::

## Comparing views, subqueries and FORK

:::{include} _snippets/common/comparing_views_subqueries_fork.md
:::

## Related pages

* [Query multiple sources](/reference/query-languages/esql/esql-multi.md): high-level overview of combining data from multiple indices, clusters, subqueries, and views.
* [Define virtual indices using ES|QL views](/reference/query-languages/esql/esql-views.md): the closest alternative to subqueries, with a persisted, named definition.
* [`FROM` command](/reference/query-languages/esql/commands/from.md): full reference for index expressions, where subqueries are used.
* [`FORK` command](/reference/query-languages/esql/commands/fork.md): the other branching construct in ES|QL, which shares the same branching limits.
* [Query multiple indices](/reference/query-languages/esql/esql-multi-index.md): how index patterns, wildcards, and date math combine sources in a single `FROM`.
