---
navigation_title: "Subqueries"
applies_to:
  serverless: ga
  stack: preview 9.4, ga 9.5+
products:
  - id: elasticsearch
---

# Nest {{esql}} queries using subqueries [esql-subquery]

A subquery is a complete ES|QL query wrapped in parentheses, nested inside another query. Each subquery runs independently and cannot reference columns from the outer query.

You can use subqueries in two places:

* **In a [`FROM` command](/reference/query-languages/esql/esql-from-subquery.md)**: each subquery runs its own pipeline and its rows are combined into the outer result set.
* **In a [`WHERE` command with `IN` or `NOT IN`](/reference/query-languages/esql/esql-in-subquery.md)**: the subquery returns exactly one column, and the outer query filters rows against those values.

## Supported source commands

A subquery starts with one of the following source commands:

- [`FROM`](/reference/query-languages/esql/commands/from.md): read from an index pattern.
- [`TS`](/reference/query-languages/esql/commands/ts.md): read from a time series index pattern.
- [`ROW`](/reference/query-languages/esql/commands/row.md): synthesize rows from literal values.

## Supported processing commands

The source command can be followed by zero or more piped processing commands:

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

## Learn more

* [Use subqueries in a `FROM` command](/reference/query-languages/esql/esql-from-subquery.md): combine result sets from independently processed sources.
* [Use subqueries in a `WHERE` command](/reference/query-languages/esql/esql-in-subquery.md): filter rows with `IN` or `NOT IN`.
