---
navigation_title: "Subqueries with IN / NOT IN"
applies_to:
  serverless: ga
  stack: preview =9.5.0, ga 9.6+
products:
  - id: elasticsearch
---

# Use {{esql}} subqueries with `IN` and `NOT IN` [esql-in-subquery]

An {{esql}} query wrapped in parentheses can be used as a subquery on the
right-hand side of the [`IN` and `NOT IN`](/reference/query-languages/esql/functions-operators/operators.md#esql-in-operator)
operators. The subquery can appear in the
[`WHERE`](/reference/query-languages/esql/commands/where.md) command, the
[`EVAL`](/reference/query-languages/esql/commands/eval.md) command, and the
per-aggregate `WHERE` filter of
[`STATS`](/reference/query-languages/esql/commands/stats-by.md) and
[`INLINE STATS`](/reference/query-languages/esql/commands/inlinestats-by.md)
{applies_to}`stack: ga 9.6.0`.

The subquery compares a field, expression, or tuple of expressions against the
set of values it returns. This lets you match against the current results of
another query without running it separately and copying its values into a
literal `IN` list.

Use `IN` to keep rows whose values match subquery results and `NOT IN` to
exclude them. In `EVAL`, the same predicates produce a boolean column instead of
filtering rows.

## Syntax

```esql
... | WHERE <expression> IN (FROM index_pattern [| processing_commands]) | ...
... | WHERE (<e1>, <e2>[, ...]) IN (FROM index_pattern [| processing_commands]) | ...
... | EVAL <column> = <expression> IN (FROM index_pattern [| processing_commands]) | ...
... | STATS <agg> WHERE <expression> IN (FROM index_pattern [| processing_commands]) | ...
... | INLINE STATS <agg> WHERE <expression> IN (FROM index_pattern [| processing_commands]) | ...
```

`NOT IN` is supported in the same positions. The tuple form
{applies_to}`stack: ga 9.6.0` also works
in `EVAL` and in the per-aggregate `WHERE` of `STATS` and `INLINE STATS`.

The subquery starts with a source command followed by zero or more piped
processing commands, all enclosed in parentheses. The source command is usually
[`FROM`](/reference/query-languages/esql/commands/from.md), and
[`ROW`](/reference/query-languages/esql/commands/row.md) and
[`TS`](/reference/query-languages/esql/commands/ts.md) are also supported.

For a single-column `IN` subquery, the subquery must return exactly one column,
whose values are compared against the left-hand side of the `IN` subquery. For a
multi-column `IN` subquery, wrap two or more expressions in parentheses on the
left-hand side {applies_to}`stack: ga 9.6.0`. The subquery must then return the
same number of columns, which are compared **by position** against the tuple.
Each pair of columns must have compatible types. A single parenthesized field
such as `(emp_no) IN (...)` is still a single-column `IN` subquery, not a
multi-column `IN` subquery.

The outer query is not limited to `FROM` either: it can also start with `ROW` or
`TS` and still use an `IN` subquery.

## Description

An `IN` subquery is non-correlated: it runs independently and cannot reference
columns from the outer query. Because it runs at query time, its results reflect
the current state of the data.

Unlike a [subquery in a `FROM` command](/reference/query-languages/esql/esql-from-subquery.md),
which contributes rows to the combined result set, an `IN` subquery returns
either one column or a tuple of columns. The outer `IN` or `NOT IN` predicate
uses those values as its comparison set.

An `IN` subquery can itself contain another `IN` subquery, and multiple `IN`
subqueries can be combined with other predicates using `AND`, `OR`, and `NOT`.
An `IN` subquery can also sit inside
[`CASE`](/reference/query-languages/esql/functions-operators/conditional-functions-and-expressions/case.md),
[`COALESCE`](/reference/query-languages/esql/functions-operators/conditional-functions-and-expressions/coalesce.md),
[`IS NULL`](/reference/query-languages/esql/functions-operators/operators.md#esql-is_null),
[`IS NOT NULL`](/reference/query-languages/esql/functions-operators/operators.md#esql-is_not_null),
[`==`](/reference/query-languages/esql/functions-operators/operators.md#esql-equals),
and [`!=`](/reference/query-languages/esql/functions-operators/operators.md#esql-not_equals)
{applies_to}`stack: ga 9.6.0`.

For the full list of supported source and processing commands inside a subquery, refer to [ES|QL subqueries](/reference/query-languages/esql/esql-subquery.md).

## Examples

The following examples show how to use `IN` subqueries.

### Filter by values from a subquery

Use `IN` to keep only the rows whose value is contained in the subquery result:

:::{include} _snippets/generated/x-pack-esql/commands/examples/in_subquery.csv-spec/basic_in_subquery.md
:::

The subquery selects the `emp_no` of every employee earning more than `70000`,
and the outer query keeps only those employees.

### Exclude values with NOT IN

Use `NOT IN` to keep only the rows whose value is *not* contained in the subquery
result:

:::{include} _snippets/generated/x-pack-esql/commands/examples/in_subquery.csv-spec/not_in_subquery.md
:::

Here the subquery selects every employee earning less than `10000`. No employee
matches, so the subquery returns an empty result. `NOT IN` therefore excludes
nothing and every employee is kept, so the first three rows by `emp_no` are
`10001` through `10003`.

### Aggregate data inside the subquery

Use `STATS` inside the subquery to compare against aggregated values:

:::{include} _snippets/generated/x-pack-esql/commands/examples/in_subquery.csv-spec/in_subquery_with_aggregation.md
:::

The subquery computes the maximum `salary` per language group, and the outer
query keeps only the employees whose `salary` matches one of those maximums.

### Combine multiple subqueries

Multiple `IN` subqueries can be combined with other predicates using `AND`, `OR`,
and `NOT`:

:::{include} _snippets/generated/x-pack-esql/commands/examples/in_subquery.csv-spec/multiple_in_subqueries.md
:::

This query keeps employees who do not speak the two languages, earn more than
`70000`, and have a known number of languages.

### Use LOOKUP JOIN inside the subquery

Use a lookup join inside the subquery before building the list of values:

:::{include} _snippets/generated/x-pack-esql/commands/examples/in_subquery.csv-spec/in_subquery_with_lookup_join.md
:::

The subquery joins each employee's `languages` code with the `languages_lookup`
index, keeps only German speakers, and returns their `emp_no`. The outer query
then keeps only those employees.

### Use ROW as the subquery source

The subquery can start with a [`ROW`](/reference/query-languages/esql/commands/row.md)
source command to match against an inline list of values:

:::{include} _snippets/generated/x-pack-esql/commands/examples/in_subquery.csv-spec/in_subquery_with_row.md
:::

The `ROW` command builds a multivalued field, `MV_EXPAND` turns it into one value
per row, and the outer query keeps only the employees whose `emp_no` is one of
those values.

### Use ROW as the outer source

The outer query can also start with `ROW`, using an `IN` subquery to keep only the
values that exist in another index:

:::{include} _snippets/generated/x-pack-esql/commands/examples/in_subquery.csv-spec/row_source_with_in_subquery.md
:::

The `ROW` command provides a list of candidate `emp_no` values, and the subquery
filters out `99999`, which does not match any employee.

### Use TS as the subquery source

The subquery can start with a [`TS`](/reference/query-languages/esql/commands/ts.md)
source command to build the list of values from time series data. In this example
the outer query reads the downsampled `k8s-downsampled` index with `FROM`, while
the subquery uses `TS` over the raw `k8s` index:

:::{include} _snippets/generated/x-pack-esql/commands/examples/in_subquery_with_ts_source.csv-spec/in_subquery_with_ts.md
:::

The subquery aggregates the raw time series metrics to find the clusters whose peak
ingest exceeds a threshold, and the outer query then reports the downsampled buckets
for those clusters. The outer `FROM` and the `TS` subquery reference different
indices because a single index cannot be read as both a standard source (`FROM`)
and a time series source (`TS`) in the same query.

### Use TS as the outer source

The outer query can also start with `TS`, so both the outer query and the subquery
read from time series data:

:::{include} _snippets/generated/x-pack-esql/commands/examples/in_subquery_with_ts_source.csv-spec/ts_source_with_in_subquery.md
:::

The subquery keeps only the clusters whose maximum ingested bytes exceed `10000`,
and the outer query reports those clusters.

### Combine ROW, FROM, and TS sources in one subquery

A subquery can union several source commands with the `FROM (...)` syntax, mixing
[`ROW`](/reference/query-languages/esql/commands/row.md),
[`FROM`](/reference/query-languages/esql/commands/from.md), and
[`TS`](/reference/query-languages/esql/commands/ts.md) branches. Each branch must
produce the same single column:

:::{include} _snippets/generated/x-pack-esql/commands/examples/in_subquery_with_ts_source.csv-spec/in_subquery_with_mixed_sources.md
:::

The `ROW` branch contributes `staging`, the `FROM` branch contributes `prod`, and
the `TS` branch contributes `qa`. Their union is the list of values the outer query
matches against, so all three clusters are kept.

### Nest `IN` subqueries in complex conditions

`IN` and `NOT IN` subqueries can be nested inside arbitrarily complex boolean
conditions built from `AND`, `OR`, and `NOT`:

:::{include} _snippets/generated/x-pack-esql/commands/examples/in_subquery.csv-spec/nested_in_subquery.md
:::

The outer query keeps an employee when either their `emp_no` is returned by the
first `IN` subquery (salaries above `73000`), or they are female and either earn more
than `70000` or their `emp_no` is *not* returned by the `NOT IN` subquery.

### Use an `IN` subquery inside a FROM subquery

An `IN` subquery can also appear inside a [subquery in the `FROM` command](/reference/query-languages/esql/esql-from-subquery.md).
Here the `FROM` command unions two employee sources, and the second branch is
filtered with an `IN` subquery that itself nests a `NOT IN` subquery:

:::{include} _snippets/generated/x-pack-esql/commands/examples/in_subquery.csv-spec/in_subquery_in_from_subquery.md
:::

The first `FROM` branch keeps every employee earning more than `70000`. The second
branch keeps employees who speak language one and whose `emp_no` is returned by
the `IN` subquery, which in turn excludes the three highest salaries with a nested
`NOT IN` subquery. The outer query reports the union of both branches.

### Combine an `IN` subquery with `FORK`

`IN` subqueries can be used inside [`FORK`](/reference/query-languages/esql/commands/fork.md)
branches, so each branch can apply its own subquery-based filter:

:::{include} _snippets/generated/x-pack-esql/commands/examples/in_subquery.csv-spec/in_subquery_with_fork.md
:::

The first `FORK` branch keeps the high earners returned by its `IN` subquery, the
second branch keeps the low earners returned by its `IN` subquery, and the `_fork`
column records which branch produced each row.

### Compute a boolean column in EVAL
```{applies_to}
stack: ga 9.6.0
```

Use `EVAL` to store the `IN` result as a boolean column. Every input row is kept:

:::{include} _snippets/generated/x-pack-esql/commands/examples/in_subquery.csv-spec/in_subquery_in_eval.md
:::

The first three employees match the subquery, so `m` is `true` for those rows and
`false` for `10004`.

### Filter aggregations in STATS and INLINE STATS
```{applies_to}
stack: ga 9.6.0
```

Use an `IN` subquery in the per-aggregate `WHERE` of
[`STATS`](/reference/query-languages/esql/commands/stats-by.md) to include only
matching rows in that aggregation:

:::{include} _snippets/generated/x-pack-esql/commands/examples/in_subquery.csv-spec/in_subquery_in_stats_where.md
:::

Multiple aggregations can each have their own `IN` filter, and you can mix them
with an unfiltered aggregation:

:::{include} _snippets/generated/x-pack-esql/commands/examples/in_subquery.csv-spec/multiple_in_subqueries_in_stats_where.md
:::

The same per-aggregate filter works with
[`INLINE STATS`](/reference/query-languages/esql/commands/inlinestats-by.md). The
count is appended to every input row:

:::{include} _snippets/generated/x-pack-esql/commands/examples/in_subquery.csv-spec/in_subquery_in_inline_stats_where.md
:::

Three employees match the subquery, so `c` is `3` on every returned row.

### Use an `IN` subquery inside an expression
```{applies_to}
stack: ga 9.6.0
```

An `IN` subquery can be nested inside
[`CASE`](/reference/query-languages/esql/functions-operators/conditional-functions-and-expressions/case.md),
[`COALESCE`](/reference/query-languages/esql/functions-operators/conditional-functions-and-expressions/coalesce.md),
[`IS NULL`](/reference/query-languages/esql/functions-operators/operators.md#esql-is_null),
[`IS NOT NULL`](/reference/query-languages/esql/functions-operators/operators.md#esql-is_not_null),
[`==`](/reference/query-languages/esql/functions-operators/operators.md#esql-equals),
and [`!=`](/reference/query-languages/esql/functions-operators/operators.md#esql-not_equals).

Use `CASE` to treat the subquery as a condition:

:::{include} _snippets/generated/x-pack-esql/commands/examples/in_subquery.csv-spec/case_when_in_subquery.md
:::

In `EVAL`, `CASE` can map the same boolean result to another value:

:::{include} _snippets/generated/x-pack-esql/commands/examples/in_subquery.csv-spec/case_with_in_subquery_in_eval.md
:::

Use `COALESCE` to replace a possible `null` match with a default:

:::{include} _snippets/generated/x-pack-esql/commands/examples/in_subquery.csv-spec/coalesce_in_subquery.md
:::

Use `IS NULL` or `IS NOT NULL` to test whether the match itself is `null`. Here
every `emp_no` produces a definite `true` or `false`, so `IS NULL` matches no
rows:

:::{include} _snippets/generated/x-pack-esql/commands/examples/in_subquery.csv-spec/is_null_in_subquery.md
:::

Compare the boolean result with `==` or `!=`:

:::{include} _snippets/generated/x-pack-esql/commands/examples/in_subquery.csv-spec/in_subquery_in_equals.md
:::

### Match a tuple of values
```{applies_to}
stack: ga 9.6.0
```

Wrap two or more expressions in parentheses to compare a tuple against the
subquery. Columns are matched **by position**, not by name, so the subquery must
return the same number of columns in the same order, and each pair must have
compatible types:

:::{include} _snippets/generated/x-pack-esql/commands/examples/in_subquery_multi_column.csv-spec/basic_multi_column_in_subquery.md
:::

The subquery returns the `(emp_no, salary)` pairs of the first three employees,
and the outer query keeps only those exact pairs.

Use `NOT IN` to exclude matching tuples:

:::{include} _snippets/generated/x-pack-esql/commands/examples/in_subquery_multi_column.csv-spec/basic_multi_column_not_in_subquery.md
:::

You can store a tuple match as a boolean column in `EVAL`:

:::{include} _snippets/generated/x-pack-esql/commands/examples/in_subquery_multi_column.csv-spec/multi_column_in_subquery_in_eval.md
:::

Or use a tuple as a per-aggregate `STATS` filter:

:::{include} _snippets/generated/x-pack-esql/commands/examples/in_subquery_multi_column.csv-spec/multi_column_in_subquery_in_stats_where.md
:::

A tuple `IN` can also sit inside `CASE`, `COALESCE`, `IS [NOT] NULL`, `==`, and
`!=`:

:::{include} _snippets/generated/x-pack-esql/commands/examples/in_subquery_multi_column.csv-spec/multi_column_in_subquery_in_case.md
:::

You can combine single-column and multi-column `IN` subqueries with `AND` and
`OR`.

## Limitations [esql-in-subquery-limitations]

#### Supported commands

An `IN` subquery can appear in the
[`WHERE`](/reference/query-languages/esql/commands/where.md) command. It can also
appear in the [`EVAL`](/reference/query-languages/esql/commands/eval.md) command
and the per-aggregate `WHERE` filter of
[`STATS`](/reference/query-languages/esql/commands/stats-by.md) and
[`INLINE STATS`](/reference/query-languages/esql/commands/inlinestats-by.md)
{applies_to}`stack: ga 9.6.0`.

It is not supported in the other commands like [`SORT`](/reference/query-languages/esql/commands/sort.md),
[`LIMIT ... BY`](/reference/query-languages/esql/commands/limit.md), as a `STATS` or `INLINE STATS` grouping expression, or as an
argument of an aggregation function such as
`STATS c = SUM(CASE(x IN (...), 1, 0))`.

#### The subquery must return the expected number of columns

A single-column `IN` subquery must return exactly one column. A multi-column
`IN` subquery must return the same number of columns as the left-hand tuple
{applies_to}`stack: ga 9.6.0`.
A subquery that returns the wrong number of columns is rejected.

#### Supported expressions

An `IN` subquery can be combined with other predicates using `AND`, `OR`, and
`NOT`. It can also be nested inside
[`CASE`](/reference/query-languages/esql/functions-operators/conditional-functions-and-expressions/case.md),
[`COALESCE`](/reference/query-languages/esql/functions-operators/conditional-functions-and-expressions/coalesce.md),
[`IS NULL`](/reference/query-languages/esql/functions-operators/operators.md#esql-is_null),
[`IS NOT NULL`](/reference/query-languages/esql/functions-operators/operators.md#esql-is_not_null),
[`==`](/reference/query-languages/esql/functions-operators/operators.md#esql-equals),
and [`!=`](/reference/query-languages/esql/functions-operators/operators.md#esql-not_equals)
{applies_to}`stack: ga 9.6.0`.
If the subquery sits inside `CASE`, `COALESCE`, or `IS [NOT] NULL`, that whole
expression can itself be nested in another expression.

Other functions, or a computed left-hand side such as `ABS(emp_no) IN (...)`
are not supported. In a `STATS` or `INLINE STATS` per-aggregate `WHERE`, the
left-hand side also cannot be a grouping alias.

#### Subqueries are non-correlated

The subquery is executed independently and cannot reference columns from the
outer query.

## Related pages

* [ES|QL subqueries](/reference/query-languages/esql/esql-subquery.md): canonical definition and supported commands.
* [Use subqueries in a `FROM` command](/reference/query-languages/esql/esql-from-subquery.md): combine result sets from independently processed sources.
* [`WHERE` command](/reference/query-languages/esql/commands/where.md): full reference for the `WHERE` command.
* [`EVAL` command](/reference/query-languages/esql/commands/eval.md): compute a boolean column from an `IN` subquery.
* [`STATS` command](/reference/query-languages/esql/commands/stats-by.md): per-aggregate `WHERE` filters, including `IN` subqueries.
* [`INLINE STATS` command](/reference/query-languages/esql/commands/inlinestats-by.md): per-aggregate `WHERE` filters that preserve input rows.
* [`IN` operator](/reference/query-languages/esql/functions-operators/operators.md): the operator used to match against a list of literal values or a subquery.
