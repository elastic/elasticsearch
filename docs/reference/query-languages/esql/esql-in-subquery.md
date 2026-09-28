---
navigation_title: "Subqueries with IN / NOT IN"
applies_to:
  serverless: preview
  stack: preview 9.5.0
products:
  - id: elasticsearch
---

# Use {{esql}} subqueries with `IN` and `NOT IN` [esql-in-subquery]

An {{esql}} query wrapped in parentheses can be used as a subquery on the
right-hand side of the [`IN` and `NOT IN`](/reference/query-languages/esql/functions-operators/operators.md#esql-in-operator)
operators in the [`WHERE`](/reference/query-languages/esql/commands/where.md)
command.

The subquery filters rows from the outer query by comparing a field or
expression against the set of values it returns. This lets you filter against
the current results of another query without running it separately and copying
its values into a literal `IN` list.

Use `IN` to keep rows whose values match subquery results and `NOT IN` to
exclude them.

{applies_to}`stack: preview 9.6+` You can also use an `IN` or `NOT IN` subquery in [`EVAL`](/reference/query-languages/esql/commands/eval.md) to add a boolean column. The query keeps every row.

## Syntax

```esql
... | WHERE <expression> IN (FROM index_pattern [| processing_commands]) | ...
... | WHERE <expression> NOT IN (FROM index_pattern [| processing_commands]) | ...
```

The subquery starts with a source command followed by zero or more piped
processing commands, all enclosed in parentheses. The source command is usually
[`FROM`](/reference/query-languages/esql/commands/from.md), and
[`ROW`](/reference/query-languages/esql/commands/row.md) and
[`TS`](/reference/query-languages/esql/commands/ts.md) are also supported. In this
form, the subquery must return exactly one column, whose values are compared
against `<expression>`. The column and `<expression>` must have compatible types.

The outer query is not limited to `FROM` either: it can also start with `ROW` or
`TS` and still filter its rows with an `IN` subquery.

### Compare a list of values [compare-a-list-of-values]
```{applies_to}
stack: preview 9.6+
```

To keep or drop rows that match a combination of values, put the values in parentheses. The subquery must return one column for each value, in the same order. Types in the same position must be compatible.

```esql
... | WHERE (<expression>, <expression>, ...) IN (FROM index_pattern [| processing_commands]) | ...
... | WHERE (<expression>, <expression>, ...) NOT IN (FROM index_pattern [| processing_commands]) | ...
```

### Add a boolean column in `EVAL` [in-subquery-in-eval]
```{applies_to}
stack: preview 9.6+
```

To record whether each row matches, assign an `IN` or `NOT IN` subquery in [`EVAL`](/reference/query-languages/esql/commands/eval.md). The new column is boolean. `IN` is true when the values match a subquery row, and `NOT IN` is true when they do not. The query keeps every row. You can compare one value or a parenthesized list.

```esql
... | EVAL <column> = <expression> IN (FROM index_pattern [| processing_commands]) | ...
... | EVAL <column> = <expression> NOT IN (FROM index_pattern [| processing_commands]) | ...
... | EVAL <column> = (<expression>, <expression>, ...) IN (FROM index_pattern [| processing_commands]) | ...
```

## Description

A subquery in a `WHERE` command is non-correlated: it runs independently and
cannot reference columns from the outer query. Because it runs at query time,
its results reflect the current state of the data.

Unlike a [subquery in a `FROM` command](/reference/query-languages/esql/esql-from-subquery.md),
which contributes rows to the combined result set, a subquery in a `WHERE`
command supplies the values that `IN` or `NOT IN` compares. The outer query
keeps or drops rows based on that comparison.

An `IN` subquery can itself contain another `IN` subquery, and multiple `IN`
subqueries can be combined with other predicates using `AND`, `OR`, and `NOT`.

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

### Match several values [match-several-values]
```{applies_to}
stack: preview 9.6+
```

Use a parenthesized list when a row must match on more than one value. `NOT IN` drops rows that match a returned combination.

```esql
FROM employees
| WHERE (emp_no, salary) IN (FROM employees | SORT emp_no ASC | LIMIT 3 | KEEP emp_no, salary)
| SORT emp_no
| KEEP emp_no, first_name, salary
```

| emp_no:integer | first_name:keyword | salary:integer |
| --- | --- | --- |
| 10001 | Georgi | 57305 |
| 10002 | Bezalel | 56371 |
| 10003 | Parto | 61805 |

The subquery returns `emp_no` and `salary` for the first three employees, in that order. The outer query keeps employees whose `emp_no` and `salary` both match one of those rows.

### Add a boolean column with `EVAL` [add-boolean-column-with-eval]
```{applies_to}
stack: preview 9.6+
```

Assign the subquery in `EVAL` when you want the match recorded on every row.

```esql
FROM employees
| EVAL m = emp_no IN (FROM employees | SORT emp_no ASC | LIMIT 3 | KEEP emp_no)
| WHERE emp_no <= 10004
| SORT emp_no ASC
| KEEP emp_no, m
```

| emp_no:integer | m:boolean |
| --- | --- |
| 10001 | true |
| 10002 | true |
| 10003 | true |
| 10004 | false |

The `WHERE` command keeps `emp_no` values through `10004`, so the result shows both a match and a miss. `m` is `true` for `10001`, `10002`, and `10003`, because those values are in the subquery result. It is `false` for `10004`. With `NOT IN`, those three values are `false` and `10004` is `true`.

You can compare a parenthesized list in `EVAL` the same way. The new column is still boolean, and every row stays. If you then filter on that column in `WHERE`, only the matching rows stay.

## Limitations [esql-in-subquery-limitations]

#### `IN` subqueries are only supported in the WHERE command
```{applies_to}
stack: preview =9.5
serverless: unavailable
```

An `IN` subquery can only appear in the [`WHERE`](/reference/query-languages/esql/commands/where.md)
command. It is not supported in other commands.

#### `IN` subqueries in `WHERE` and `EVAL`
```{applies_to}
stack: preview 9.6+
```

An `IN` subquery compares values and returns a boolean.

- In [`WHERE`](/reference/query-languages/esql/commands/where.md), matching rows stay and other rows are dropped.
- In [`EVAL`](/reference/query-languages/esql/commands/eval.md), the comparison adds a boolean column and every row stays.

#### The subquery must return exactly one column
```{applies_to}
stack: preview =9.5
serverless: unavailable
```

The subquery must produce a single column to compare against the left-hand side
expression. A subquery that returns zero or more than one column is rejected.

#### Match the column count to the values you compare
```{applies_to}
stack: preview 9.6+
```

The number of subquery columns must match the values you compare.

- A single value needs one column.
- A parenthesized list needs one column per value, in the same order. Types in the same position must be compatible.

A different column count is rejected.

#### The `IN` subquery must be a top-level predicate

An `IN` subquery must sit at the top of the `WHERE` condition, optionally combined
with other predicates using `AND`, `OR`, and `NOT`. It cannot be used as an
argument to another expression, such as inside a scalar function or an
`IS NOT NULL` check.

#### Subqueries are non-correlated

The subquery is executed independently and cannot reference columns from the
outer query.

## Related pages

* [ES|QL subqueries](/reference/query-languages/esql/esql-subquery.md): canonical definition and supported commands.
* [Use subqueries in a `FROM` command](/reference/query-languages/esql/esql-from-subquery.md): combine result sets from independently processed sources.
* [`WHERE` command](/reference/query-languages/esql/commands/where.md): full reference for the `WHERE` command.
* {applies_to}`stack: preview 9.6+` [`EVAL` command](/reference/query-languages/esql/commands/eval.md): Add a boolean column with an `IN` subquery.
* [`IN` operator](/reference/query-languages/esql/functions-operators/operators.md): the operator used to match against a list of literal values or a subquery.
