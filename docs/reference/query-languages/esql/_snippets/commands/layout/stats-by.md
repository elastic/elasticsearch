## `STATS` [esql-stats-by]

The `STATS` processing command groups rows according to a common value
and calculates one or more aggregated values over the grouped rows.

**Syntax**

```esql
STATS [column1 =] expression1 [WHERE boolean_expression1][,
      ...,
      [columnN =] expressionN [WHERE boolean_expressionN]]
      [BY grouping_expression1[, ..., grouping_expressionN]]
```

**Parameters**

`columnX`
:   The name by which the aggregated value is returned. If omitted, the name is
    equal to the corresponding expression (`expressionX`).
    If multiple columns have the same name, all but the rightmost column with this
    name will be ignored.

`expressionX`
:   An expression that computes an aggregated value.

`grouping_expressionX`
:   An expression that outputs the values to group by.
    If its name coincides with one of the computed columns, that column will be ignored.

`boolean_expressionX`
:   The condition that must be met for a row to be included in the evaluation of `expressionX`.

::::{note}
Individual `null` values are skipped when computing aggregations.
::::


**Description**

The `STATS` processing command groups rows according to a common value
and calculates one or more aggregated values over the grouped rows. For the
calculation of each aggregated value, the rows in a group can be filtered with
`WHERE`. If `BY` is omitted, the output table contains exactly one row with
the aggregations applied over the entire dataset.

The following [aggregation functions](/reference/query-languages/esql/functions-operators/aggregation-functions.md) are supported:

:::{include} ../../lists/aggregation-functions.md
:::

The following [grouping functions](/reference/query-languages/esql/functions-operators/grouping-functions.md) are supported:

:::{include} ../../lists/grouping-functions.md
:::

::::{note}
`STATS` without any groups is much much faster than adding a group.
::::


::::{note}
Grouping on a single expression is currently much more optimized than grouping
on many expressions. In some tests we have seen grouping on a single `keyword`
column to be five times faster than grouping on two `keyword` columns. Do
not try to work around this by combining the two columns together with
something like [`CONCAT`](/reference/query-languages/esql/functions-operators/string-functions.md#esql-concat)
and then grouping - that is not going to be faster.
::::


**Examples**

Calculating a statistic and grouping by the values of another column:

:::{include} ../examples/stats.csv-spec/stats.md
:::

Omitting `BY` returns one row with the aggregations applied over the entire
dataset:

:::{include} ../examples/stats.csv-spec/statsWithoutBy.md
:::

It’s possible to calculate multiple values:

:::{include} ../examples/stats.csv-spec/statsCalcMultipleValues.md
:::

To filter the rows that go into an aggregation, use the `WHERE` clause:

:::{include} ../examples/stats.csv-spec/aggFiltering.md
:::

The aggregations can be mixed, with and without a filter and grouping is
optional as well:

:::{include} ../examples/stats.csv-spec/aggFilteringNoGroup.md
:::

$$$esql-stats-mv-group$$$
If the grouping key is multivalued then the input row is in all groups:

:::{include} ../examples/stats.csv-spec/mv-group.md
:::

It’s also possible to group by multiple values:

:::{include} ../examples/stats.csv-spec/statsGroupByMultipleValues.md
:::
If all the grouping keys are multivalued then the input row is in all groups:

:::{include} ../examples/stats.csv-spec/multi-mv-group.md
:::

Both the aggregating functions and the grouping expressions accept other
functions. This is useful for using `STATS` on multivalue columns.
For example, to calculate the average salary change, you can use `MV_AVG` to
first average the multiple values per employee, and use the result with the
`AVG` function:

:::{include} ../examples/stats.csv-spec/docsStatsAvgNestedExpression.md
:::

An example of grouping by an expression is grouping employees on the first
letter of their last name:

:::{include} ../examples/stats.csv-spec/docsStatsByExpression.md
:::

Specifying the output column name is optional. If not specified, the new column
name is equal to the expression. The following query returns a column named
`AVG(salary)`:

:::{include} ../examples/stats.csv-spec/statsUnnamedColumn.md
:::

Because this name contains special characters,
[it needs to be quoted](/reference/query-languages/esql/esql-syntax.md#esql-identifiers)
with backticks (```) when using it in subsequent commands:

:::{include} ../examples/stats.csv-spec/statsUnnamedColumnEval.md
:::
