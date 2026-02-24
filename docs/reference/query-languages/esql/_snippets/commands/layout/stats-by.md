```yaml {applies_to}
serverless: ga
stack: ga
```

The `STATS` processing command groups rows according to a common value
and calculates one or more aggregated values over the grouped rows.

## Syntax

```esql
STATS [column1 =] expression1 [WHERE boolean_expression1][,
      ...,
      [columnN =] expressionN [WHERE boolean_expressionN]]
      [BY grouping_expression1[, ..., grouping_expressionN]]
```

## Parameters

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
:   The condition that must be met for a row to be included in the evaluation of
    `expressionX`. Has no effect on `grouping_expressionX` or other aggregation
    expressions. Consequently, the following are _not_ equivalent:

    ```esql
    ... | STATS ... WHERE <condition> ...
    ```

    ```esql
    ... | WHERE <condition> | STATS ...
    ```

::::{note}
Individual `null` values are skipped when computing aggregations.
::::


## Description

The `STATS` processing command groups rows according to a common value
and calculates one or more aggregated values over the grouped rows. For the
calculation of each aggregated value, the rows in a group can be filtered with
`WHERE`. If `BY` is omitted, the output table contains exactly one row with
the aggregations applied over the entire dataset.

The following [aggregation functions](/reference/query-languages/esql/functions-operators/aggregation-functions.md) are supported:

:::{include} ../../lists/aggregation-functions.md
:::

When `STATS` is used under the [`TS`](/reference/query-languages/esql/commands/ts.md) source command,
[time series aggregation functions](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions.md)
are also supported.

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


## Examples

The following examples demonstrate common `STATS` patterns.

### Group by column

Combine an aggregation with `BY` to compute a value for each group:

:::{include} ../examples/stats.csv-spec/stats.md
:::

### Aggregate without grouping

Omitting `BY` returns one row with the aggregations applied over the entire
dataset:

:::{include} ../examples/stats.csv-spec/statsWithoutBy.md
:::

### Calculate multiple values

Separate multiple aggregations with commas to compute them in a single pass:

:::{include} ../examples/stats.csv-spec/statsCalcMultipleValues.md
:::

### Filter aggregations with WHERE

Use per-aggregation `WHERE` to compute conditional metrics from the same
dataset in a single pass:

:::{include} ../examples/stats.csv-spec/aggFiltering.md
:::

### Mix filtered and unfiltered aggregations

Filtered and unfiltered aggregations can be freely mixed. Grouping is also
optional:

:::{include} ../examples/stats.csv-spec/aggFilteringNoGroup.md
:::

### Filter on the grouping key

The `WHERE` clause can also filter on the grouping key. The group itself will
still appear in the output, but with a default value for the aggregation:

:::{include} ../examples/stats.csv-spec/aggFilteringOnGroup.md
:::

Compare this to filtering with `WHERE` before `STATS`, where rows are excluded
before grouping, so non-matching groups don't appear in the output at all:

:::{include} ../examples/stats.csv-spec/aggFilteringBefore.md
:::

### Group by multiple values

Separate multiple grouping expressions with a comma:

:::{include} ../examples/stats.csv-spec/statsGroupByMultipleValues.md
:::

$$$esql-stats-mv-group$$$
### Multivalued inputs

If the grouping key is multivalued then the input row is in all groups:

:::{include} ../examples/stats.csv-spec/mv-group.md
:::

If all the grouping keys are multivalued then the input row is in all groups:

:::{include} ../examples/stats.csv-spec/multi-mv-group.md
:::

The input **ROW** is in all groups. The entire row. All the values. Even group
keys. That means that:

:::{include} ../examples/stats.csv-spec/mv-group-values.md
:::

The `VALUES` function above sees the whole row - all of the values of the group
key. If you want to send the group key to the function then `MV_EXPAND` first:

:::{include} ../examples/stats.csv-spec/mv-group-values-expand.md
:::

Refer to [elasticsearch/issues/134792](https://github.com/elastic/elasticsearch/issues/134792#issuecomment-3361168090)
for an even more in depth explanation.

### Multivalue functions

Both aggregation and grouping expressions accept other functions, which is
useful for using `STATS` on multivalue columns. For example, to calculate the
average salary change, use `MV_AVG` to first average the multiple values per
employee, then pass the result to `AVG`:

:::{include} ../examples/stats.csv-spec/docsStatsAvgNestedExpression.md
:::

Grouping expressions aren't limited to column references — any expression
works. For example, group by a derived value using `LEFT`:

:::{include} ../examples/stats.csv-spec/docsStatsByExpression.md
:::

### Output column naming

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
