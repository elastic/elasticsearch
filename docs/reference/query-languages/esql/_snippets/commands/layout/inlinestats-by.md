```yaml {applies_to}
serverless: ga
stack: preview 9.2.0, ga 9.3.0
```

The `INLINE STATS` processing command groups rows according to a common value
and calculates one or more aggregated values over the grouped rows. The results
are appended as new columns to the input rows.

The command is identical to [`STATS`](/reference/query-languages/esql/commands/stats-by.md) except that it preserves all the columns from the input table.

**Syntax**

```esql
INLINE STATS [column1 =] expression1 [WHERE boolean_expression1][,
      ...,
      [columnN =] expressionN [WHERE boolean_expressionN]]
      [BY [grouping_name1 =] grouping_expression1[,
          ...,
          [grouping_nameN = ] grouping_expressionN]]
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
    If its name coincides with one of the existing or computed columns, that column will be overridden by this one.

`boolean_expressionX`
:   The condition that determines which rows are included when evaluating `expressionX`.

::::{note}
Individual `null` values are skipped when computing aggregations.
::::


**Description**

The `INLINE STATS` processing command groups rows according to a common value
(also known as the grouping key), specified after `BY`, and calculates one or more
aggregated values over the grouped rows. The output table contains the same
number of rows as the input table. The command only adds new columns or overrides
existing columns with the same name as the result.

If column names overlap, existing column values may be overridden and column order
may change. The new columns are added/moved so that they appear in the order
they are defined in the `INLINE STATS` command.

For the calculation of each aggregated value, the rows in a group can be filtered with
`WHERE`. If `BY` is omitted the aggregations are applied over the entire dataset.

The following [aggregation functions](/reference/query-languages/esql/functions-operators/aggregation-functions.md) are supported:

:::{include} ../../lists/aggregation-functions.md
:::

The following [grouping functions](/reference/query-languages/esql/functions-operators/grouping-functions.md) are supported:

* [`BUCKET`](/reference/query-languages/esql/functions-operators/grouping-functions.md#esql-bucket)
* [`TBUCKET`](/reference/query-languages/esql/functions-operators/grouping-functions.md#esql-tbucket)


**Examples**

The following example shows how to calculate a statistic on one column and group
by the values of another column.

:::{note}
The `languages` column moves to the last position in the output table because it is
a column overridden by the `INLINE STATS` command (it's the grouping key) and it is the last column defined by it.
:::

:::{include} ../examples/inlinestats.csv-spec/max-salary.md
:::

The following example shows how to calculate an aggregation over the entire dataset
by omitting `BY`. The order of the existing columns is preserved and a new column
with the calculated maximum salary value is added as the last column:

:::{include} ../examples/inlinestats.csv-spec/max-salary-without-by.md
:::

The following example shows how to calculate multiple aggregations with multiple grouping keys:

:::{include} ../examples/inlinestats.csv-spec/multi-agg-multi-grouping.md
:::

The following example shows how to filter which rows are used for each aggregation, using the `WHERE` clause:

:::{include} ../examples/inlinestats.csv-spec/avg-salaries-where.md
:::


**Limitations**

- The [`CATEGORIZE`](/reference/query-languages/esql/functions-operators/grouping-functions.md#esql-categorize) grouping function is not currently supported.
- You cannot currently use [`LIMIT`](/reference/query-languages/esql/commands/limit.md) (explicit or implicit) before `INLINE STATS`, because this can lead to unexpected results.
- You cannot currently use [`FORK`](/reference/query-languages/esql/commands/fork.md) before `INLINE STATS`, because `FORK` adds an implicit [`LIMIT`](/reference/query-languages/esql/commands/limit.md) to each branch, which can lead to unexpected results.
