```yaml {applies_to}
serverless: preview
stack: preview 9.2.0
```

The `INLINE STATS` processing command groups rows according to a common value
and calculates one or more aggregated values over the grouped rows. The results
are appended as new columns to the input rows.

The command is identical to [`STATS`](/reference/query-languages/esql/commands/stats-by.md) except that it does not reduce
the number of columns in the output table.

**Syntax**

```esql
INLINE STATS [column1 =] expression1 [WHERE boolean_expression1][,
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

The `INLINE STATS` processing command groups rows according to a common value
(what comes after `BY`) and calculates one or more aggregated values over the
grouped rows. The output table contains the same number of rows as the input
table and the command just adds new columns or overrides any existent ones with
the same name to the result. The resulting calculated values are matched to the
input rows according to the common value(s) (also known as grouping key(s)).

In case there are overlapping column names between the newly added columns and the
existing ones, besides overriding the existing columns, there can be a change in
the column order. The new columns are added/moved so that they appear in the order
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

Calculating a statistic and grouping by the values of another column; note also
that `languages` column is moved as the last column in the output since it is
used as grouping key (the `KEEP` command before `INLINE STATS` had `languages`
set as the second column):

:::{include} ../examples/inlinestats.csv-spec/max-salary.md
:::

Omitting `BY` calculates the aggregation applied over the entire dataset, the
order of the existent columns is preserved and a new column with the calculated
maximum salary value is added as the last column:

:::{include} ../examples/inlinestats.csv-spec/max-salary-without-by.md
:::

It’s possible to calculate multiple values in more complex queries:

:::{include} ../examples/inlinestats.csv-spec/multi-agg-multi-grouping.md
:::

To filter the rows that go into an aggregation, use the `WHERE` clause:

:::{include} ../examples/inlinestats.csv-spec/avg-salaries-where.md
:::

Specifying the output column name is optional. If not specified, the new column
name is equal to the expression.

**Limitations**

- [`CATEGORIZE`](/reference/query-languages/esql/_snippets/functions/layout/categorize.md) grouping function is not
currently supported.
- `INLINE STATS` cannot yet have an unbounded [`SORT`](/reference/query-languages/esql/_snippets/commands/layout/sort.md) before it.
You must either move the SORT after it, or add a [`LIMIT`](/reference/query-languages/esql/_snippets/commands/layout/limit.md) before the [`SORT`](/reference/query-languages/esql/_snippets/commands/layout/sort.md).
