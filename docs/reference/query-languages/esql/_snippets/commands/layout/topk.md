```yaml {applies_to}
stack: preview 9.6+
serverless: preview
```

The `TOPK` processing command keeps the rows with the highest values of an expression, optionally per group.

## Syntax

```esql
TOPK sort_expression, max_number_of_rows [BY grouping_expr1[, ..., grouping_exprN]]
```

## Parameters

`sort_expression`
:   The expression whose highest values select the surviving rows. Can be a column name,
    a [function](/reference/query-languages/esql/esql-functions-operators.md#esql-functions), or an
    arithmetic expression, like in [`SORT`](/reference/query-languages/esql/commands/sort.md).
    `null` values sort first, as in a descending `SORT`.

`max_number_of_rows`
:   The maximum number of rows to return. When `BY` is specified, the maximum
    number of rows to return **per group**. Must be a non-negative integer.

`grouping_exprX`
:   An expression that outputs the values to group by.

## Description

Use the `TOPK` processing command to keep the top rows by value while preserving
full rows, unlike the [`TOP`](/reference/query-languages/esql/functions-operators/aggregation-functions.md#esql-top)
aggregation function which collects values into a multivalue array.

`TOPK sort_expression, k [BY ...]` is equivalent to
`SORT sort_expression DESC | LIMIT k [BY ...]` and shares its execution path.

See also [`BOTTOMK`](/reference/query-languages/esql/commands/bottomk.md) for the
lowest values and [`LIMITK`](/reference/query-languages/esql/commands/limitk.md)
for an arbitrary sample.

## Examples

### Top row per group

:::{include} ../../generated/x-pack-esql/commands/examples/topk.csv-spec/topkByGroup.md
:::
