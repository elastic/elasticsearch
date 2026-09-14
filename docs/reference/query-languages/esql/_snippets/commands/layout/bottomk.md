```yaml {applies_to}
stack: preview 9.6+
serverless: preview
```

The `BOTTOMK` processing command keeps the rows with the lowest values of an expression, optionally per group.

## Syntax

```esql
BOTTOMK sort_expression, max_number_of_rows [BY grouping_expr1[, ..., grouping_exprN]]
```

## Parameters

`sort_expression`
:   The expression whose lowest values select the surviving rows. Can be a column name,
    a [function](/reference/query-languages/esql/esql-functions-operators.md#esql-functions), or an
    arithmetic expression, like in [`SORT`](/reference/query-languages/esql/commands/sort.md).
    `null` values sort last, as in an ascending `SORT`.

`max_number_of_rows`
:   The maximum number of rows to return. When `BY` is specified, the maximum
    number of rows to return **per group**. Must be a non-negative integer.

`grouping_exprX`
:   An expression that outputs the values to group by.

## Description

Use the `BOTTOMK` processing command to keep the bottom rows by value while preserving
full rows.

`BOTTOMK sort_expression, k [BY ...]` is equivalent to
`SORT sort_expression ASC | LIMIT k [BY ...]` and shares its execution path.

See also [`TOPK`](/reference/query-languages/esql/commands/topk.md) for the
highest values and [`LIMITK`](/reference/query-languages/esql/commands/limitk.md)
for an arbitrary sample.

## Examples

### Bottom row per group

:::{include} ../../generated/x-pack-esql/commands/examples/topk.csv-spec/bottomkByGroup.md
:::
