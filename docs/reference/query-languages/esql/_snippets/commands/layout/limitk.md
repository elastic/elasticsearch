```yaml {applies_to}
stack: preview 9.6+
serverless: preview
```

The `LIMITK` processing command keeps an arbitrary sample of rows, optionally per group.

## Syntax

```esql
LIMITK max_number_of_rows [BY grouping_expr1[, ..., grouping_exprN]]
```

## Parameters

`max_number_of_rows`
:   The maximum number of rows to return. When `BY` is specified, the maximum
    number of rows to return **per group**. Must be a non-negative integer.

`grouping_exprX`
:   An expression that outputs the values to group by.

## Description

Use the `LIMITK` processing command to keep an arbitrary sample of rows while
preserving full rows. Unlike [`TOPK`](/reference/query-languages/esql/commands/topk.md)
and [`BOTTOMK`](/reference/query-languages/esql/commands/bottomk.md), no ordering
is applied: which rows survive is not specified.

`LIMITK k [BY ...]` is equivalent to `LIMIT k [BY ...]` and shares its execution path.

## Examples

### Sample rows per group

:::{include} ../../generated/x-pack-esql/commands/examples/topk.csv-spec/limitkByGroup.md
:::
