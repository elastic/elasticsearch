```yaml {applies_to}
serverless: ga
stack: ga
```

The `LIMIT` processing command limits the number of rows returned.

## Syntax

```esql
LIMIT max_number_of_rows
```

## Parameters

`max_number_of_rows`
:   The maximum number of rows to return.

## Description

Use the `LIMIT` processing command to limit the number of rows returned.

:::{include} ../../common/result-set-size-limitation.md
:::

## Example

:::{include} ../examples/limit.csv-spec/basic.md
:::
