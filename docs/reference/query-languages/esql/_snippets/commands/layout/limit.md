## `LIMIT` [esql-limit]

The `LIMIT` processing command enables you to limit the number of rows that are
returned.

**Syntax**

```esql
LIMIT max_number_of_rows
```

**Parameters**

`max_number_of_rows`
:   The maximum number of rows to return.

**Description**

The `LIMIT` processing command enables you to limit the number of rows that are
returned.
:::{include} ../../common/result-set-size-limitation.md
:::

**Example**

:::{include} ../examples/limit.csv-spec/basic.md
:::
