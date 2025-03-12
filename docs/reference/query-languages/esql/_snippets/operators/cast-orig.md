## `Cast (::)` [esql-cast-operator]

The `::` operator provides a convenient alternative syntax to the TO_<type> [conversion functions](../esql-functions-operators.md#esql-type-conversion-functions).

**Example**

```esql
ROW ver = CONCAT(("0"::INT + 1)::STRING, ".2.3")::VERSION
```

| ver:version |
| --- |
| 1.2.3 |
