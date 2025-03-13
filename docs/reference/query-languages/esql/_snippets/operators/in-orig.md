## `IN` [esql-in-operator]

The `IN` operator allows testing whether a field or expression equals an element in a list of literals, fields or expressions.

**Example**

```esql
ROW a = 1, b = 4, c = 3
| WHERE c-a IN (3, b / 2, a)
```

| a:integer | b:integer | c:integer |
| --- | --- | --- |
| 1 | 4 | 3 |
