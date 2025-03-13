## `SIGNUM` [esql-signum]

**Syntax**

:::{image} ../../../../../images/signum.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Numeric expression. If `null`, the function returns `null`.

**Description**

Returns the sign of the given number. It returns `-1` for negative numbers, `0` for `0` and `1` for positive numbers.

**Supported types**

| number | result |
| --- | --- |
| double | double |
| integer | double |
| long | double |
| unsigned_long | double |

**Example**

```esql
ROW d = 100.0
| EVAL s = SIGNUM(d)
```

| d: double | s:double |
| --- | --- |
| 100 | 1.0 |


