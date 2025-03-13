## `EXP` [esql-exp]

**Syntax**

:::{image} ../../../../../images/exp.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Numeric expression. If `null`, the function returns `null`.

**Description**

Returns the value of e raised to the power of the given number.

**Supported types**

| number | result |
| --- | --- |
| double | double |
| integer | double |
| long | double |
| unsigned_long | double |

**Example**

```esql
ROW d = 5.0
| EVAL s = EXP(d)
```

| d: double | s:double |
| --- | --- |
| 5.0 | 148.413159102576603 |


