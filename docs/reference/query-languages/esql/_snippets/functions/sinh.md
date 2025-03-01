## `SINH` [esql-sinh]

**Syntax**

:::{image} ../../../../../images/sinh.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Numeric expression. If `null`, the function returns `null`.

**Description**

Returns the [hyperbolic sine](https://en.wikipedia.org/wiki/Hyperbolic_functions) of a number.

**Supported types**

| number | result |
| --- | --- |
| double | double |
| integer | double |
| long | double |
| unsigned_long | double |

**Example**

```esql
ROW a=1.8
| EVAL sinh=SINH(a)
```

| a:double | sinh:double |
| --- | --- |
| 1.8 | 2.94217428809568 |


