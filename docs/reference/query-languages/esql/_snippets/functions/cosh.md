## `COSH` [esql-cosh]

**Syntax**

:::{image} ../../../../../images/cosh.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Numeric expression. If `null`, the function returns `null`.

**Description**

Returns the [hyperbolic cosine](https://en.wikipedia.org/wiki/Hyperbolic_functions) of a number.

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
| EVAL cosh=COSH(a)
```

| a:double | cosh:double |
| --- | --- |
| 1.8 | 3.1074731763172667 |


