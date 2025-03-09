## `TANH` [esql-tanh]

**Syntax**

:::{image} ../../../../../images/tanh.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Numeric expression. If `null`, the function returns `null`.

**Description**

Returns the [hyperbolic tangent](https://en.wikipedia.org/wiki/Hyperbolic_functions) of a number.

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
| EVAL tanh=TANH(a)
```

| a:double | tanh:double |
| --- | --- |
| 1.8 | 0.9468060128462683 |


