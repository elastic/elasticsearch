## `POW` [esql-pow]

**Syntax**

:::{image} ../../../../../images/pow.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`base`
:   Numeric expression for the base. If `null`, the function returns `null`.

`exponent`
:   Numeric expression for the exponent. If `null`, the function returns `null`.

**Description**

Returns the value of `base` raised to the power of `exponent`.

::::{note}
It is still possible to overflow a double result here; in that case, null will be returned.
::::


**Supported types**

| base | exponent | result |
| --- | --- | --- |
| double | double | double |
| double | integer | double |
| double | long | double |
| double | unsigned_long | double |
| integer | double | double |
| integer | integer | double |
| integer | long | double |
| integer | unsigned_long | double |
| long | double | double |
| long | integer | double |
| long | long | double |
| long | unsigned_long | double |
| unsigned_long | double | double |
| unsigned_long | integer | double |
| unsigned_long | long | double |
| unsigned_long | unsigned_long | double |

**Examples**

```esql
ROW base = 2.0, exponent = 2
| EVAL result = POW(base, exponent)
```

| base:double | exponent:integer | result:double |
| --- | --- | --- |
| 2.0 | 2 | 4.0 |

The exponent can be a fraction, which is similar to performing a root. For example, the exponent of `0.5` will give the square root of the base:

```esql
ROW base = 4, exponent = 0.5
| EVAL s = POW(base, exponent)
```

| base:integer | exponent:double | s:double |
| --- | --- | --- |
| 4 | 0.5 | 2.0 |


