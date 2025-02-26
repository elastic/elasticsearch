## `FLOOR` [esql-floor]

**Syntax**

:::{image} ../../../../../images/floor.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Numeric expression. If `null`, the function returns `null`.

**Description**

Round a number down to the nearest integer.

::::{note}
This is a noop for `long` (including unsigned) and `integer`. For `double` this picks the closest `double` value to the integer similar to [Math.floor](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Math.md#floor(double)).
::::


**Supported types**

| number | result |
| --- | --- |
| double | double |
| integer | integer |
| long | long |
| unsigned_long | unsigned_long |

**Example**

```esql
ROW a=1.8
| EVAL a=FLOOR(a)
```

| a:double |
| --- |
| 1 |


