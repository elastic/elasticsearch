## `TO_DEGREES` [esql-to_degrees]

**Syntax**

:::{image} ../../../../../images/to_degrees.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Input value. The input can be a single- or multi-valued column or an expression.

**Description**

Converts a number in [radians](https://en.wikipedia.org/wiki/Radian) to [degrees](https://en.wikipedia.org/wiki/Degree_(angle)).

**Supported types**

| number | result |
| --- | --- |
| double | double |
| integer | double |
| long | double |
| unsigned_long | double |

**Example**

```esql
ROW rad = [1.57, 3.14, 4.71]
| EVAL deg = TO_DEGREES(rad)
```

| rad:double | deg:double |
| --- | --- |
| [1.57, 3.14, 4.71] | [89.95437383553924, 179.9087476710785, 269.86312150661774] |


