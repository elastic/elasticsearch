## `ATAN2` [esql-atan2]

**Syntax**

:::{image} ../../../../../images/atan2.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`y_coordinate`
:   y coordinate. If `null`, the function returns `null`.

`x_coordinate`
:   x coordinate. If `null`, the function returns `null`.

**Description**

The [angle](https://en.wikipedia.org/wiki/Atan2) between the positive x-axis and the ray from the origin to the point (x , y) in the Cartesian plane, expressed in radians.

**Supported types**

| y_coordinate | x_coordinate | result |
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

**Example**

```esql
ROW y=12.9, x=.6
| EVAL atan2=ATAN2(y, x)
```

| y:double | x:double | atan2:double |
| --- | --- | --- |
| 12.9 | 0.6 | 1.5243181954438936 |


