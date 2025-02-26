## `MV_AVG` [esql-mv_avg]

**Syntax**

:::{image} ../../../../../images/mv_avg.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Multivalue expression.

**Description**

Converts a multivalued field into a single valued field containing the average of all of the values.

**Supported types**

| number | result |
| --- | --- |
| double | double |
| integer | double |
| long | double |
| unsigned_long | double |

**Example**

```esql
ROW a=[3, 5, 1, 6]
| EVAL avg_a = MV_AVG(a)
```

| a:integer | avg_a:double |
| --- | --- |
| [3, 5, 1, 6] | 3.75 |


