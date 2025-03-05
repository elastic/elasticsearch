## `MV_SUM` [esql-mv_sum]

**Syntax**

:::{image} ../../../../../images/mv_sum.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Multivalue expression.

**Description**

Converts a multivalued field into a single valued field containing the sum of all of the values.

**Supported types**

| number | result |
| --- | --- |
| double | double |
| integer | integer |
| long | long |
| unsigned_long | unsigned_long |

**Example**

```esql
ROW a=[3, 5, 6]
| EVAL sum_a = MV_SUM(a)
```

| a:integer | sum_a:integer |
| --- | --- |
| [3, 5, 6] | 14 |


