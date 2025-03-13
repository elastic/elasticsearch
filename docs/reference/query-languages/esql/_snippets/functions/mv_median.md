## `MV_MEDIAN` [esql-mv_median]

**Syntax**

:::{image} ../../../../../images/mv_median.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Multivalue expression.

**Description**

Converts a multivalued field into a single valued field containing the median value.

**Supported types**

| number | result |
| --- | --- |
| double | double |
| integer | integer |
| long | long |
| unsigned_long | unsigned_long |

**Examples**

```esql
ROW a=[3, 5, 1]
| EVAL median_a = MV_MEDIAN(a)
```

| a:integer | median_a:integer |
| --- | --- |
| [3, 5, 1] | 3 |

If the row has an even number of values for a column, the result will be the average of the middle two entries. If the column is not floating point, the average rounds **down**:

```esql
ROW a=[3, 7, 1, 6]
| EVAL median_a = MV_MEDIAN(a)
```

| a:integer | median_a:integer |
| --- | --- |
| [3, 7, 1, 6] | 4 |


