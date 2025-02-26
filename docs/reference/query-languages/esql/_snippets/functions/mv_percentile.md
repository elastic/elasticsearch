## `MV_PERCENTILE` [esql-mv_percentile]

**Syntax**

:::{image} ../../../../../images/mv_percentile.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Multivalue expression.

`percentile`
:   The percentile to calculate. Must be a number between 0 and 100. Numbers out of range will return a null instead.

**Description**

Converts a multivalued field into a single valued field containing the value at which a certain percentage of observed values occur.

**Supported types**

| number | percentile | result |
| --- | --- | --- |
| double | double | double |
| double | integer | double |
| double | long | double |
| integer | double | integer |
| integer | integer | integer |
| integer | long | integer |
| long | double | long |
| long | integer | long |
| long | long | long |

**Example**

```esql
ROW values = [5, 5, 10, 12, 5000]
| EVAL p50 = MV_PERCENTILE(values, 50), median = MV_MEDIAN(values)
```

| values:integer | p50:integer | median:integer |
| --- | --- | --- |
| [5, 5, 10, 12, 5000] | 10 | 10 |


