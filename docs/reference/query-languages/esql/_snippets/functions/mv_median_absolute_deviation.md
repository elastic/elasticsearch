## `MV_MEDIAN_ABSOLUTE_DEVIATION` [esql-mv_median_absolute_deviation]

**Syntax**

:::{image} ../../../../../images/mv_median_absolute_deviation.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Multivalue expression.

**Description**

Converts a multivalued field into a single valued field containing the median absolute deviation.  It is calculated as the median of each data point’s deviation from the median of the entire sample. That is, for a random variable `X`, the median absolute deviation is `median(|median(X) - X|)`.

::::{note}
If the field has an even number of values, the medians will be calculated as the average of the middle two values. If the value is not a floating point number, the averages are rounded towards 0.
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
ROW values = [0, 2, 5, 6]
| EVAL median_absolute_deviation = MV_MEDIAN_ABSOLUTE_DEVIATION(values), median = MV_MEDIAN(values)
```

| values:integer | median_absolute_deviation:integer | median:integer |
| --- | --- | --- |
| [0, 2, 5, 6] | 2 | 3 |


