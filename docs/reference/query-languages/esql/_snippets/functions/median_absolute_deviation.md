## `MEDIAN_ABSOLUTE_DEVIATION` [esql-median_absolute_deviation]

**Syntax**

:::{image} ../../../../../images/median_absolute_deviation.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

true
**Description**

Returns the median absolute deviation, a measure of variability. It is a robust statistic, meaning that it is useful for describing data that may have outliers, or may not be normally distributed. For such data it can be more descriptive than standard deviation.  It is calculated as the median of each data point’s deviation from the median of the entire sample. That is, for a random variable `X`, the median absolute deviation is `median(|median(X) - X|)`.

::::{note}
Like [`PERCENTILE`](../../esql-functions-operators.md#esql-percentile), `MEDIAN_ABSOLUTE_DEVIATION` is [usually approximate](../../esql-functions-operators.md#esql-percentile-approximate).
::::


**Supported types**

| number | result |
| --- | --- |
| double | double |
| integer | double |
| long | double |

**Examples**

```esql
FROM employees
| STATS MEDIAN(salary), MEDIAN_ABSOLUTE_DEVIATION(salary)
```

| MEDIAN(salary):double | MEDIAN_ABSOLUTE_DEVIATION(salary):double |
| --- | --- |
| 47003 | 10096.5 |

The expression can use inline functions. For example, to calculate the median absolute deviation of the maximum values of a multivalued column, first use `MV_MAX` to get the maximum value per row, and use the result with the `MEDIAN_ABSOLUTE_DEVIATION` function

```esql
FROM employees
| STATS m_a_d_max_salary_change = MEDIAN_ABSOLUTE_DEVIATION(MV_MAX(salary_change))
```

| m_a_d_max_salary_change:double |
| --- |
| 5.69 |

::::{warning}
`MEDIAN_ABSOLUTE_DEVIATION` is also [non-deterministic](https://en.wikipedia.org/wiki/Nondeterministic_algorithm). This means you can get slightly different results using the same data.

::::



