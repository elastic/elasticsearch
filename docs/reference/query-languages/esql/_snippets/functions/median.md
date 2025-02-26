## `MEDIAN` [esql-median]

**Syntax**

:::{image} ../../../../../images/median.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

true
**Description**

The value that is greater than half of all values and less than half of all values, also known as the 50% [`PERCENTILE`](../../esql-functions-operators.md#esql-percentile).

::::{note}
Like [`PERCENTILE`](../../esql-functions-operators.md#esql-percentile), `MEDIAN` is [usually approximate](../../esql-functions-operators.md#esql-percentile-approximate).
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
| STATS MEDIAN(salary), PERCENTILE(salary, 50)
```

| MEDIAN(salary):double | PERCENTILE(salary, 50):double |
| --- | --- |
| 47003 | 47003 |

The expression can use inline functions. For example, to calculate the median of the maximum values of a multivalued column, first use `MV_MAX` to get the maximum value per row, and use the result with the `MEDIAN` function

```esql
FROM employees
| STATS median_max_salary_change = MEDIAN(MV_MAX(salary_change))
```

| median_max_salary_change:double |
| --- |
| 7.69 |

::::{warning}
`MEDIAN` is also [non-deterministic](https://en.wikipedia.org/wiki/Nondeterministic_algorithm). This means you can get slightly different results using the same data.

::::



