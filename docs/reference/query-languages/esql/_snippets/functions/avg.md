## `AVG` [esql-avg]

**Syntax**

:::{image} ../../../../../images/avg.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

true
**Description**

The average of a numeric field.

**Supported types**

| number | result |
| --- | --- |
| double | double |
| integer | double |
| long | double |

**Examples**

```esql
FROM employees
| STATS AVG(height)
```

| AVG(height):double |
| --- |
| 1.7682 |

The expression can use inline functions. For example, to calculate the average over a multivalued column, first use `MV_AVG` to average the multiple values per row, and use the result with the `AVG` function

```esql
FROM employees
| STATS avg_salary_change = ROUND(AVG(MV_AVG(salary_change)), 10)
```

| avg_salary_change:double |
| --- |
| 1.3904535865 |


