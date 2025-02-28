## `STD_DEV` [esql-std_dev]

**Syntax**

:::{image} ../../../../../images/std_dev.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

true
**Description**

The standard deviation of a numeric field.

**Supported types**

| number | result |
| --- | --- |
| double | double |
| integer | double |
| long | double |

**Examples**

```esql
FROM employees
| STATS STD_DEV(height)
```

| STD_DEV(height):double |
| --- |
| 0.20637044362020449 |

The expression can use inline functions. For example, to calculate the standard deviation of each employee’s maximum salary changes, first use `MV_MAX` on each row, and then use `STD_DEV` on the result

```esql
FROM employees
| STATS stddev_salary_change = STD_DEV(MV_MAX(salary_change))
```

| stddev_salary_change:double |
| --- |
| 6.875829592924112 |


