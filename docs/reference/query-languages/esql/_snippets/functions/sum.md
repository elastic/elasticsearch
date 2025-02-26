## `SUM` [esql-sum]

**Syntax**

:::{image} ../../../../../images/sum.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

true
**Description**

The sum of a numeric expression.

**Supported types**

| number | result |
| --- | --- |
| double | double |
| integer | long |
| long | long |

**Examples**

```esql
FROM employees
| STATS SUM(languages)
```

| SUM(languages):long |
| --- |
| 281 |

The expression can use inline functions. For example, to calculate the sum of each employee’s maximum salary changes, apply the `MV_MAX` function to each row and then sum the results

```esql
FROM employees
| STATS total_salary_changes = SUM(MV_MAX(salary_change))
```

| total_salary_changes:double |
| --- |
| 446.75 |


