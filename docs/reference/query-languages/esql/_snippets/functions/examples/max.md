% This is generated by ESQL's AbstractFunctionTestCase. Do not edit it. See ../README.md for how to regenerate it.

**Examples**

```esql
FROM employees
| STATS MAX(languages)
```

| MAX(languages):integer |
| --- |
| 5 |

The expression can use inline functions. For example, to calculate the maximum over an average of a multivalued column, use `MV_AVG` to first average the multiple values per row, and use the result with the `MAX` function

```esql
FROM employees
| STATS max_avg_salary_change = MAX(MV_AVG(salary_change))
```

| max_avg_salary_change:double |
| --- |
| 13.75 |


