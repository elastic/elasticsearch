```esql
FROM employees
| STATS AVG(salary)
| EVAL avg_salary_rounded = ROUND(`AVG(salary)`)
```

| AVG(salary):double | avg_salary_rounded:double |
| --- | --- |
| 48248.55 | 48249.0 |
