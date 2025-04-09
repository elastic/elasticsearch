```esql
FROM employees
| STATS AVG(salary)
```

| AVG(salary):double |
| --- |
| 48248.55 |
