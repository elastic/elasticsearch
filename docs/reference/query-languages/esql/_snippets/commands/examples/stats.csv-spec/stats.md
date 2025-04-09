```esql
FROM employees
| STATS count = COUNT(emp_no) BY languages
| SORT languages
```

| count:long | languages:integer |
| --- | --- |
| 15 | 1 |
| 19 | 2 |
| 17 | 3 |
| 18 | 4 |
| 21 | 5 |
| 10 | null |
