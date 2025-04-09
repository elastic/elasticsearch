```esql
FROM employees
| KEEP first_name, last_name, height
| SORT height
```

