```esql
FROM employees
| KEEP first_name, last_name, height
| SORT first_name ASC NULLS FIRST
```

