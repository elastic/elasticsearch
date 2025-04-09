```esql
FROM employees
| KEEP first_name, last_name, height
| WHERE LENGTH(first_name) < 4
```

