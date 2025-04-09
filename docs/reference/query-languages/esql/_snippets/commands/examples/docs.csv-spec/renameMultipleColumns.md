```esql
FROM employees
| KEEP first_name, last_name
| RENAME first_name AS fn, last_name AS ln
```

