```esql
FROM employees
| KEEP first_name, last_name, still_hired
| RENAME  still_hired AS employed
```

