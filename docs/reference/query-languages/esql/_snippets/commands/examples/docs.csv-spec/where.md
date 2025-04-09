```esql
FROM employees
| KEEP first_name, last_name, still_hired
| WHERE still_hired == true
```

