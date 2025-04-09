```esql
FROM employees
| SORT emp_no
| KEEP first_name, last_name, height
| EVAL height = height * 3.281
```

| first_name:keyword | last_name:keyword | height:double |
| --- | --- | --- |
| Georgi | Facello | 6.66043 |
| Bezalel | Simmel | 6.82448 |
| Parto | Bamford | 6.004230000000001 |
