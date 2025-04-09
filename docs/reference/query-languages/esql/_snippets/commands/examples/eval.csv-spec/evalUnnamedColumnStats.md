```esql
FROM employees
| EVAL height * 3.281
| STATS avg_height_feet = AVG(`height * 3.281`)
```

| avg_height_feet:double |
| --- |
| 5.801464200000001 |
