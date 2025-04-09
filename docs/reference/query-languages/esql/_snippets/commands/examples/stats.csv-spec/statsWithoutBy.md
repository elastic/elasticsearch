```esql
FROM employees
| STATS avg_lang = AVG(languages)
```

| avg_lang:double |
| --- |
| 3.1222222222222222 |
