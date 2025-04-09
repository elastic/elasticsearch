```esql
FROM employees
| STATS avg_lang = AVG(languages), max_lang = MAX(languages)
```

| avg_lang:double | max_lang:integer |
| --- | --- |
| 3.1222222222222222 | 5 |
