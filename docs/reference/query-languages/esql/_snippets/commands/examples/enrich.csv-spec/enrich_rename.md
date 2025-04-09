```esql
ROW a = "1"
| ENRICH languages_policy ON a WITH name = language_name
```

| a:keyword | name:keyword |
| --- | --- |
| 1 | English |
