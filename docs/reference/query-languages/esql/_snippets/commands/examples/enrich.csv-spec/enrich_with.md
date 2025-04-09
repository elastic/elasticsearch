```esql
ROW a = "1"
| ENRICH languages_policy ON a WITH language_name
```

| a:keyword | language_name:keyword |
| --- | --- |
| 1 | English |
