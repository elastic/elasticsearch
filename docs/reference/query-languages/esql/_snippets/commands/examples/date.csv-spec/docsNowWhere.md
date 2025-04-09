```esql
FROM sample_data
| WHERE @timestamp > NOW() - 1 hour
```

