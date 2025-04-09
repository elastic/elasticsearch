```esql
ROW i=1, a=["a", "b"] | STATS MIN(i) BY a | SORT a ASC
```

| MIN(i):integer | a:keyword |
| --- | --- |
| 1 | a |
| 1 | b |
