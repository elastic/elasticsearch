**Example**

```esql
ROW ver = CONCAT(("0"::INT + 1)::STRING, ".2.3")::VERSION
```

| ver:version |
| --- |
| 1.2.3 |
