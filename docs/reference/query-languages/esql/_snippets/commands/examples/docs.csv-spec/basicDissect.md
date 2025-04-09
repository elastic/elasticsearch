```esql
ROW a = "2023-01-23T12:15:00.000Z - some text - 127.0.0.1"
| DISSECT a """%{date} - %{msg} - %{ip}"""
| KEEP date, msg, ip
```

| date:keyword | msg:keyword | ip:keyword |
| --- | --- | --- |
| 2023-01-23T12:15:00.000Z | some text | 127.0.0.1 |
