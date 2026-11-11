```console
POST /_query?format=txt
{
  "query": """
    PROMQL index=promql-tutorial start=1792670119 end=1792670839 step=60s
      http_rate=(sum(rate(requests_total[5m])))
    | SORT step DESC
    """
}
```
