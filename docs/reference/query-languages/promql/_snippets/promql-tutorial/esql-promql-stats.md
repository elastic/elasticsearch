```console
POST /_query
{
  "query": """
    PROMQL index=promql-tutorial start=1792670119 end=1792670839 step=60s
      http_rate=(sum(rate(requests_total[5m])))
    | WHERE http_rate > 0.07
    | STATS avg_rate = AVG(http_rate), peak_rate = MAX(http_rate)
  """
}
```
