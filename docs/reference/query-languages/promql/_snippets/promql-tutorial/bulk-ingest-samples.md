```console
POST /promql-tutorial/_bulk?refresh
{ "create": {} }
{"@timestamp":"2026-10-22T12:05:49Z","labels":{"__name__":"requests_total","job":"tutorial","instance":"us-east-1"},"metrics":{"requests_total":170}}
{ "create": {} }
{"@timestamp":"2026-10-22T12:05:49Z","labels":{"__name__":"requests_total","job":"tutorial","instance":"us-west-1"},"metrics":{"requests_total":80}}
{ "create": {} }
{"@timestamp":"2026-10-22T12:06:49Z","labels":{"__name__":"requests_total","job":"tutorial","instance":"us-east-1"},"metrics":{"requests_total":177}}
{ "create": {} }
{"@timestamp":"2026-10-22T12:04:49Z","labels":{"__name__":"requests_total","job":"tutorial","instance":"us-east-1"},"metrics":{"requests_total":163}}
{ "create": {} }
{"@timestamp":"2026-10-22T12:04:49Z","labels":{"__name__":"requests_total","job":"tutorial","instance":"us-west-1"},"metrics":{"requests_total":77}}
{ "create": {} }
{"@timestamp":"2026-10-22T12:06:49Z","labels":{"__name__":"requests_total","job":"tutorial","instance":"us-west-1"},"metrics":{"requests_total":83}}
```
