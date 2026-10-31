% Section of prometheus-compatibility.md, also included in other pages. Rows are numbered across all sections.

These differences apply to the [Prometheus-compatible HTTP API](/reference/query-languages/promql/promql-http-api.md) only.

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 65 | $$$prometheus-compatibility-form-post$$$ Form-encoded POST: parameters in an `application/x-www-form-urlencoded` body | {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` Accepted only when [security](/reference/elasticsearch/configuration-reference/security-settings.md) is enabled, [`xpack.security.http.ssl.enabled`](/reference/elasticsearch/configuration-reference/security-settings.md) is `true` on the {{es}} HTTP interface, and the request is authenticated. TLS that terminates before {{es}} doesn't count. Otherwise `406 Not Acceptable`; use `GET` with query-string parameters instead | Accepted | - |
| 66 | $$$prometheus-compatibility-unsupported-query-params$$$ Query parameters that the [PromQL HTTP API](/reference/query-languages/promql/promql-http-api.md) doesn't document, such as `lookback_delta` and `stats` | `400 Bad Request`. Configure clients to omit them | Accepted | [#149939](https://github.com/elastic/elasticsearch/issues/149939) |
| 67 | `warnings` and `infos` annotations, for example for a quantile outside the range 0 to 1 | Not returned. The only warning is `results truncated due to limit` | Returned | - |
| 68 | $$$prometheus-compatibility-metadata-help$$$ Metric metadata `help` on [`/api/v1/metadata`](/reference/query-languages/promql/promql-http-api.md#promql-http-api-metadata-endpoint) | {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` Always an empty string | The `HELP` text of the metric | [#157230](https://github.com/elastic/elasticsearch/issues/157230) |
| 69 | $$$prometheus-compatibility-exemplars$$$ Exemplar queries: `/api/v1/query_exemplars` | Not implemented. Turn off exemplar queries in the client; in Grafana, go to **Data sources** > **Elasticsearch** > **Exemplars** and disable all exemplar links | Returns exemplars | [#154786](https://github.com/elastic/elasticsearch/issues/154786) |
