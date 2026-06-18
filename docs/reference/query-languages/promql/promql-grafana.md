---
description: Use Elasticsearch as a Prometheus data source in Grafana to query metrics with PromQL, power autocomplete and Metrics Drilldown, and reuse existing dashboards.
navigation_title: Prometheus data source in Grafana
applies_to:
  stack: preview 9.4, ga 9.5
  serverless: ga
products:
  - id: elasticsearch
---

# Use {{es}} as a Prometheus data source in Grafana [promql-grafana]

{{es}} exposes a [Prometheus-compatible HTTP API](promql-http-api.md) under the `/_prometheus/` prefix.
Because the API follows the [Prometheus query API](https://prometheus.io/docs/prometheus/latest/querying/api/), you can point Grafana's built-in **Prometheus** data source directly at {{es}} without sidecars, adapters, or pipeline changes.

Grafana then treats {{es}} like any other Prometheus backend: dashboard panels, query autocompletion, template variables, and [Metrics Drilldown](https://grafana.com/docs/grafana/latest/visualizations/simplified-exploration/metrics/) all use PromQL against metrics stored in {{es}} [time series data streams](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md) (TSDS).

This guide covers connecting Grafana to {{es}}. To ingest metrics from Prometheus, see [Prometheus remote write](docs-content://manage-data/data-store/data-streams/tsds-ingest-prometheus-remote-write.md).

::::{important}
Before you build dashboards, review the [PromQL limitations](promql-limitations.md).
Behavior differs from upstream Prometheus in a few areas, and some PromQL constructs are not evaluated yet.
Knowing these up front helps you tell an expected limitation apart from a real problem when [troubleshooting](#promql-grafana-troubleshooting).
::::

## Before you begin [promql-grafana-prerequisites]

You need:

- The {{es}} endpoint for your deployment.
  On {{es-serverless}}, find it in the Elastic Cloud console under **Manage > Application endpoints, cluster and component IDs**, next to **Elasticsearch**.
  The endpoint looks like `https://<project-id>.es.<region>.<provider>.elastic.cloud`.
- An [API key](docs-content://deploy-manage/api-keys/elasticsearch-api-keys.md) for Grafana to query metrics.

Create an API key scoped to read metrics data streams only, so a leaked Grafana key cannot be used to write data.
In **Control security privileges**, use a role descriptor such as:

```json
{
  "query": {
    "indices": [
      {
        "names": ["metrics-*.prometheus-*"],
        "privileges": ["read", "view_index_metadata"]
      }
    ]
  }
}
```

The `metrics-*.prometheus-*` pattern scopes the key to metrics ingested through [Prometheus remote write](docs-content://manage-data/data-store/data-streams/tsds-ingest-prometheus-remote-write.md), which is what this guide focuses on.
Because the key can read only those data streams, Grafana queries stay limited to that Prometheus data even when they use the default `/_prometheus/` endpoint.

When the key is created, copy the **Encoded** value (select the **Encoded** format if Kibana offers a format dropdown). This is the value you pass directly as `ApiKey <encoded>`; you cannot retrieve it again after closing the dialog.

::::{tip}
You can configure a separate Grafana data source per index pattern to give teams scoped access to their own metrics.
Add an index expression after `/_prometheus/`, for example `https://<es_endpoint>/_prometheus/metrics-prod-*`.
See [Index scoping](promql-http-api.md#promql-http-api-index-scope).
::::

## Add the data source [promql-grafana-datasource]

Configure the data source through the Grafana UI or through [provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/#data-sources).

### Add the data source in the UI [promql-grafana-datasource-ui]

1. In Grafana, go to **Connections > Data sources > Add data source**, then choose **Prometheus**.
2. Set the following:
    - **Name**: `Elasticsearch`
    - **Prometheus server URL**: `https://<es_endpoint>/_prometheus`
    - Under **Custom HTTP Headers**, add a header:
        - **Header**: `Authorization`
        - **Value**: `ApiKey <query_api_key>`
3. Click **Save & test**.

Grafana confirms the connection, and autocompletion starts working in the query editor.
The data source has more options than the ones above; for the authoritative steps and the full option reference, see the [Grafana Prometheus data source documentation](https://grafana.com/docs/grafana/latest/datasources/prometheus/configure/).

::::{note}
Grafana sends Prometheus queries with `POST` by default, which {{es}} accepts on authenticated HTTPS endpoints such as {{es-serverless}}.
If TLS terminates before {{es}} and the node receives plain HTTP, set the data source **HTTP method** to `GET` instead.
See [Form-encoded POST requests](promql-limitations.md#promql-limitations-form-post).
::::

### Add the data source with provisioning [promql-grafana-datasource-provisioning]

To provision the data source from a file, create `provisioning/datasources/datasource.yml`, replacing `<es_endpoint>` and `<query_api_key>`:

```yaml
apiVersion: 1

datasources:
  - name: Elasticsearch
    type: prometheus
    access: proxy
    url: "https://<es_endpoint>/_prometheus"
    uid: elasticsearch-prometheus
    isDefault: true
    jsonData:
      httpHeaderName1: Authorization
    secureJsonData:
      httpHeaderValue1: "ApiKey <query_api_key>"
```

Mount the `provisioning` directory into Grafana at `/etc/grafana/provisioning`.

## Use dashboards [promql-grafana-dashboard]

Existing Prometheus dashboards work against {{es}} without changes, as long as the PromQL they use is supported (see [Limitations](promql-limitations.md)).
You can point an existing dashboard at {{es}} or build a new one on top of the data source.

### Point an existing dashboard at {{es}} [promql-grafana-dashboard-switch]

If a dashboard already runs against another Prometheus-compatible backend, switch its data source to {{es}} using the dashboard's [data source variable](https://grafana.com/docs/grafana/latest/dashboards/variables/add-template-variables/) dropdown (for dashboards that use one) or by editing the panels or the dashboard JSON model (for panels that reference a data source directly).

### Build a new dashboard [promql-grafana-dashboard-new]

1. Go to **Dashboards > New > New dashboard > Add visualization**.
2. Select the **Elasticsearch** data source.
3. In the query editor, write a PromQL query (for example, `sum(up)`). Autocompletion and the metric browser are backed by the {{es}} [metadata and discovery endpoints](promql-http-api.md#promql-http-api-metadata).
4. Choose a visualization, then save the dashboard.

## Troubleshooting [promql-grafana-troubleshooting]

When a panel shows an error, returns no data, or renders something unexpected, the cause is in one of two layers:

- The {{es}} Prometheus API (owned by Elastic).
- The Grafana **Prometheus** data source, which translates Grafana's requests into Prometheus API calls (owned by Grafana, not Elastic).

### Step 1: Read the panel error [promql-grafana-troubleshooting-read-error]

Grafana shows the data source's error message directly on the panel: hover or click the warning indicator in the panel's top-left corner.
For most errors {{es}} returns a descriptive message, and you can act on it without reproducing anything:

- A message that a PromQL construct or query parameter is **not supported yet** points to a [documented limitation](promql-limitations.md). Adjust the query, or wait for the feature. No issue needed.
- An authentication or authorization message (`401`/`403`) points to the API key. See [Common symptoms](#promql-grafana-troubleshooting-symptoms).
- A `406 Not Acceptable` on every query points to the form-encoded `POST` requirement. See [Common symptoms](#promql-grafana-troubleshooting-symptoms).

If the message is clear and matches a documented limitation, you're done.
If it is unclear, the panel returns wrong or empty data with no error, or you suspect the data source layer, continue to Step 2.

### Step 2: Inspect the request and response [promql-grafana-troubleshooting-inspect]

If the panel error is unclear, or the panel returns wrong or empty data with no error, open the query inspector (panel menu > **Inspect > Query**) and run the query.
The inspector shows both the request Grafana sent (endpoint, `query`, `start`, `end`, `step`) and the raw response from {{es}}, which usually points straight at the cause.

To confirm {{es}}'s behavior independently of Grafana, replay the same request directly against the [HTTP API](promql-http-api.md).
Replace `<es_endpoint>` and `<query_api_key>` with your values:

```bash
curl "https://<es_endpoint>/_prometheus/api/v1/query_range" \
  -H "Authorization: ApiKey <query_api_key>" \
  --data-urlencode 'query=up' \
  --data-urlencode 'start=2026-06-18T09:00:00Z' \
  --data-urlencode 'end=2026-06-18T10:00:00Z' \
  --data-urlencode 'step=15s'
```

For metric discovery, autocompletion, or template-variable problems, check the relevant [metadata endpoint](promql-http-api.md#promql-http-api-metadata) instead (for example `/_prometheus/api/v1/labels` or `/_prometheus/api/v1/series`).

### Step 3: Decide where the issue belongs [promql-grafana-troubleshooting-decide]

- **{{es}} returns the expected data (in the inspector response or via curl), but the panel does not show it.**
  The cause is in Grafana, not {{es}}, and is almost always configuration or usage rather than a bug: panel and visualization settings, template variables, the dashboard time range, or transformations.
  Review the panel and dashboard configuration. For help, use the [Grafana documentation](https://grafana.com/docs/grafana/latest/) and [community forums](https://community.grafana.com/). Only [open a Grafana issue](https://github.com/grafana/grafana/issues) if you can confirm an actual bug in the Prometheus data source.
- **{{es}} returns an error (`"status": "error"`) or wrong data.**
  Before assuming an {{es}} problem, rule out a genuinely invalid query, which would fail against upstream Prometheus too.
  Confirm the expression is valid PromQL, for example with [PromLens](https://demo.promlens.com/).
  Then check whether the behavior is an expected difference:
    - The expression might use a [construct that {{es}} does not evaluate yet](promql-limitations.md#promql-limitations-unsupported-constructs), such as set operators or group modifiers. These return a `4xx` with `errorType: bad_data`.
    - The request might include a [query parameter {{es}} does not support yet](promql-limitations.md#promql-limitations-unsupported-query-params), which also fails with `4xx`.
    - [Instant queries](promql-limitations.md#promql-limitations-instant-query) and [staleness handling](promql-limitations.md#promql-limitations-staleness) differ from upstream Prometheus.

    If the query is valid and the behavior is not explained by a documented limitation, [open an {{es}} issue](https://github.com/elastic/elasticsearch/issues) with the request and the full response body.

### Common symptoms [promql-grafana-troubleshooting-symptoms]

| Symptom | Likely cause | What to check |
| --- | --- | --- |
| `406 Not Acceptable` on every query | Form-encoded `POST` is not accepted by this deployment | Set the data source **HTTP method** to `GET`, or ensure TLS terminates at {{es}}. See [Form-encoded POST requests](promql-limitations.md#promql-limitations-form-post) |
| `401`/`403` errors | API key missing, invalid, or lacking privileges | Verify the `Authorization: ApiKey <key>` header and that the key grants `read` and `view_index_metadata` on `metrics-*.prometheus-*` |
| Empty results, no error | No matching data in the time range or index scope | Confirm metrics exist in the queried [index scope](promql-http-api.md#promql-http-api-index-scope) and time window by reproducing the request directly |
| `bad_data` error on a specific expression | Unsupported PromQL construct | Compare the expression against [unsupported constructs](promql-limitations.md#promql-limitations-unsupported-constructs) |

## Further reading

- [HTTP API](promql-http-api.md): the Prometheus-compatible endpoints Grafana calls.
- [Limitations](promql-limitations.md): unsupported PromQL constructs and HTTP behavior.
- [Prometheus remote write](docs-content://manage-data/data-store/data-streams/tsds-ingest-prometheus-remote-write.md): ingest metrics into {{es}}.
