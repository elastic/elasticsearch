---
navigation_title: "Quickstart"
description: "Step-by-step tutorial for setting up ES|QL Data Federation with a public S3 bucket, creating a dataset, and running your first federated query."
applies_to:
  stack: experimental =9.5
  serverless: unavailable
products:
  - id: elasticsearch
---

# Get started with {{esql}} Data Federation

This guide walks you through connecting {{es}} to external data and querying it with {{esql}}. By the end, you have a working data source, a dataset, and a query returning results from external storage.

The example uses the [Ookla Open Speedtest dataset](https://github.com/teamookla/ookla-open-data), a publicly accessible collection of internet performance metrics aggregated by geographic tile. Because the bucket allows anonymous access, you can follow along without AWS credentials.

:::{include} _snippets/data-federation/experimental-warning.md
:::

## Before you begin

Make sure you have the following:

- An {{es}} deployment running version 9.5 or later, with [ES|QL Data Federation enabled](esql-data-federation.md#enable-the-feature).
- An [Enterprise subscription](https://www.elastic.co/subscriptions) for {{ech}}, {{ece}}, {{eck}}, or self-managed deployments.
- The cluster `manage` privilege to create data sources.
- The index `manage` privilege to create datasets.

## Quickstart

These steps walk you through registering a data source, creating a dataset, and querying federated data with {{esql}}.

:::::::{stepper}

::::::{step} Register a data source
A data source defines the connection to an external storage system, including its type, region, and credentials. Once registered, any number of datasets can reference it.

This example registers a data source that points at a public S3 bucket with anonymous access.

::::{tab-set}
:group: surface

:::{tab-item} UI
:sync: ui
1. Go to **Data management** > **{{esql}} Data Federation**.
2. On the **Data sources** tab, click **Connect data source**.
3. Set **Data source type** to **Amazon S3**.
4. Enter `ookla_speedtest` as the **Name**.
5. Set **Region** to `us-east-1`.
6. Under **Authentication**, from the **Preferred method** menu, select **Anonymous**.
7. Click **Connect**.
:::

:::{tab-item} Console
:sync: console
```console
PUT /_query/data_source/ookla_speedtest
{
  "type": "s3",
  "settings": {
    "region": "us-east-1",
    "auth": "anonymous" <1>
  }
}
```
1. Enables anonymous access for public buckets. For private data, use `auth: 'static_credentials'` with `access_key` and `secret_key` instead.

A successful request returns `{"acknowledged": true}`.
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X PUT "${ELASTICSEARCH_URL}/_query/data_source/ookla_speedtest" \
  -H "Authorization: ApiKey ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
  "type": "s3",
  "settings": {
    "region": "us-east-1",
    "auth": "anonymous"
  }
}'
```

A successful request returns `{"acknowledged": true}`.
:::

::::

Confirm the data source was created:

::::{tab-set}
:group: surface

:::{tab-item} UI
:sync: ui
The new data source appears on the **Data sources** tab, showing its type and region:

:::{image} images/data-federation/data-sources-list.png
:alt: The Data sources tab listing the ookla_speedtest data source
:width: 600px
:::
:::

:::{tab-item} Console
:sync: console
```console
GET /_query/data_source/ookla_speedtest
```
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X GET "${ELASTICSEARCH_URL}/_query/data_source/ookla_speedtest" \
  -H "Authorization: ApiKey ${API_KEY}"
```
:::

::::

:::{note}
Creating a data source does not validate connectivity to the external system. To verify that a data source is working, create a dataset that references it and run a query. If the credentials or endpoint are incorrect, the query returns an error.
:::
::::::

::::::{step} Create a dataset
A dataset points at specific files within a data source and makes them queryable as a virtual index. It references a data source by name and specifies a resource path that identifies the files to read.

This example creates a dataset over one quarter of Ookla's fixed-broadband performance data. Each Parquet file contains speedtest results aggregated into geographic tiles. The key columns are:

- `avg_d_kbps`, `avg_u_kbps`: average download and upload throughput per tile, in kbps
- `avg_lat_ms`: average latency per tile, in milliseconds
- `tests`, `devices`: number of speedtests and unique devices per tile

::::{tab-set}
:group: surface

:::{tab-item} UI
:sync: ui
1. Select the **Datasets** tab, then click **Add dataset**.
2. Select `ookla_speedtest` as the **Data source**.
3. Enter `speedtest_fixed` as the **Name**.
4. In **Resource**, enter the resource path that selects the files to read:

   ```text
   s3://ookla-open-data/parquet/performance/type=fixed/year=2024/quarter=1/*.parquet
   ```
5. Set **Format** to **Parquet**.
6. Click **Add**.

:::{dropdown} Show the completed Add dataset flyout
:::{image} images/data-federation/add-dataset.png
:alt: Add dataset flyout configured for the Ookla Q1 2024 fixed-broadband Parquet files
:width: 450px
:::
:::
:::

:::{tab-item} Console
:sync: console
```console
PUT /_query/dataset/speedtest_fixed
{
  "data_source": "ookla_speedtest", <1>
  "resource": "s3://ookla-open-data/parquet/performance/type=fixed/year=2024/quarter=1/*.parquet" <2>
}
```
1. The name of the data source to connect through.
2. A glob pattern matching all Parquet files for Q1 2024 fixed-broadband tests. The `*` wildcard matches any filename in that directory.

A successful request returns `{"acknowledged": true}`.
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X PUT "${ELASTICSEARCH_URL}/_query/dataset/speedtest_fixed" \
  -H "Authorization: ApiKey ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
  "data_source": "ookla_speedtest",
  "resource": "s3://ookla-open-data/parquet/performance/type=fixed/year=2024/quarter=1/*.parquet"
}'
```

A successful request returns `{"acknowledged": true}`.
:::

::::

Confirm the dataset was created:

::::{tab-set}
:group: surface

:::{tab-item} UI
:sync: ui
The new dataset appears on the **Datasets** tab, showing its data source and resource:

:::{image} images/data-federation/datasets-list.png
:alt: The Datasets tab listing the speedtest_fixed dataset
:width: 600px
:::
:::

:::{tab-item} Console
:sync: console
```console
GET /_query/dataset/speedtest_fixed
```
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X GET "${ELASTICSEARCH_URL}/_query/dataset/speedtest_fixed" \
  -H "Authorization: ApiKey ${API_KEY}"
```
:::

::::

::::::

::::::{step} Check field mappings
Before writing queries, check what field mappings {{es}} inferred from the Parquet files. Query the dataset with `LIMIT 1` to return a single row with all columns:

::::{tab-set}
:group: surface

:::{tab-item} {{esql}}
:sync: esql
```esql
FROM speedtest_fixed
| LIMIT 1
```
:::

:::{tab-item} Console
:sync: console
```console
POST /_query
{
  "query": "FROM speedtest_fixed | LIMIT 1"
}
```
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X POST "${ELASTICSEARCH_URL}/_query" \
  -H "Authorization: ApiKey ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
  "query": "FROM speedtest_fixed | LIMIT 1"
}'
```
:::

::::

The response lists every column name and its inferred type:

:::{dropdown} View response
```json
{ "name": "quadkey", "type": "keyword" }
{ "name": "tile", "type": "keyword" }
{ "name": "tile_x", "type": "double" }
{ "name": "tile_y", "type": "double" }
{ "name": "avg_d_kbps", "type": "long" }
{ "name": "avg_u_kbps", "type": "long" }
{ "name": "avg_lat_ms", "type": "long" }
{ "name": "avg_lat_down_ms", "type": "integer" }
{ "name": "avg_lat_up_ms", "type": "integer" }
{ "name": "tests", "type": "long" }
{ "name": "devices", "type": "long" }
{ "name": "type", "type": "keyword" }
{ "name": "year", "type": "integer" }
{ "name": "quarter", "type": "integer" }
```

The `type`, `year`, and `quarter` columns come from Hive-style partition paths in the S3 bucket. {{es}} detects these automatically when `partition_detection` is set to `auto` (the default).
:::

:::{tip}
If a column has an unexpected type, you can override it with a [dataset mapping](esql-data-federation-datasets.md#declare-a-dataset-mapping).
:::
::::::

::::::{step} Query the dataset
Once a dataset exists, query it with `FROM` just like any {{es}} index. This query summarizes the Q1 2024 fixed-broadband data across all geographic tiles:

::::{tab-set}
:group: surface

:::{tab-item} {{esql}}
:sync: esql
```esql
FROM speedtest_fixed
| STATS
    total_tiles        = COUNT(*),    // Geographic tiles in the dataset
    total_tests        = SUM(tests),  // Speedtests recorded in Q1 2024
    total_devices      = SUM(devices), // Unique devices that contributed
    max_tests_per_tile = MAX(tests)   // Busiest single tile
```
:::

:::{tab-item} Console
:sync: console
```console
POST /_query
{
  "query": "FROM speedtest_fixed | STATS total_tiles = COUNT(*), total_tests = SUM(tests), total_devices = SUM(devices), max_tests_per_tile = MAX(tests)"
}
```
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X POST "${ELASTICSEARCH_URL}/_query" \
  -H "Authorization: ApiKey ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
  "query": "FROM speedtest_fixed | STATS total_tiles = COUNT(*), total_tests = SUM(tests), total_devices = SUM(devices), max_tests_per_tile = MAX(tests)"
}'
```
:::

::::

The response includes execution metadata, followed by the result columns and values. Execution metadata is omitted here because it varies by cluster:

```json
{
  "columns": [
    {
      "name": "total_tiles",
      "type": "long"
    },
    {
      "name": "total_tests",
      "type": "long"
    },
    {
      "name": "total_devices",
      "type": "long"
    },
    {
      "name": "max_tests_per_tile",
      "type": "long"
    }
  ],
  "values": [
    [6655986, 118589626, 34467251, 35873]
  ]
}
```

If the query returns results, your data source is working. You can now use the full range of {{esql}} processing commands on this dataset.
::::::

::::::{step} Query federated and indexed data together
Datasets share the same namespace as regular indices, so you can query both in a single `FROM`. This lets you combine external data with indexed data in a single query.

First, create an index with a few sample documents to query alongside the dataset:

```console
PUT /network_incidents
{
  "mappings": {
    "properties": {
      "category":     { "type": "keyword" },
      "severity":     { "type": "keyword" },
      "duration_min": { "type": "integer" }
    }
  }
}
```

Then index a few documents:

```console
POST /_bulk
{"index":{"_index":"network_incidents"}}
{"category":"outage","severity":"high","duration_min":45}
{"index":{"_index":"network_incidents"}}
{"category":"degradation","severity":"medium","duration_min":12}
{"index":{"_index":"network_incidents"}}
{"category":"outage","severity":"low","duration_min":8}
```

Now query both sources together. `FROM` resolves each name independently, whether it is an index, data stream, alias, [{{esql}} view](esql-views.md), or dataset. Use `METADATA _index` to see where each row came from:

::::{tab-set}
:group: surface

:::{tab-item} {{esql}}
:sync: esql
```esql
FROM speedtest_fixed, network_incidents METADATA _index
| KEEP _index, category, severity, duration_min, avg_d_kbps, avg_lat_ms
| LIMIT 5
```
:::

:::{tab-item} Console
:sync: console
```console
POST /_query
{
  "query": "FROM speedtest_fixed, network_incidents METADATA _index | KEEP _index, category, severity, duration_min, avg_d_kbps, avg_lat_ms | LIMIT 5"
}
```
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X POST "${ELASTICSEARCH_URL}/_query" \
  -H "Authorization: ApiKey ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
  "query": "FROM speedtest_fixed, network_incidents METADATA _index | KEEP _index, category, severity, duration_min, avg_d_kbps, avg_lat_ms | LIMIT 5"
}'
```
:::

::::

The `_index` column shows where each row came from. Columns that do not exist in a given source return `null`. Execution metadata is omitted here:

```json
{
  "columns": [
    { "name": "_index", "type": "keyword" },
    { "name": "category", "type": "keyword" },
    { "name": "severity", "type": "keyword" },
    { "name": "duration_min", "type": "integer" },
    { "name": "avg_d_kbps", "type": "long" },
    { "name": "avg_lat_ms", "type": "long" }
  ],
  "values": [
    ["network_incidents", "outage",      "high",   45, null, null],
    ["network_incidents", "degradation", "medium", 12, null, null],
    ["network_incidents", "outage",      "low",     8, null, null],
    ["speedtest_fixed",   null,           null,   null, 8033,  70],
    ["speedtest_fixed",   null,           null,   null, 10327,  8]
  ]
}
```
::::::

:::::::

## Use your own data

The quickstart uses a public bucket with anonymous access. To connect to a private bucket, supply credentials when registering the data source. Several [authentication methods](esql-data-federation-sources.md#authentication) are available. For example, using static credentials:

```console
PUT /_query/data_source/my_s3_logs
{
  "type": "s3",
  "description": "Production logs bucket",
  "settings": {
    "region": "us-east-1",
    "auth": "static_credentials",
    "access_key": "<AWS_ACCESS_KEY_ID>",
    "secret_key": "<AWS_SECRET_ACCESS_KEY>"
  }
}
```

:::{important}
When a data source includes credentials, {{es}} encrypts them before storing them in the cluster state, using the cluster state encryption key. This key is available automatically in most deployments. If it is not available, a request that includes credentials returns a `503` error. Refer to [credential encryption](esql-data-federation-security.md#credential-encryption) for details.
:::

Credential values are never returned in API responses. When you retrieve a data source, secrets are replaced by `::es_redacted::`.

## Clean up

To remove the resources created in this guide, delete the dataset first because a data source cannot be deleted while datasets reference it. Then delete the data source and the sample index:

::::{tab-set}
:group: surface

:::{tab-item} Console
:sync: console
```console
DELETE /_query/dataset/speedtest_fixed
DELETE /_query/data_source/ookla_speedtest
DELETE /network_incidents
```
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X DELETE "${ELASTICSEARCH_URL}/_query/dataset/speedtest_fixed" \
  -H "Authorization: ApiKey ${API_KEY}"

curl -X DELETE "${ELASTICSEARCH_URL}/_query/data_source/ookla_speedtest" \
  -H "Authorization: ApiKey ${API_KEY}"

curl -X DELETE "${ELASTICSEARCH_URL}/network_incidents" \
  -H "Authorization: ApiKey ${API_KEY}"
```
:::

::::

## Next steps

Now that you have a working data source and dataset, you can:

- **Speed up queries.** Learn how column selection, partition pruning, and filter pushdown reduce the data read from storage. Refer to [query external datasets](esql-data-federation-querying.md).
- **Connect your own bucket.** The quickstart uses anonymous access. To connect a private bucket with credentials or federated identity, refer to [connect external data sources](esql-data-federation-sources.md).
- **Tune dataset settings.** Override file formats, customize schema inference, and declare explicit column mappings. Refer to [select external datasets](esql-data-federation-datasets.md).
