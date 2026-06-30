---
navigation_title: "Quickstart"
description: "Register a data source, create a dataset, and run your first federated data query using {{esql}}."
applies_to:
  stack: preview =9.5
  serverless: preview
products:
  - id: elasticsearch
---

# {{esql}} federated data quickstart

This guide walks you through connecting {{es}} to external data and querying it with {{esql}}. By the end, you will have a working data source, a dataset, and a query returning results from external storage.

The example uses the [Ookla Open Speedtest dataset](https://github.com/teamookla/ookla-open-data), a publicly accessible collection of internet performance metrics aggregated by geographic tile. Because the bucket allows anonymous access, you can follow along without AWS credentials.

## Before you begin

Make sure you have the following:

- An {{es}} deployment running version 9.5 or later, or a serverless project.
- The cluster `manage` privilege to create data sources.
- The index `manage` privilege to create datasets.

## Quickstart

These steps walk you through registering a data source, creating a dataset, and querying federated data with {{esql}}.

:::::::{stepper}

::::::{step} Register a data source
A data source defines the connection to an external storage system, including its type, region, and credentials. Once registered, any number of datasets can reference it. Create one by sending a `PUT` request.

This example registers a data source pointing at a public S3 bucket with anonymous access:

::::{tab-set}
:group: api-examples

:::{tab-item} Console
:sync: console
```console
PUT /_query/data_source/ookla_speedtest
{
  "type": "s3",
  "settings": {
    "region": "us-east-1",
    "auth": "none" <1>
  }
}
```
1. Enables anonymous access for public buckets. For private data, supply `access_key` and `secret_key` instead.
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
    "auth": "none"
  }
}'
```
:::

::::

A successful request returns:

```json
{
  "acknowledged": true
}
```

Confirm that the data source was created by retrieving it:

::::{tab-set}
:group: api-examples

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

The response includes all data source settings:

```json
{
  "data_sources": [
    {
      "name": "ookla_speedtest",
      "type": "s3",
      "settings": {
        "region": "us-east-1",
        "auth": "none"
      }
    }
  ]
}
```

:::{note}
Creating a data source does not validate connectivity to the external system. To verify that a data source is working, create a dataset that references it and run a query. If the credentials or endpoint are incorrect, the query will return an error.
:::
::::::

::::::{step} Create a dataset
A dataset points at specific files within a data source and makes them queryable as a virtual index. It references a data source by name and specifies a resource path that identifies the files to read.

This example creates a dataset over one quarter of Ookla's fixed-broadband performance data. Each Parquet file contains speedtest results aggregated into geographic tiles, with columns for download and upload throughput (`avg_d_kbps`, `avg_u_kbps`), latency (`avg_lat_ms`), and the number of tests and devices per tile.

::::{tab-set}
:group: api-examples

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
:::

::::

A successful request returns:

```json
{
  "acknowledged": true
}
```

Confirm that the dataset was created:

::::{tab-set}
:group: api-examples

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

The response includes the full dataset definition:

```json
{
  "datasets": [
    {
      "name": "speedtest_fixed",
      "data_source": "ookla_speedtest",
      "resource": "s3://ookla-open-data/parquet/performance/type=fixed/year=2024/quarter=1/*.parquet"
    }
  ]
}
```
::::::

::::::{step} Query the dataset
Once a dataset exists, query it with `FROM` just like any {{es}} index. This query calculates global averages across all tiles:

::::{tab-set}
:group: query-examples

:::{tab-item} {{esql}}
:sync: esql
```esql
FROM speedtest_fixed
| STATS
    avg_download = AVG(avg_d_kbps), // Average download speed across all tiles, in kbps
    avg_upload   = AVG(avg_u_kbps), // Average upload speed across all tiles, in kbps
    avg_latency  = AVG(avg_lat_ms), // Average latency in milliseconds
    total_tests  = SUM(tests)       // Total number of speedtests in Q1 2024
| EVAL
    avg_download_mbps = ROUND(avg_download / 1000, 1),
    avg_upload_mbps   = ROUND(avg_upload / 1000, 1),
    avg_latency       = ROUND(avg_latency, 1)
| KEEP avg_download_mbps, avg_upload_mbps, avg_latency, total_tests
```
:::

:::{tab-item} Console
:sync: console
```console
POST /_query
{
  "query": "FROM speedtest_fixed | STATS avg_download = AVG(avg_d_kbps), avg_upload = AVG(avg_u_kbps), avg_latency = AVG(avg_lat_ms), total_tests = SUM(tests) | EVAL avg_download_mbps = ROUND(avg_download / 1000, 1), avg_upload_mbps = ROUND(avg_upload / 1000, 1), avg_latency = ROUND(avg_latency, 1) | KEEP avg_download_mbps, avg_upload_mbps, avg_latency, total_tests"
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
  "query": "FROM speedtest_fixed | STATS avg_download = AVG(avg_d_kbps), avg_upload = AVG(avg_u_kbps), avg_latency = AVG(avg_lat_ms), total_tests = SUM(tests) | EVAL avg_download_mbps = ROUND(avg_download / 1000, 1), avg_upload_mbps = ROUND(avg_upload / 1000, 1), avg_latency = ROUND(avg_latency, 1) | KEEP avg_download_mbps, avg_upload_mbps, avg_latency, total_tests"
}'
```
:::

::::

The response includes execution metadata, followed by the result columns and values. Execution metadata is omitted here because it varies by cluster:

```json
{
  "columns": [
    {
      "name": "avg_download_mbps",
      "type": "double"
    },
    {
      "name": "avg_upload_mbps",
      "type": "double"
    },
    {
      "name": "avg_latency",
      "type": "double"
    },
    {
      "name": "total_tests",
      "type": "long"
    }
  ],
  "values": [
    [156.4, 78.3, 23.8, 118589626]
  ]
}
```

If the query returns results, your data source is working. You can now use the full range of {{esql}} processing commands on this dataset.
::::::

<!-- TODO: Heterogeneous FROM (FROM dataset, index) was not working against the 9.5.0 snapshot
     used during testing — "FROM mixing datasets and non-datasets is not supported". The fix
     landed in elastic/elasticsearch#151977 (merged 2026-06-25) after the snapshot was cut.
     Verify this step works against the 9.5.0 release build before publishing. -->

::::::{step} Query federated and indexed data together
Datasets share the same namespace as regular indices, so you can query both in a single `FROM`. This lets you correlate external data with indexed data without ingesting anything.

First, index a few sample documents to query alongside the dataset:

```console
POST /_bulk
{"index":{"_index":"network_incidents"}}
{"quadkey":"0320101","type":"outage","duration_min":45}
{"index":{"_index":"network_incidents"}}
{"quadkey":"1202032","type":"degradation","duration_min":12}
{"index":{"_index":"network_incidents"}}
{"quadkey":"0320101","type":"degradation","duration_min":8}
```

The response confirms that all three documents were created. Generated document IDs and shard metadata are omitted here:

```json
{
  "errors": false,
  "items": [
    {
      "index": {
        "_index": "network_incidents",
        "result": "created",
        "status": 201
      }
    },
    {
      "index": {
        "_index": "network_incidents",
        "result": "created",
        "status": 201
      }
    },
    {
      "index": {
        "_index": "network_incidents",
        "result": "created",
        "status": 201
      }
    }
  ]
}
```

Now query both sources together. `FROM` resolves each name independently, whether it is an index (or index abstraction such as a data stream, alias, etc.) or a dataset:

```esql
FROM speedtest_fixed, network_incidents
| WHERE quadkey IS NOT NULL
| STATS avg_latency = AVG(avg_lat_ms), records = COUNT(*) BY quadkey
| SORT records DESC
| LIMIT 20
```
::::::

:::::::

## Use your own data

To connect to a private S3 bucket, supply credentials when registering the data source:

```console
PUT /_query/data_source/my_s3_logs
{
  "type": "s3",
  "description": "Production logs bucket",
  "settings": {
    "region": "us-east-1",
    "access_key": "<AWS_ACCESS_KEY_ID>",
    "secret_key": "<AWS_SECRET_ACCESS_KEY>"
  }
}
```

When credential encryption is configured, a successful request returns:

```json
{
  "acknowledged": true
}
```

:::{important}
When a data source includes credentials, {{es}} encrypts them before storing them in the cluster state. This is handled automatically in {{ech}} and {{serverless-short}} deployments. For self-managed, {{ece}}, and {{eck}} deployments, you must configure [credential encryption](esql-federated-data-reference.md#credential-encryption) first.
:::

Credential values are never returned in API responses. When you retrieve a data source, secrets are replaced by `::es_redacted::`.

## Next steps

- For the full API reference, refer to the [federated data reference](esql-federated-data-reference.md).
- For a high-level overview of concepts and capabilities, refer to [{{esql}} federated data](esql-federated-data.md).
