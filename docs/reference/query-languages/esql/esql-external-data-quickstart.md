---
navigation_title: "Quickstart"
description: "Register a data source, create a dataset, and run your first query against external data using {{esql}}."
applies_to:
  stack: preview =9.5
  serverless: preview
products:
  - id: elasticsearch
---

# {{esql}} external data sources quickstart

This guide walks you through connecting {{es}} to external data and querying it with {{esql}}. By the end, you will have a working data source, a dataset, and a query returning results from external storage.

## Before you begin

Make sure you have the following:

- An {{es}} deployment running version 9.5 or later, or a serverless project.
- A [supported external data store](esql-external-data.md#supported-data-source-types) containing data in a [supported format](esql-external-data.md#supported-file-formats).
- Credentials with read access to the data source, or a data source that allows anonymous access.
- The cluster `manage` privilege to create data sources.
- The index `manage` privilege to create datasets.

## Quickstart

These steps walk you through registering a data source, creating a dataset, and querying external data with {{esql}}.

:::::::{stepper}

::::::{step} Register a data source
A data source tells {{es}} how to connect to an external storage system. Create one by sending a `PUT` request with the connection type and credentials.

This example registers an S3 data source:

::::{tab-set}
:group: api-examples

:::{tab-item} Console
:sync: console
```console
PUT /_query/data_source/prod_s3_logs
{
  "type": "s3", <1>
  "description": "Production S3 logs bucket, us-east-1",
  "settings": {
    "region": "us-east-1", <2>
    "access_key": "AKIA...",
    "secret_key": "wJal..."
  }
}
```
1. The data source type. For the full list, refer to [supported data source types](esql-external-data.md#supported-data-source-types).
2. Type-specific settings. Each data source type has its own connection and authentication fields. For the full list, refer to the [external data source reference](esql-external-data-reference.md).
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X PUT "${ELASTICSEARCH_URL}/_query/data_source/prod_s3_logs" \
  -H "Authorization: ApiKey ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
  "type": "s3",
  "description": "Production S3 logs bucket, us-east-1",
  "settings": {
    "region": "us-east-1",
    "access_key": "AKIA...",
    "secret_key": "wJal..."
  }
}'
```
:::

::::

:::{tip}
Credentials are optional for all data source types. When omitted, {{es}} uses the default credential chain for the storage system, such as IAM roles, environment variables, or managed identity.
:::

Confirm that the data source was created by retrieving it:

::::{tab-set}
:group: api-examples

:::{tab-item} Console
:sync: console
```console
GET /_query/data_source/prod_s3_logs
```
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X GET "${ELASTICSEARCH_URL}/_query/data_source/prod_s3_logs" \
  -H "Authorization: ApiKey ${API_KEY}"
```
:::

::::

The response includes all settings, with credential values replaced by `::es_redacted::`:

```json
{
  "data_source_id": "prod_s3_logs",
  "type": "s3",
  "description": "Production S3 logs bucket, us-east-1",
  "settings": {
    "region": "us-east-1",
    "access_key": "::es_redacted::", <1>
    "secret_key": "::es_redacted::"
  }
}
```
1. Credential values are never returned in API responses.

:::{note}
Creating a data source does not validate connectivity to the external system. To verify that a data source is working, create a dataset that references it and run a query. If the credentials or endpoint are incorrect, the query will return an error.
:::
::::::

::::::{step} Create a dataset
A dataset points at specific files within a data source and makes them queryable as a virtual index. It references a data source by name and specifies a resource path that identifies the files to read.

This example creates a dataset that points at Parquet files in an S3 bucket:

::::{tab-set}
:group: api-examples

:::{tab-item} Console
:sync: console
```console
PUT /_query/dataset/access_logs
{
  "data_source": "prod_s3_logs", <1>
  "resource": "s3://logs-bucket/access/**/*.parquet", <2>
  "description": "Production access logs",
  "settings": {
    "partition_detection": "hive" <3>
  }
}
```
1. The name of the data source to connect through.
2. A glob pattern that matches the files to query. The `**` wildcard matches any number of subdirectories.
3. Optional dataset settings. Here, Hive-style partition detection is enabled so that directory names like `year=2024/month=03/` are exposed as queryable fields.
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X PUT "${ELASTICSEARCH_URL}/_query/dataset/access_logs" \
  -H "Authorization: ApiKey ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
  "data_source": "prod_s3_logs",
  "resource": "s3://logs-bucket/access/**/*.parquet",
  "description": "Production access logs",
  "settings": {
    "partition_detection": "hive"
  }
}'
```
:::

::::

Confirm that the dataset was created by listing all datasets:

::::{tab-set}
:group: api-examples

:::{tab-item} Console
:sync: console
```console
GET /_query/dataset/access_logs
```
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X GET "${ELASTICSEARCH_URL}/_query/dataset/access_logs" \
  -H "Authorization: ApiKey ${API_KEY}"
```
:::

::::

The response includes the full dataset definition:

```json
{
  "dataset_id": "access_logs",
  "data_source": "prod_s3_logs",
  "resource": "s3://logs-bucket/access/**/*.parquet",
  "description": "Production access logs",
  "settings": {
    "partition_detection": "hive"
  }
}
```
::::::

::::::{step} Query the dataset
Once a dataset exists, query it with `FROM` just like any {{es}} index:

::::{tab-set}
:group: query-examples

:::{tab-item} {{esql}}
:sync: esql
```esql
FROM access_logs
| WHERE status_code >= 400
| STATS error_count = COUNT(*) BY service_name
| SORT error_count DESC
| LIMIT 20
```
:::

:::{tab-item} Console
:sync: console
```console
POST /_query
{
  "query": "FROM access_logs | WHERE status_code >= 400 | STATS error_count = COUNT(*) BY service_name | SORT error_count DESC | LIMIT 20"
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
  "query": "FROM access_logs | WHERE status_code >= 400 | STATS error_count = COUNT(*) BY service_name | SORT error_count DESC | LIMIT 20"
}'
```
:::

::::

If the query returns results, your external data source is working. You can now use the full range of {{esql}} processing commands on this dataset.
::::::

:::::::

## Next steps

- For the full API reference, refer to the [external data source reference](esql-external-data-reference.md).
- For a high-level overview of concepts and capabilities, refer to [query external data with {{esql}}](esql-external-data.md).
