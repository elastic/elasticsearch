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

The example uses a publicly accessible S3 dataset, so you can follow along without AWS credentials.

## Before you begin

Make sure you have the following:

- An {{es}} deployment running version 9.5 or later, or a serverless project.
- The cluster `manage` privilege to create data sources.
- The index `manage` privilege to create datasets.

## Quickstart

These steps walk you through registering a data source, creating a dataset, and querying external data with {{esql}}.

:::::::{stepper}

::::::{step} Register a data source
A data source tells {{es}} how to connect to an external storage system. Create one by sending a `PUT` request with the connection type and settings.

This example registers a data source pointing at a public S3 bucket with anonymous access:

::::{tab-set}
:group: api-examples

:::{tab-item} Console
:sync: console
```console
PUT /_query/data_source/nyc_taxi
{
  "type": "s3",
  "description": "NYC Taxi & Limousine Commission trip data (public)",
  "settings": {
    "region": "us-east-1",
    "auth": "none" <1>
  }
}
```
1. `auth: "none"` enables anonymous access for public buckets. For private data, supply `access_key` and `secret_key` instead.
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X PUT "${ELASTICSEARCH_URL}/_query/data_source/nyc_taxi" \
  -H "Authorization: ApiKey ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
  "type": "s3",
  "description": "NYC Taxi & Limousine Commission trip data (public)",
  "settings": {
    "region": "us-east-1",
    "auth": "none"
  }
}'
```
:::

::::

Confirm that the data source was created by retrieving it:

::::{tab-set}
:group: api-examples

:::{tab-item} Console
:sync: console
```console
GET /_query/data_source/nyc_taxi
```
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X GET "${ELASTICSEARCH_URL}/_query/data_source/nyc_taxi" \
  -H "Authorization: ApiKey ${API_KEY}"
```
:::

::::

:::{note}
Creating a data source does not validate connectivity to the external system. To verify that a data source is working, create a dataset that references it and run a query. If the credentials or endpoint are incorrect, the query will return an error.
:::
::::::

::::::{step} Create a dataset
A dataset points at specific files within a data source and makes them queryable as a virtual index. It references a data source by name and specifies a resource path that identifies the files to read.

This example creates a dataset that points at Parquet files in the NYC taxi bucket:

::::{tab-set}
:group: api-examples

:::{tab-item} Console
:sync: console
```console
PUT /_query/dataset/nyc_taxi_trips
{
  "data_source": "nyc_taxi", <1>
  "resource": "s3://nyc-tlc/trip data/*.parquet", <2>
  "description": "NYC yellow taxi trip records"
}
```
1. The name of the data source to connect through.
2. A glob pattern that matches the files to query.
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X PUT "${ELASTICSEARCH_URL}/_query/dataset/nyc_taxi_trips" \
  -H "Authorization: ApiKey ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
  "data_source": "nyc_taxi",
  "resource": "s3://nyc-tlc/trip data/*.parquet",
  "description": "NYC yellow taxi trip records"
}'
```
:::

::::

Confirm that the dataset was created:

::::{tab-set}
:group: api-examples

:::{tab-item} Console
:sync: console
```console
GET /_query/dataset/nyc_taxi_trips
```
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X GET "${ELASTICSEARCH_URL}/_query/dataset/nyc_taxi_trips" \
  -H "Authorization: ApiKey ${API_KEY}"
```
:::

::::
::::::

::::::{step} Query the dataset
Once a dataset exists, query it with `FROM` just like any {{es}} index:

::::{tab-set}
:group: query-examples

:::{tab-item} {{esql}}
:sync: esql
```esql
FROM nyc_taxi_trips
| STATS trip_count = COUNT(*), avg_fare = AVG(fare_amount) BY payment_type
| SORT trip_count DESC
| LIMIT 10
```
:::

:::{tab-item} Console
:sync: console
```console
POST /_query
{
  "query": "FROM nyc_taxi_trips | STATS trip_count = COUNT(*), avg_fare = AVG(fare_amount) BY payment_type | SORT trip_count DESC | LIMIT 10"
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
  "query": "FROM nyc_taxi_trips | STATS trip_count = COUNT(*), avg_fare = AVG(fare_amount) BY payment_type | SORT trip_count DESC | LIMIT 10"
}'
```
:::

::::

If the query returns results, your external data source is working. You can now use the full range of {{esql}} processing commands on this dataset.
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
    "access_key": "AKIA...",
    "secret_key": "wJal..."
  }
}
```

:::{important}
When a data source includes credentials, {{es}} encrypts them before storing them in the cluster state. This is handled automatically in {{ech}} and {{serverless-short}} deployments. For self-managed, {{ece}}, and {{eck}} deployments, you must configure [credential encryption](esql-external-data-reference.md#credential-encryption) first.
:::

Credential values are never returned in API responses. When you retrieve a data source, secrets are replaced by `::es_redacted::`.

## Next steps

- For the full API reference, refer to the [external data source reference](esql-external-data-reference.md).
- For a high-level overview of concepts and capabilities, refer to [query external data with {{esql}}](esql-external-data.md).
