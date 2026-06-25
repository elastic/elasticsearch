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
PUT /_query/data_source/public_blockchain
{
  "type": "s3",
  "settings": {
    "region": "us-east-2",
    "auth": "none" <1>
  }
}
```
1. `auth: "none"` enables anonymous access for public buckets. For private data, supply `access_key` and `secret_key` instead.
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X PUT "${ELASTICSEARCH_URL}/_query/data_source/public_blockchain" \
  -H "Authorization: ApiKey ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
  "type": "s3",
  "settings": {
    "region": "us-east-2",
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
GET /_query/data_source/public_blockchain
```
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X GET "${ELASTICSEARCH_URL}/_query/data_source/public_blockchain" \
  -H "Authorization: ApiKey ${API_KEY}"
```
:::

::::

The response includes all data source settings:

```json
{
  "data_sources": [
    {
      "name": "public_blockchain",
      "type": "s3",
      "settings": {
        "region": "us-east-2",
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

This example creates a dataset that points at Bitcoin transaction Parquet files in the AWS Public Blockchain Data bucket:

::::{tab-set}
:group: api-examples

:::{tab-item} Console
:sync: console
```console
PUT /_query/dataset/btc_transactions
{
  "data_source": "public_blockchain", <1>
  "resource": "s3://aws-public-blockchain/v1.0/btc/transactions/**/*.parquet" <2>
}
```
1. The name of the data source to connect through.
2. A glob pattern that matches the files to query. The `**` wildcard matches any number of subdirectories.
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X PUT "${ELASTICSEARCH_URL}/_query/dataset/btc_transactions" \
  -H "Authorization: ApiKey ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
  "data_source": "public_blockchain",
  "resource": "s3://aws-public-blockchain/v1.0/btc/transactions/**/*.parquet"
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
GET /_query/dataset/btc_transactions
```
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X GET "${ELASTICSEARCH_URL}/_query/dataset/btc_transactions" \
  -H "Authorization: ApiKey ${API_KEY}"
```
:::

::::

The response includes the full dataset definition:

```json
{
  "datasets": [
    {
      "name": "btc_transactions",
      "data_source": "public_blockchain",
      "resource": "s3://aws-public-blockchain/v1.0/btc/transactions/**/*.parquet"
    }
  ]
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
FROM btc_transactions
| WHERE fee > 0
| STATS tx_count = COUNT(*), avg_fee = AVG(fee) BY is_coinbase
| SORT tx_count DESC
```
:::

:::{tab-item} Console
:sync: console
```console
POST /_query
{
  "query": "FROM btc_transactions | WHERE fee > 0 | STATS tx_count = COUNT(*), avg_fee = AVG(fee) BY is_coinbase | SORT tx_count DESC"
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
  "query": "FROM btc_transactions | WHERE fee > 0 | STATS tx_count = COUNT(*), avg_fee = AVG(fee) BY is_coinbase | SORT tx_count DESC"
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
