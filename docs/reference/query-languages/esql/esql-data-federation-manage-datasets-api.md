---
navigation_title: "Manage with API"
description: "Create, get, list, and delete ES|QL Data Federation datasets with the /_query/dataset API."
applies_to:
  stack: experimental 9.5+
  serverless: unavailable
products:
  - id: elasticsearch
---

# Manage datasets with the {{es}} API

:::{include} _snippets/data-federation/experimental-warning.md
:::

Datasets are managed under the `/_query/dataset` endpoint. All dataset operations require the index `manage` privilege on the dataset name, or a fine-grained dataset privilege. Refer to [manage credentials and privileges](esql-data-federation-security.md) for details.

| Operation | Endpoint | API reference |
|---|---|---|
| [Create or update](#create-or-update-a-dataset) | `PUT /_query/dataset/{name}` | [Create or update an ES\|QL dataset]({{es-apis}}operation/operation-esql-put-dataset) |
| [Get](#get-a-dataset) | `GET /_query/dataset/{name}` | [Get ES\|QL datasets]({{es-apis}}operation/operation-esql-get-dataset) |
| [List all](#list-all-datasets) | `GET /_query/dataset` | [Get ES\|QL datasets]({{es-apis}}operation/operation-esql-get-dataset) |
| [Delete](#delete-a-dataset) | `DELETE /_query/dataset/{name}` | [Delete ES\|QL datasets]({{es-apis}}operation/operation-esql-delete-dataset) |

## Create or update a dataset

[`PUT /_query/dataset/{name}`]({{es-apis}}operation/operation-esql-put-dataset) creates a new dataset or replaces an existing one entirely.

A dataset cannot have the same name as an existing index, data stream, alias, or view, because dataset names share the same namespace. Dataset names must be lowercase and cannot begin with `-`, `_`, or `+`.

The optional `description` can be at most 1,000 characters long.

{applies_to}`stack: experimental 9.6+` For restrictions on S3 bucket names, aliases, and Amazon Resource Names (ARNs), refer to [Amazon S3 resource restrictions](esql-data-federation-patterns.md#s3-resource-requirements).

::::{tab-set}
:group: api-ref

:::{tab-item} Console
:sync: console
```console
PUT /_query/dataset/access_logs
{
  "data_source": "prod_s3_logs",
  "resource": "s3://logs-bucket/access/**/*.parquet",
  "description": "Production access logs",
  "settings": {
    "partition_detection": "hive"
  }
}
```
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

### Setting validation
```{applies_to}
stack: experimental 9.6+
```

{{es}} validates setting values when you register the dataset, rather than waiting until the first query. A value that isn't supported, such as a multi-character `delimiter`, an unknown `encoding`, or a `segment_size` below the minimum, returns a `400` error that identifies the setting.

:::{note}
Datasets registered before this validation was introduced continue to work without being revalidated. Replacing one of these datasets triggers validation, so correct any unsupported values in the replacement request.
:::

### Verify the inferred schema

After creating a dataset, verify the inferred schema by [checking its field mappings](esql-data-federation-quickstart.md#check-field-mappings).

## Get a dataset

[`GET /_query/dataset/{name}`]({{es-apis}}operation/operation-esql-get-dataset) retrieves a dataset by name.

::::{tab-set}
:group: api-ref

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

## List all datasets

[`GET /_query/dataset`]({{es-apis}}operation/operation-esql-get-dataset) returns all registered datasets.

::::{tab-set}
:group: api-ref

:::{tab-item} Console
:sync: console
```console
GET /_query/dataset
```
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X GET "${ELASTICSEARCH_URL}/_query/dataset" \
  -H "Authorization: ApiKey ${API_KEY}"
```
:::

::::

## Delete a dataset

[`DELETE /_query/dataset/{name}`]({{es-apis}}operation/operation-esql-delete-dataset) deletes a dataset by name.

::::{tab-set}
:group: api-ref

:::{tab-item} Console
:sync: console
```console
DELETE /_query/dataset/access_logs
```
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X DELETE "${ELASTICSEARCH_URL}/_query/dataset/access_logs" \
  -H "Authorization: ApiKey ${API_KEY}"
```
:::

::::

## Next steps

:::{include} _snippets/data-federation/datasets-next-steps.md
:::
