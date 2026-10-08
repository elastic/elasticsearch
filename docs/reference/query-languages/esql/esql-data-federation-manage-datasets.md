---
navigation_title: "Create and manage"
description: "Create, inspect, update, and delete ES|QL Data Federation datasets in Kibana or with the Elasticsearch API."
applies_to:
  stack: experimental 9.5+
  serverless: unavailable
products:
  - id: elasticsearch
---

# Create and manage datasets for {{esql}} Data Federation

Create and manage {{esql}} Data Federation datasets in {{kib}} or with the `/_query/dataset` API. Before
creating a dataset, [connect a data source](esql-data-federation-sources.md) and review how to
[define a dataset](esql-data-federation-datasets.md#define-a-dataset).

:::{include} _snippets/data-federation/experimental-warning.md
:::

## Manage datasets in the UI

In {{kib}}, you create and manage datasets from the **Datasets** tab under **Data management** > **{{esql}} Data Federation**.

The **Datasets** tab lists each dataset with its:

- Data source and data source type
- Resource
- Description

From this tab you can search your datasets, filter by data source, add a new one, and edit or delete an existing one.

### Add a new dataset

Click **Add dataset** to open a flyout where you define the dataset:

- **Data source**: the connected data source to read through.
- **Name**: a unique name for use in queries. Names must be lowercase and cannot begin with `-`, `_`, or `+`. A dataset cannot share a name with any existing index, data stream, alias, or view.
- **Description**: an optional description (up to 1,000 characters).
- **Resource**: the URI and glob pattern that selects the files to read. Refer to [resource patterns](esql-data-federation-patterns.md) for the pattern language.
- **Format**: the file format. This selection is required in the {{kib}} UI. The API can omit `settings.format` when the resource pattern implies exactly one format. Extensionless or mixed patterns require `format`. Refer to [supported file formats](esql-data-federation-file-formats.md).

To configure how the format is read, expand **Advanced settings**. Refer to [dataset settings](esql-data-federation-dataset-settings.md).

To customize the inferred schema, rename columns, or override field types, [declare a schema explicitly](esql-data-federation-schema.md#declare-a-schema-explicitly). Schema customization is not available in the UI.

## Manage datasets using the API

Datasets are managed under the `/_query/dataset` endpoint. All dataset operations require the index `manage` privilege on the dataset name, or a fine-grained dataset privilege. Refer to [manage credentials and privileges](esql-data-federation-security.md) for details.

| Operation | Endpoint | API reference |
|---|---|---|
| [Create or update](#create-or-update-a-dataset) | `PUT /_query/dataset/{name}` | [Create or update an ES\|QL dataset](https://www.elastic.co/docs/api/doc/elasticsearch/v9/operation/operation-esql-put-dataset) |
| [Get](#get-a-dataset) | `GET /_query/dataset/{name}` | [Get ES\|QL datasets](https://www.elastic.co/docs/api/doc/elasticsearch/v9/operation/operation-esql-get-dataset) |
| [List all](#list-all-datasets) | `GET /_query/dataset` | [Get ES\|QL datasets](https://www.elastic.co/docs/api/doc/elasticsearch/v9/operation/operation-esql-get-dataset) |
| [Delete](#delete-a-dataset) | `DELETE /_query/dataset/{name}` | [Delete ES\|QL datasets](https://www.elastic.co/docs/api/doc/elasticsearch/v9/operation/operation-esql-delete-dataset) |

### Create or update a dataset

[`PUT /_query/dataset/{name}`](https://www.elastic.co/docs/api/doc/elasticsearch/v9/operation/operation-esql-put-dataset) creates a new dataset or replaces an existing one entirely.

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

#### Setting validation
```{applies_to}
stack: experimental 9.6+
```

{{es}} validates setting values when you register the dataset, rather than waiting until the first query. A value that isn't supported, such as a multi-character `delimiter`, an unknown `encoding`, or a `segment_size` below the minimum, returns a `400` error that identifies the setting.

:::{note}
Datasets registered before this validation was introduced continue to work without being revalidated. Replacing one of these datasets triggers validation, so correct any unsupported values in the replacement request.
:::

#### Verify the inferred schema

After creating a dataset, verify the inferred schema by [checking its field mappings](esql-data-federation-quickstart.md#check-field-mappings).

### Get a dataset

[`GET /_query/dataset/{name}`](https://www.elastic.co/docs/api/doc/elasticsearch/v9/operation/operation-esql-get-dataset) retrieves a dataset by name.

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

### List all datasets

[`GET /_query/dataset`](https://www.elastic.co/docs/api/doc/elasticsearch/v9/operation/operation-esql-get-dataset) returns all registered datasets.

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

### Delete a dataset

[`DELETE /_query/dataset/{name}`](https://www.elastic.co/docs/api/doc/elasticsearch/v9/operation/operation-esql-delete-dataset) deletes a dataset by name.

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

After creating a dataset, [query it with {{esql}}](esql-data-federation-querying.md). To change how the dataset selects or interprets files, return to [define a dataset](esql-data-federation-datasets.md#define-a-dataset).
