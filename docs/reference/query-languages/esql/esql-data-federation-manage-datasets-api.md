---
navigation_title: "Manage with API"
description: "Create, get, list, and delete ES|QL Data Federation datasets with the /_query/dataset API."
applies_to:
  stack: preview 9.6+, experimental =9.5
  serverless: unavailable
products:
  - id: elasticsearch
---

# Create and manage datasets with the {{es}} API

:::{include} _snippets/data-federation/experimental-warning.md
:::

Datasets are managed under the `/_query/dataset` endpoint. All dataset operations require the index `manage` privilege on the dataset name, or a fine-grained dataset privilege. Refer to [manage credentials and privileges](esql-data-federation-security.md) for details.

| Operation | Endpoint | API reference |
|---|---|---|
| [Create or update](#create-or-update-a-dataset) | `PUT /_query/dataset/{name}` | [Create or update an ES\|QL dataset]({{es-apis}}operation/operation-esql-put-dataset) |
| [Get](#get-a-dataset) | `GET /_query/dataset/{name}` | [Get ES\|QL datasets]({{es-apis}}operation/operation-esql-get-dataset) |
| [List all](#list-all-datasets) | `GET /_query/dataset` | [Get ES\|QL datasets]({{es-apis}}operation/operation-esql-get-dataset) |
| [Enable](#enable-or-disable-a-dataset) {applies_to}`stack: preview 9.7+` | `POST /_query/dataset/{name}/_enable` | Enable an ES\|QL dataset |
| [Disable](#enable-or-disable-a-dataset) {applies_to}`stack: preview 9.7+` | `POST /_query/dataset/{name}/_disable` | Disable an ES\|QL dataset |
| [Delete](#delete-a-dataset) | `DELETE /_query/dataset/{name}` | [Delete ES\|QL datasets]({{es-apis}}operation/operation-esql-delete-dataset) |

This page is a quick reference for each operation. For the complete request and response details, follow the API reference links in the table.

## Create or update a dataset

[`PUT /_query/dataset/{name}`]({{es-apis}}operation/operation-esql-put-dataset) creates a new dataset or replaces an existing one entirely.

A dataset cannot have the same name as an existing index, data stream, alias, or view, because dataset names share the same namespace. Dataset names must be lowercase and cannot begin with `-`, `_`, or `+`.

The optional `description` can be at most 1,000 characters long.

{applies_to}`stack: preview 9.6+` For restrictions on S3 bucket names, aliases, and Amazon Resource Names (ARNs), refer to [Amazon S3 resource restrictions](esql-data-federation-patterns.md#s3-resource-requirements).

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
stack: preview 9.6+
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

## Enable or disable a dataset

{applies_to}`stack: preview 9.7+` Disable a dataset to stop queries from reading it, for example to control cost, then re-enable it after investigation. Requires `manage_dataset`, `manage`, or `all` on the dataset name. GET responses include an `enabled` field. Creating a dataset enables it, and replacing a dataset keeps its current state.

A disabled dataset behaves as follows:

* A query that names the dataset in `FROM` fails with a `400` error.
* A `FROM` wildcard that matches the dataset, when the `wildcards_match_datasets` query setting is on, skips it and returns a warning. The rest of the query runs.
* A query that is already running may finish, or fail if the node that reads the dataset starts after you disable it.
* You can still get, list, and replace the dataset.

Enabling and disabling are refused until every node in the cluster supports them, for example during a rolling upgrade. Both actions are recorded in the [audit log](esql-data-federation-manage-datasets.md#audit-logging). Enabling or disabling a dataset that is already in that state succeeds and changes nothing.

```console
POST /_query/dataset/access_logs/_disable
```

```console
POST /_query/dataset/access_logs/_enable
```

## Delete a dataset

[`DELETE /_query/dataset/{name}`]({{es-apis}}operation/operation-esql-delete-dataset) deletes a dataset by name.

Deleting a dataset doesn't check whether an [{{esql}} view](/reference/query-languages/esql/esql-views.md) reads from it. Views that read from a deleted dataset fail when queried.

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
