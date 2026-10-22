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
[define a dataset](esql-data-federation-datasets.md#define-a-dataset). Creating a dataset doesn't check that the data source can read its resource, so also [grant read access](esql-data-federation-sources.md#grant-read-access) to the data in your cloud provider.

:::{include} _snippets/data-federation/experimental-warning.md
:::

## Manage datasets in the UI

In the {{kib}} UI, you create and manage datasets from the **Datasets** tab under **Data management** > **{{esql}} Data Federation**.

The **Datasets** tab lists each dataset with its data source, data source type, resource, and description. You can search your datasets, filter them by data source, and add new ones. Each row has the following actions:

- **Open in Discover** ({icon}`product_discover`): Opens **Discover** with a `FROM <dataset>` query.
- **Edit** and **Delete**: Click **More actions** ({icon}`ellipsis`) at the end of the row, then select an action.

The following example shows two datasets that read through the same data source:

![The Datasets tab listing two datasets that read from the ookla_speedtest data source](images/data-federation/datasets-tab.png)

### Add a new dataset

Click **Add dataset** to open a four-step wizard. A progress bar at the top of the page shows each step:

![The progress bar of the Add dataset wizard, showing the Define dataset, Optional settings, Schema mappings, and Review steps, with Define dataset active](images/data-federation/add-dataset-steps.png)

After you complete a step, you can return to it by clicking it in the progress bar.

#### Define dataset

The first step sets what the dataset reads:

- **Data source**: The connected data source to read through. To connect a new one without leaving the wizard, select **Connect new data source**.
- **Dataset name**: A unique name for use in queries. Names must be lowercase and cannot begin with `-`, `_`, or `+`. A dataset cannot share a name with any existing index, data stream, alias, or view.
- **Description (optional)**: Up to 1,000 characters.
- **Resource**: The URI and glob pattern that selects the files to read. Refer to [resource patterns](esql-data-federation-patterns.md) for the pattern language.
- **Format**: Detected from the file extension in **Resource**. Select a format yourself when the pattern has no extension or matches more than one format. Detection never replaces a format you select. Refer to [supported file formats](esql-data-federation-file-formats.md).

:::{note}
Creating a dataset doesn't test whether the data source can read the files. Before you query the dataset, [grant read access](esql-data-federation-sources.md#grant-read-access) in your cloud provider.
:::

#### Optional settings

The second step controls how the files are read. If your files match the defaults for their format, skip it. Hidden files, temporary files, and Delta Lake logs are already excluded, and Hive-style folder paths such as `year=2024/` are read as columns automatically.

Change settings in these cases:

- Your CSV or TSV files don't use the default layout:
  - For a separator other than a comma, set **Delimiter**.
  - For files without a header row, turn off **Header row**. Columns are then named by position, such as `col0` and `col1`.
  - For files that mark missing values with a string such as `NA`, set **Null value**.
- Your dates aren't in ISO 8601 format, in CSV, TSV, or NDJSON files: set **Date and time format**.
- Your folder paths encode values in a pattern other than `key=value`: set **Partition detection** to **Template** and describe the pattern.
- Some rows are malformed: by default, one bad row fails the query. To skip bad rows or read their bad fields as null, change **Error mode**.

To learn what each setting does, refer to [dataset settings](esql-data-federation-dataset-settings.md).

#### Schema mappings

The third step sets the dataset's schema. To learn how declared and inferred columns combine, refer to [schema inference](esql-data-federation-schema.md#declare-a-schema-explicitly).

- **Enable time-based filtering**: On by default. In **Field name**, enter the date column to map to `@timestamp`, so queries and dashboards can filter the dataset by time.
- **Infer unmapped fields**: The default. Fields you don't map are inferred from the files. To choose how schemas from different files are combined, select **Configure schema resolution**.
- **Use mapped fields only**: Only the fields you map are available to query. You must map at least one field other than `@timestamp`.
- **Add field**: Maps a column. Set its **Field type** and the **Field name** to use in queries. If the column has a different name in your files, enter that name in **Original field name**.

:::{tip}
If your files have no date column, turn off **Enable time-based filtering**. Otherwise, you can't move to the next step without entering a **Field name**.
:::

The following example maps two columns and infers the rest:

- The `timestamp` column is mapped to `@timestamp`.
- The **Add field** form declares `status_code` as a **Keyword** field. Because **Infer unmapped fields** is selected, this overrides the type inferred for `status_code`.
- Every other column is still inferred from the files.

![The Schema mappings step with time-based filtering mapped to a timestamp column and the Add field form open for a status_code keyword field](images/data-federation/add-dataset-schema-mappings-add-field.png)

#### Review

The last step summarizes the dataset on the **Summary** tab. The **Request** tab shows the equivalent create request, which you can copy to create the same dataset with the API:

![The Request tab on the Review step, showing the PUT request that creates the speedtest_fixed dataset](images/data-federation/add-dataset-review-request.png)

Click **Add dataset** to create the dataset.

### Edit a dataset

To change a dataset, click **More actions** ({icon}`ellipsis`) at the end of its row, then select **Edit**. The wizard opens with the dataset's current configuration. Click **Save** on the **Review** step to apply your changes.

:::{warning}
Changing the **Dataset name** saves the dataset under the new name and then deletes the dataset with the old name. Queries that use the old name stop working.
:::

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

## Audit logging

With [audit logging enabled](docs-content://deploy-manage/security/logging-configuration/enabling-audit-logs.md),
creating, updating, and deleting datasets generates `security_config_change` events. Refer to
[put_dataset](../../elasticsearch/elasticsearch-audit-events.md#event-put-dataset) and
[delete_dataset](../../elasticsearch/elasticsearch-audit-events.md#event-delete-dataset).

## Next steps

After creating a dataset, [query it with {{esql}}](esql-data-federation-querying.md). To change how the dataset selects or interprets files, return to [define a dataset](esql-data-federation-datasets.md#define-a-dataset).
