---
navigation_title: "Manage data sources"
description: "Connect, view, edit, and delete ES|QL Data Federation data sources in Kibana or with the Elasticsearch API."
applies_to:
  stack: preview 9.6+, experimental =9.5
  serverless: unavailable
products:
  - id: elasticsearch
---

# Connect and manage data sources for {{esql}} Data Federation

:::{include} _snippets/data-federation/experimental-warning.md
:::

You can manage data sources in the {{kib}} UI or with the {{es}} API.

## Manage data sources in the {{kib}} UI [manage-data-sources-using-the-kibana-ui]

In the {{kib}} UI, you connect and manage data sources from the **Data sources** tab under **Data management** > **{{esql}} Data Federation**.

{applies_to}`stack: experimental =9.5` To use the UI, [enable it in {{kib}}](esql-data-federation.md#enable-the-feature) first.

### View data sources

The **Data sources** tab lists each data source with its type, its description, and the number of datasets that read from it. You can search your data sources and connect new ones. Each row has the following actions:

- **Edit** ({icon}`pencil`): Opens the data source's connection settings.
- **Delete** ({icon}`trash`): Available only when no datasets read from the data source.

In the following example, `ookla_speedtest` can't be deleted because a dataset reads from it:

:::{image} images/data-federation/data-sources-tab.png
:alt: The Data sources tab listing two Amazon S3 data sources, with the delete action unavailable for ookla_speedtest because one dataset reads from it
:width: 800px
:::

### Connect a new data source [connect-a-new-data-source]

To connect a data source:

1. Click **Connect data source** to open the **Connect external data source** flyout.
2. Enter a **Name** and an optional **Description**.
3. In **Preferred method**, choose how {{es}} [authenticates](esql-data-federation-sources.md#authentication) to your storage:
   - Your bucket is public: select **Anonymous**. No credentials are stored.
   - Your bucket is private and your deployment runs on {{ech}} on AWS, Azure, or Google Cloud: select **Federated Identity**. It's the recommended method because no keys are stored, and it's the default when it's available. Refer to [connect with federated identity](esql-data-federation-federated-identity.md).
   - Your bucket is private and federated identity isn't available: select **Access and Secret Keys**, then enter the key pair. Refer to [connect with static credentials](esql-data-federation-static-credentials.md).
4. Click **Connect**.

:::{note}
Some connection options are available only with the [API](#manage-data-sources-using-the-api):

- A custom Amazon S3 [`endpoint`](esql-data-federation-data-source-settings.md#endpoint)
- [Managed identity](esql-data-federation-sources.md#authentication) authentication
:::

#### Check the connection

Connecting a data source doesn't test whether it can read your files. To check the connection, [create a dataset](esql-data-federation-manage-datasets.md) that reads from it and query the dataset.

<!--
{applies_to}`stack: preview 9.7+` You can also check the connection with the [test a connection](#test-a-connection) API.
-->

### Edit a data source

To change a data source:

1. Click **Edit** ({icon}`pencil`) in the data source's row.
2. Update its settings. You can't change the data source type or name.
3. Click **Save**.

Credential fields that you leave unchanged keep their stored values. When credentials rotate, update them here. Datasets that read from the data source use the new credentials without any changes.

### Delete a data source [delete-a-data-source-in-the-ui]

A data source can't be deleted while datasets read from it. To delete a data source:

1. [Delete the datasets](esql-data-federation-manage-datasets.md) that read from it.
2. Click **Delete** ({icon}`trash`) in the data source's row.

To delete several unused data sources at once, select them, then click **Delete**.

## Manage data sources with the {{es}} API [manage-data-sources-using-the-api]

Data sources are managed under the `/_query/data_source` endpoint. All data source operations require the cluster `manage` privilege or a `global.data_source` privilege. Refer to [manage credentials and privileges](esql-data-federation-security.md) for details.

| Operation | Endpoint | API reference |
|---|---|---|
| [Create or update](#create-or-update-a-data-source) | `PUT /_query/data_source/{name}` | [Create or update an ES\|QL data source]({{es-apis}}operation/operation-esql-put-data-source) |
| [Get](#get-a-data-source) | `GET /_query/data_source/{name}` | [Get ES\|QL data sources]({{es-apis}}operation/operation-esql-get-data-source) |
| [List all](#list-all-data-sources) | `GET /_query/data_source` | [Get ES\|QL data sources]({{es-apis}}operation/operation-esql-get-data-source) |
| [Delete](#delete-a-data-source) | `DELETE /_query/data_source/{name}` | [Delete ES\|QL data sources]({{es-apis}}operation/operation-esql-delete-data-source) |

This section is a quick reference for each operation. For the complete request and response details, follow the API reference links in the table.

<!-- Restore this table row when the test connection API is released:
| [Test connection](#test-a-connection) {applies_to}`stack: preview 9.7+` | `POST /_query/data_source/_test` | [Test an ES\|QL data source connection]({{es-apis}}operation/operation-esql-data-source-test-connection) |
-->

### Create or update a data source [create-or-update-a-data-source]

`PUT` creates a new data source or replaces an existing one entirely with one exception. Secrets you omit from the request are carried forward from the existing definition rather than cleared, so you can update non-secret settings without re-sending credentials.

The create request does not validate connectivity to the external system.

<!--
{applies_to}`stack: preview 9.7+` To verify that credentials and endpoint are correct before saving, use the [test connection](#test-a-connection) endpoint.
-->

::::{tab-set}
:group: api-ref

:::{tab-item} Console
:sync: console
```console
PUT /_query/data_source/prod_s3_logs
{
  "type": "s3",
  "description": "Production S3 logs bucket",
  "settings": {
    "auth": "static_credentials",
    "access_key": "<AWS_ACCESS_KEY_ID>",
    "secret_key": "<AWS_SECRET_ACCESS_KEY>"
  }
}
```
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X PUT "${ELASTICSEARCH_URL}/_query/data_source/prod_s3_logs" \
  -H "Authorization: ApiKey ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
  "type": "s3",
  "description": "Production S3 logs bucket",
  "settings": {
    "auth": "static_credentials",
    "access_key": "<AWS_ACCESS_KEY_ID>",
    "secret_key": "<AWS_SECRET_ACCESS_KEY>"
  }
}'
```
:::

::::

:::{tip}
For step-by-step guides on setting up each authentication model in AWS, refer to the [Amazon S3 authentication models](esql-data-federation-s3.md#s3-auth-models).
:::

#### Naming rules and limits

A create or update request is rejected when it breaks one of these rules or limits:

- **Name**: Follows the same rules as index names. Lowercase only and at most 255 bytes. Can't begin with `-`, `_`, or `+`, and can't contain spaces or the characters `\ / * ? " < > |`.
- **Description**: At most 1,000 characters.
- **Number of data sources**: At most 100 per cluster by default. In {{stack}} deployments, you can raise the limit with the [`esql.data_sources.max_count`](esql-data-federation-cluster-settings.md#object-limits) cluster setting.

### Get a data source [get-a-data-source]

Retrieves a data source by name. You can pass a comma-separated list of names and use `*` wildcards. A concrete name that does not exist returns a `404`; a wildcard that matches nothing returns an empty list. Credential values are replaced by `::es_redacted::` in the response.

::::{tab-set}
:group: api-ref

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

### List all data sources [list-all-data-sources]

Returns all registered data sources.

::::{tab-set}
:group: api-ref

:::{tab-item} Console
:sync: console
```console
GET /_query/data_source
```
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X GET "${ELASTICSEARCH_URL}/_query/data_source" \
  -H "Authorization: ApiKey ${API_KEY}"
```
:::

::::

### Delete a data source [delete-a-data-source]

Deletes one or more data sources by name. You can pass a comma-separated list. If any named data source does not exist, the request returns a `404` and nothing is deleted.

::::{tab-set}
:group: api-ref

:::{tab-item} Console
:sync: console
```console
DELETE /_query/data_source/prod_s3_logs
```
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X DELETE "${ELASTICSEARCH_URL}/_query/data_source/prod_s3_logs" \
  -H "Authorization: ApiKey ${API_KEY}"
```
:::

::::

:::{important}
A data source cannot be deleted while datasets still reference it. Delete the dependent datasets first, or the request returns a `409 Conflict` error.
:::

<!-- The test connection API is behind a feature flag that is off in release builds.

### Test a connection [test-a-connection]
```{applies_to}
stack: preview 9.7+
```

Use `POST /_query/data_source/_test` to verify that a configuration can reach its backend before saving it. The data source does not need to exist in cluster state — the endpoint accepts the same `type` and `settings` fields as the `PUT` request.

The response contains a `status` field with one of three values:

| Status | Meaning |
|---|---|
| `success` | The probe ran on every eligible node and the backend is reachable with the given credentials. |
| `failure` | The probe ran but the backend was unreachable or rejected the credentials. An `error` field contains a human-readable reason. |
| `untestable` | The configuration cannot be verified at the data source level. An optional `message` field, when present, explains why. Common reasons: the backend type has no connectivity probe; credentials are scoped to a container or bucket rather than the account; authentication uses anonymous or managed-identity access that requires a dataset path to resolve the endpoint; or no eligible node could complete the probe. The configuration may still be correct — create a dataset and run a query to validate access. |

The probe runs on every eligible node in parallel, with a 30-second timeout per node. If any node cannot complete the probe, the result is `untestable` rather than `success`.

::::{tab-set}
:group: api-ref

:::{tab-item} Console
:sync: console
```console
POST /_query/data_source/_test
{
  "type": "s3",
  "settings": {
    "auth": "static_credentials",
    "access_key": "<AWS_ACCESS_KEY_ID>",
    "secret_key": "<AWS_SECRET_ACCESS_KEY>"
  }
}
```
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X POST "${ELASTICSEARCH_URL}/_query/data_source/_test" \
  -H "Authorization: ApiKey ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
  "type": "s3",
  "settings": {
    "auth": "static_credentials",
    "access_key": "<AWS_ACCESS_KEY_ID>",
    "secret_key": "<AWS_SECRET_ACCESS_KEY>"
  }
}'
```
:::

::::

Example responses:

```json
{ "status": "success" }
```

```json
{ "status": "failure", "error": "The AWS Access Key Id you provided does not exist in our records." }
```

```json
{ "status": "untestable", "message": "Anonymous access targets public buckets; create a dataset to validate read access." }
```

:::{note}
An unknown `type` value returns a `400 Bad Request` rather than a `failure` status, because the type is not registered — it is a client error, not a connectivity problem.
:::
-->

## Next steps

:::{include} _snippets/data-federation/data-sources-next-steps.md
:::
