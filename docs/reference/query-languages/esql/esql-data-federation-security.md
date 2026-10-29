---
navigation_title: "Security"
description: "Secure ES|QL Data Federation by controlling access to data sources and datasets, encrypting credentials, and configuring privileges."
applies_to:
  stack: preview 9.6+, experimental =9.5
  serverless: unavailable
products:
  - id: elasticsearch
---

# Manage credentials and privileges for {{esql}} Data Federation

Because federated data lives outside {{es}}, connecting to a private store means {{es}} holds that data source's credentials and must control who can manage connections and read external data. This page covers credential encryption and masking, and the privilege model for data sources and datasets.

## Credentials

When a data source connects to a private bucket, its encrypted credentials are stored in the cluster state. {{es}} protects these credentials with encryption at rest and redaction in API responses.

### Credential encryption

When a data source includes credentials, {{es}} encrypts them using the cluster state encryption key before storing them in the cluster state.

The cluster state encryption key is available automatically in most environments, including {{ech}}, {{ece}}, {{eck}}, and {{serverless-short}}.

By default, if the cluster state encryption key is not available when you create a data source, a `PUT /_query/data_source` request that includes credentials returns a `503` error.

On upgraded self-managed clusters, the encryption password is not configured automatically. To configure a cluster state encryption key, add `cluster.state.encryption.password.<id>` and `cluster.state.encryption.active_password_id` to the keystore on every node, then call `POST /_nodes/reload_secure_settings`.

:::{warning}
Setting `cluster.state.encryption.required: false` lets a data source with credentials be created even when no encryption key is available. The credentials are then stored unencrypted in the cluster state, with only a warning in the logs. Keep the default unless you accept that risk.
:::

Learn more about the [cluster state encryption key](/reference/elasticsearch/cluster-state-encryption-key.md).

### Credential masking

All credential values are replaced by `::es_redacted::` in GET responses. Credentials are never returned in API responses.

A data source's credentials are masked when its definition is read back, and at query time the store is accessed using the data source's stored credentials.

## Privileges

These privileges control what users can do in {{es}}. What a data source can read in external storage is controlled by your cloud provider. Refer to [grant read access to your data](esql-data-federation-sources.md#grant-read-access).

`superuser` has full access to data sources and datasets.

### Dataset privileges [dataset-privileges]

Dataset operations are authorized by the standard {{es}} [index privileges](../../elasticsearch/security-privileges.md#privileges-list-indices), so a role that already administers or reads the matching index names covers datasets with no additional grant. Grant these index privileges on the dataset name:

| Operation | Privilege |
|---|---|
| Query a dataset | `read` |
| Create, read, or delete a dataset | `manage` or `all` |
| Dataset administration granted on its own | `create_dataset`, `read_dataset_metadata`, `delete_dataset`, `manage_dataset` |

The `read` privilege granted on a dataset name must not carry document-level or field-level security. `FROM <dataset>` is rejected if it does. The same restriction applies to [{{esql}} views](esql-views.md#esql-views-privileges).

Creating a dataset that references a data source also requires the `read` [data source privilege](#data-source-privileges) for that data source. The two are authorized independently.

### Data source privileges [data-source-privileges]

Data source operations require either the cluster `manage` (or `all`) privilege, which covers every data source, or fine-grained `global.data_source` privileges on specific data sources. Data source names in `global.data_source` accept wildcard patterns, such as `acme_*`, the same as index names in a role.

Each `global.data_source` privilege allows one operation, and `manage` allows all of them:

| Operation | `global.data_source` privilege |
|---|---|
| Create or replace a data source | `create` |
| Read a data source definition | `read_metadata` |
| Delete a data source | `delete` |
| Reference a data source from a dataset | `read` |
| All of the above | `manage` |
| Test a data source connection | None. Requires the cluster `manage` privilege. |

:::{note}
The {{esql}} Data Federation UI in {{kib}} requires the cluster `manage` privilege. A role with only `global.data_source` privileges can manage data sources with the API, but not in the UI.
:::

#### Grant data source privileges in {{kib}} [grant-data-source-privileges-in-kibana]
```{applies_to}
stack: preview 9.6+
```

You can grant `global.data_source` privileges from the {{kib}} role editor:

1. Find **Roles** in the navigation menu or use the global search field, then create or edit a role.
2. Under **Data source privileges**, click **Add data source privilege**.
3. In **Data sources**, enter the data source names or patterns, such as `acme_*`.
4. In **Privileges**, select the privileges to grant.

The **Data source privileges** section appears with the other {{es}} privileges:

:::{image} images/data-federation/role-editor-data-source-privileges.png
:alt: The Data source privileges section of the Kibana role editor, with fields for data source patterns and privileges
:width: 800px
:::

### Example role [example-role]

The following role grants querying `sales` and `clicks`, dataset administration over `acme_*`, and management of the `acme_*` data sources:

```json
{
  "indices": [
    { "names": ["sales", "clicks"], "privileges": ["read"] },
    { "names": ["acme_*"], "privileges": ["manage"] }
  ],
  "global": {
    "data_source": [
      { "names": ["acme_*"], "privileges": ["manage"] }
    ]
  }
}
```

## Next steps

Continue with the following security and configuration tasks:

- To set up credentials for a data source, refer to [connect with static credentials](esql-data-federation-static-credentials.md) or [connect with federated identity](esql-data-federation-federated-identity.md).
- For the operator-level setting that gates managed identity, refer to the [authentication cluster settings](esql-data-federation-cluster-settings.md#authentication).
- For the full {{es}} privilege reference, refer to [security privileges](../../elasticsearch/security-privileges.md).
