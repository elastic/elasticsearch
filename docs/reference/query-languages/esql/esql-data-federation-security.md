---
navigation_title: "Manage access"
description: "Secure ES|QL Data Federation by controlling access to data sources and datasets, encrypting credentials, and configuring privileges."
applies_to:
  stack: experimental =9.5
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

Dataset operations are authorized by the standard {{es}} [index privileges](../../elasticsearch/security-privileges.md#privileges-list-indices), so a role that already administers or reads the matching index names covers datasets with no additional grant.

| Operation | Privilege | Type |
|---|---|---|
| Query a dataset | `read` | Index, on the dataset name |
| Create, read, or delete a dataset | `manage` or `all` | Index, on the dataset name |
| Dataset administration granted on its own | `create_dataset`, `read_dataset_metadata`, `delete_dataset`, `manage_dataset` | Index, on the dataset name |
| Create or replace a data source | `global.data_source` `create` / `cluster.manage` | Global (fine-grained) / Cluster |
| Read a data source definition | `global.data_source` `read_metadata` / `cluster.manage` | Global (fine-grained) / Cluster |
| Delete a data source | `global.data_source` `delete` / `cluster.manage` | Global (fine-grained) / Cluster |
| Reference a data source from a dataset | `global.data_source` `read` / `cluster.manage` | Global (fine-grained) / Cluster |
| All data source operations | `global.data_source` `manage` / `cluster.manage` | Global (fine-grained) / Cluster |

Creating a dataset that references a data source also requires the `read` data source privilege for that data source. The two are authorized independently.

The `read` privilege granted on a dataset name must not carry document-level or field-level security. `FROM <dataset>` is rejected if it does. The same restriction applies to [{{esql}} views](esql-views.md).

`superuser` has full access to data sources and datasets. Data source management is reached through the cluster `manage` (or `all`) privilege, or through a role explicitly granted `global.data_source` for fine-grained control.

A role configures these privileges as follows. The example grants querying `sales` and `clicks`, dataset administration over `acme_*`, and management of the `acme_*` data sources:

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

- To set up credentials for a data source, refer to [connect with static credentials](esql-data-federation-static-credentials.md) or [connect with federated identity](esql-data-federation-federated-identity.md).
- For the operator-level settings that gate managed identity and federated identity, refer to the [authentication cluster settings](esql-data-federation-cluster-settings.md#authentication).
- For the full {{es}} privilege reference, refer to [security privileges](../../elasticsearch/security-privileges.md).
