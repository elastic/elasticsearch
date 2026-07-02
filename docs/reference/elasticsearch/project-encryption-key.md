---
navigation_title: "Project encryption key"
description: "How Elasticsearch encrypts sensitive values stored in cluster state using a project encryption key, including password requirements, key rotation, and recovery."
applies_to:
  stack: preview 9.5
products:
  - id: elasticsearch
---

# Project encryption key [project-encryption-key]

Some {{es}} features need to store sensitive values, such as credentials for external data sources, in cluster state. The **project encryption key** encrypts this data at rest and in transit between nodes, so that secrets are never persisted or replicated in plain text.

{{es}} generates a single, cluster-wide encryption key automatically. Features that need to persist secrets never handle the key directly. They call an internal encryption service that encrypts and decrypts values on their behalf. The key itself is never exposed through any API.

The first feature to use this mechanism is [ES|QL data source credentials](docs-content://explore-analyze/query-filter/languages/esql.md). Other features may adopt it over time.

## How the key works [project-encryption-key-lifecycle]

{{es}} generates the project encryption key automatically and stores it in cluster state, where it's distributed to every node. It's excluded from cluster state REST responses and from snapshots.

Each node keeps a copy of the key in memory. To survive a restart, a node also persists a copy of the key to local disk, protected by a password so it isn't stored in plain text.

## Set the encryption password [project-encryption-key-password]

To let a node persist the key to disk, it needs a password configured in the {{es}} keystore:

`cluster.state.encryption.password.<id>`
:   A secure setting holding an encryption password, identified by `<id>`.

`cluster.state.encryption.active_password_id`
:   The `<id>` of the password currently used to protect newly written keys.

**On {{ecloud}}, {{ece}}, and {{eck}}**, the control plane supplies this password automatically.

**On self-managed** deployments, the password is normally generated for you: when [security auto-configuration](docs-content://deploy-manage/deploy/self-managed/installing-elasticsearch.md) runs on a node's first start, alongside setting up TLS, it also generates a random password, stores it in the node's keystore as `cluster.state.encryption.password.autoconfigured`, and sets it as the active password ID. You only need to configure a password yourself using [`elasticsearch-keystore`](/reference/elasticsearch/command-line-tools/elasticsearch-keystore.md) if auto-configuration didn't run, for example because it was skipped or the node joined a cluster through a different provisioning path.

### The `required` escape hatch [project-encryption-key-required]

`cluster.state.encryption.required`
:   Whether a password is required before {{es}} will store secrets using the project encryption key. Defaults to `true`.

If no password is configured and `cluster.state.encryption.required` is left at its default (`true`), requests that would store a secret are rejected until a password is available.

Setting `cluster.state.encryption.required` to `false` is **not recommended**: it tells {{es}} to fall back to storing secrets in plain text when no password is configured, and logs a warning each time it does. Only use it for local testing or if you understand and accept that consequence.

## Automatic key rotation [project-encryption-key-rotation]

{{es}} rotates the project encryption key automatically. You can control the schedule with:

`xpack.encryption.key_rotation.interval`
:   How often the key is rotated. Defaults to `30d`. Set to `0` to disable automatic rotation.

`xpack.encryption.key_rotation.check_interval`
:   How often {{es}} checks whether rotation is due. Defaults to `1h`, must be at least `1s`, and can't be greater than `key_rotation.interval`.

Rotation happens in two phases: {{es}} first installs a new key, which is used for all new encryption operations while the previous key remains available for decrypting existing data. {{es}} then re-encrypts existing secrets with the new key in the background. The previous key is retired shortly after all data has been re-encrypted.

There's currently no API to trigger rotation manually.

## Check encryption health [project-encryption-key-health]

The `project_encryption_key` health indicator reports on the status of the project encryption key:

* **Green**: encryption is either working normally, or hasn't been configured (which is expected on most self-managed clusters that haven't set a password).
* **Yellow**: a password is missing where one is required, a node can't persist the key to disk, or {{es}} failed to decrypt the key.

## Reset the project encryption key [project-encryption-key-reset]

:::{warning}
Resetting the project encryption key is **destructive and irreversible**. Any data that was encrypted with the previous key is permanently lost.
:::

As a last resort, for example after suspected key compromise or an unrecoverable encryption state, you can discard the current project encryption key and everything encrypted with it:

```console
POST /_encryption/_reset?accept_data_loss=true
```

The `accept_data_loss=true` query parameter is required. {{es}} generates a new project encryption key on the next write.
