---
navigation_title: GCS repository
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/repository-gcs.html
applies_to:
  stack: all
---

# Google Cloud Storage repository settings [repository-gcs-settings]

You can use [Google Cloud Storage](https://cloud.google.com/storage/) as a repository for [Snapshot and restore](docs-content://deploy-manage/tools/snapshot-and-restore.md). This page lists the settings you can use to configure how {{es}} connects to GCS and how it stores data in your repository.

There are two categories of settings:

- **[Client settings](#repository-gcs-client-settings)** control how {{es}} connects to Google Cloud Storage, including authentication credentials, endpoints, proxies, and timeouts. These are defined per client in `elasticsearch.yml` or the {{es}} keystore, and can be shared across multiple repositories.
- **[Repository settings](#repository-gcs-repository-settings)** control per-repository behavior such as the target bucket, chunk size, and throttling. These are specified when creating or updating a repository via the API.

For step-by-step setup instructions, refer to [Google Cloud Storage repository](docs-content://deploy-manage/tools/snapshot-and-restore/google-cloud-storage-repository.md).

## Client settings [repository-gcs-client-settings]

The client used to connect to Google Cloud Storage has a number of available settings. Client setting names are of the form `gcs.client.CLIENT_NAME.SETTING_NAME` and are specified inside [`elasticsearch.yml`](docs-content://deploy-manage/stack-settings.md). The default client name looked up by a `gcs` repository is called `default`, but can be customized with the repository setting `client`.

For example:

```console
PUT _snapshot/my_gcs_repository
{
  "type": "gcs",
  "settings": {
    "bucket": "my_bucket",
    "client": "my_alternate_client"
  }
}
```

Some settings are sensitive and must be stored in the [{{es}} keystore](docs-content://deploy-manage/security/secure-settings.md). This is the case for the service account file:

```sh
bin/elasticsearch-keystore add-file gcs.client.default.credentials_file /path/service-account.json
```

The following are the available client settings. Those that must be stored in the keystore are marked as `Secure`.

`gcs.client.CLIENT_NAME.credentials_file` ([Secure](docs-content://deploy-manage/security/secure-settings.md), [reloadable](docs-content://deploy-manage/security/secure-settings.md#reloadable-secure-settings))
:   The service account file that is used to authenticate to the Google Cloud Storage service.

`gcs.client.CLIENT_NAME.endpoint`
:   The Google Cloud Storage service endpoint to connect to. This will be automatically determined by the Google Cloud Storage client but can be specified explicitly.

`gcs.client.CLIENT_NAME.connect_timeout`
:   The timeout to establish a connection to the Google Cloud Storage service. The value should specify the unit. For example, a value of `5s` specifies a 5 second timeout. The value of `-1` corresponds to an infinite timeout. The default value is 20 seconds.

`gcs.client.CLIENT_NAME.read_timeout`
:   The timeout to read data from an established connection. The value should specify the unit. For example, a value of `5s` specifies a 5 second timeout. The value of `-1` corresponds to an infinite timeout. The default value is 20 seconds.

`gcs.client.CLIENT_NAME.application_name`
:   Name used by the client when it uses the Google Cloud Storage service. Setting a custom name can be useful to authenticate your cluster when requests statistics are logged in the Google Cloud Platform. Default to `repository-gcs`.

`gcs.client.CLIENT_NAME.project_id`
:   The Google Cloud project id. This will be automatically inferred from the credentials file but can be specified explicitly. For example, it can be used to switch between projects when the same credentials are usable for both the production and the development projects.

`gcs.client.CLIENT_NAME.proxy.host`
:   Host name of a proxy to connect to the Google Cloud Storage through.

`gcs.client.CLIENT_NAME.proxy.port`
:   Port of a proxy to connect to the Google Cloud Storage through.

`gcs.client.CLIENT_NAME.proxy.type`
:   Proxy type for the client. Supported values are `direct` (no proxy), `http`, and `socks`. Defaults to `direct`.


## Repository settings [repository-gcs-repository-settings]

The `gcs` repository type supports a number of settings to customize how data is stored in Google Cloud Storage. These can be specified when creating the repository. For example:

```console
PUT _snapshot/my_gcs_repository
{
  "type": "gcs",
  "settings": {
    "bucket": "my_other_bucket",
    "base_path": "dev"
  }
}
```

The following settings are supported:

`bucket`
:   (Required) The name of the bucket to be used for snapshots.

`client`
:   The name of the client to use to connect to Google Cloud Storage. Defaults to `default`.

`base_path`
:   Specifies the path within bucket to repository data. Defaults to the root of the bucket.

    ::::{note}
    Don't set `base_path` when configuring a snapshot repository for {{ECE}}. {{ECE}} automatically generates the `base_path` for each deployment so that multiple deployments may share the same bucket.
    ::::


`chunk_size`
:   Big files can be broken down into multiple smaller blobs in the blob store during snapshotting. It is not recommended to change this value from its default unless there is an explicit reason for limiting the size of blobs in the repository. Setting a value lower than the default can result in an increased number of API calls to the Google Cloud Storage Service during snapshot create as well as restore operations compared to using the default value and thus make both operations slower as well as more costly. Specify the chunk size as a value and unit, for example: `10MB`, `5KB`, `500B`. Defaults to the maximum size of a blob in the Google Cloud Storage Service which is `5TB`.

`compress`
:   When set to `true` metadata files are stored in compressed format. This setting doesn't affect index files that are already compressed by default. Defaults to `true`.

`max_restore_bytes_per_sec`
:   (Optional, [byte value](/reference/elasticsearch/rest-apis/api-conventions.md#byte-units)) Maximum snapshot restore rate per node. Defaults to unlimited. Note that restores are also throttled through [recovery settings](/reference/elasticsearch/configuration-reference/index-recovery-settings.md).

`max_snapshot_bytes_per_sec`
:   (Optional, [byte value](/reference/elasticsearch/rest-apis/api-conventions.md#byte-units)) Maximum snapshot creation rate per node. Defaults to `40mb` per second. Note that if the [recovery settings for managed services](/reference/elasticsearch/configuration-reference/index-recovery-settings.md#recovery-settings-for-managed-services) are set, then it defaults to unlimited, and the rate is additionally throttled through [recovery settings](/reference/elasticsearch/configuration-reference/index-recovery-settings.md).

`readonly`
:   (Optional, Boolean) If `true`, the repository is read-only. The cluster can retrieve and restore snapshots from the repository but not write to the repository or create snapshots in it.

    Only a cluster with write access can create snapshots in the repository. All other clusters connected to the repository should have the `readonly` parameter set to `true`.

    If `false`, the cluster can write to the repository and create snapshots in it. Defaults to `false`.

    ::::{important}
    If you register the same snapshot repository with multiple clusters, only one cluster should have write access to the repository. Having multiple clusters write to the repository at the same time risks corrupting the contents of the repository.

    ::::


`application_name`
:   :::{admonition} Deprecated in 6.3.0
    This setting was deprecated in 6.3.0.
    :::

    Name used by the client when it uses the Google Cloud Storage service.
