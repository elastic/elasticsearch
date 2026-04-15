---
navigation_title: Read-only URL repository
applies_to:
  deployment:
    self: ga
    eck: ga
---

# Read-only URL repository settings [repository-url-settings]

You can use a URL repository to give a cluster read-only access to snapshot data exposed at a URL. You specify settings for the `url` repository type when creating or updating a repository via the [Create or update a snapshot repository](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-snapshot-create-repository) API.

For setup and examples, refer to [Read-only URL repository](docs-content://deploy-manage/tools/snapshot-and-restore/read-only-url-repository.md).

There are two categories of settings:

- [{{es}} settings](#repository-url-node-settings) that apply to the URL repository plugin. You configure them in [`elasticsearch.yml`](docs-content://deploy-manage/stack-settings.md) on each node.
- [Repository settings](#repository-url-repository-settings) control per-repository behavior. You pass them in the `settings` object when you create or update a repository.

## {{es}} settings [repository-url-node-settings]

$$$repositories-url-allowed$$$

`repositories.url.allowed_urls` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Specifies the [read-only URL repositories](docs-content://deploy-manage/tools/snapshot-and-restore/read-only-url-repository.md) that snapshots can be restored from. The default value is an empty list.

## Repository settings [repository-url-repository-settings]

The `url` repository type supports a list of settings. You supply them in the `settings` object when you register or update the repository.
For example, the following request registers `my_read_only_url_repository`:

```console
PUT _snapshot/my_read_only_url_repository
{
  "type": "url",
  "settings": {
    "url": "file:/mount/backups/my_fs_backup_location"
  }
}
```

The following settings are supported:

`chunk_size`
:   (Optional, [byte value](/reference/elasticsearch/rest-apis/api-conventions.md#byte-units)) Maximum size of files in snapshots. In snapshots, files larger than this are broken down into chunks of this size or smaller. Defaults to `null` (unlimited file size).

`http_max_retries`
:   (Optional, integer) Maximum number of retries for `http` and `https` URLs. Defaults to `5`.

`http_socket_timeout`
:   (Optional, [time value](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) Maximum wait time for data transfers over a connection. Defaults to `50s`.

`compress`
:   (Optional, Boolean) If `true`, metadata files, such as index mappings and settings, are compressed in snapshots. Data files are not compressed. Defaults to `true`.

`max_number_of_snapshots`
:   (Optional, integer) Maximum number of snapshots the repository can contain. Defaults to `Integer.MAX_VALUE`, which is `2`^`31`^`-1` or `2147483647`.

`max_restore_bytes_per_sec`
:   (Optional, [byte value](/reference/elasticsearch/rest-apis/api-conventions.md#byte-units)) Maximum snapshot restore rate per node. Defaults to unlimited. Note that restores are also throttled through [recovery settings](/reference/elasticsearch/configuration-reference/index-recovery-settings.md).

`max_snapshot_bytes_per_sec`
:   (Optional, [byte value](/reference/elasticsearch/rest-apis/api-conventions.md#byte-units)) Maximum snapshot creation rate per node. Defaults to `40mb` per second. Note that if the [recovery settings for managed services](/reference/elasticsearch/configuration-reference/index-recovery-settings.md#recovery-settings-for-managed-services) are set, then it defaults to unlimited, and the rate is additionally throttled through [recovery settings](/reference/elasticsearch/configuration-reference/index-recovery-settings.md).

`url`
:   (Required, string) URL location of the root of the shared filesystem repository. The following protocols are supported:

    * `file`
    * `ftp`
    * `http`
    * `https`
    * `jar`

    URLs using the `http`, `https`, or `ftp` protocols must be explicitly allowed with the [`repositories.url.allowed_urls`](#repositories-url-allowed) node setting. This setting supports wildcards in the place of a host, path, query, or fragment in the URL.

    URLs using the `file` protocol must point to the location of a shared filesystem accessible to all master and data nodes in the cluster. This location must be registered in the `path.repo` setting, in the same way as when configuring a [shared file system repository](docs-content://deploy-manage/tools/snapshot-and-restore/shared-file-system-repository.md), and it must contain the snapshot data. You don't need to set `path.repo` when using URLs with the `ftp`, `http`, `https`, or `jar` protocols.
