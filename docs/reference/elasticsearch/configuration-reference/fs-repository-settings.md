---
navigation_title: Shared file system repository
applies_to:
  deployment:
    self:
    eck:
---

# Shared file system repository settings [repository-fs-settings]

You can use a shared file system as a repository for [Snapshot and restore](docs-content://deploy-manage/tools/snapshot-and-restore.md). You register repositories via the [Create or update a snapshot repository](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-snapshot-create-repository) API.

There are two categories of settings:

- [{{es}} settings](#repository-fs-node-settings) control which filesystem paths `fs` repositories can use. You configure them in [`elasticsearch.yml`](docs-content://deploy-manage/stack-settings.md) on each node.
- [Repository settings](#repository-fs-repository-settings) control each registered repository. You pass them in the `settings` object when you create or update a repository.

## {{es}} settings [repository-fs-node-settings]

$$$path-repo$$$

`path.repo`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) List of filesystem paths that {{es}} can use for `fs` snapshot repositories. Each repository `location` must resolve to a path under one of these entries on every master and data node. Defaults to an empty list.

    To register a shared filesystem repository, mount the storage at the same location on each node and add that path (or a parent directory) to `path.repo`. For step-by-step setup, mount paths, and platform-specific examples, refer to [Shared file system repository](docs-content://deploy-manage/tools/snapshot-and-restore/shared-file-system-repository.md).

## Repository settings [repository-fs-repository-settings]

The `fs` repository type supports a list of repository settings. You supply them in the `settings` object when you register or update the repository.

For example, the following request uses the API to register a shared file system repository named `my_fs_backup`: 

```console
PUT _snapshot/my_fs_backup
{
  "type": "fs",
  "settings": {
    "location": "/mount/backups/my_fs_backup_location"
  }
}
```


The following settings are supported:

`chunk_size`
:   (Optional, [byte value](/reference/elasticsearch/rest-apis/api-conventions.md#byte-units)) Maximum size of files in snapshots. In snapshots, files larger than this are broken down into chunks of this size or smaller. Defaults to `null` (unlimited file size).

`compress`
:   (Optional, Boolean) If `true`, metadata files, such as index mappings and settings, are compressed in snapshots. Data files are not compressed. Defaults to `true`.

`location`
:   (Required, string) Location of the shared filesystem used to store and retrieve snapshots. This location must fall under a path allowed by [`path.repo`](#path-repo) on every master and data node. Unlike `path.repo`, the repository `location` setting is a single path.

`max_number_of_snapshots`
:   (Optional, integer) Maximum number of snapshots the repository can contain. Defaults to `Integer.MAX_VALUE`, which is `2`^`31`^`-1` or `2147483647`.

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
