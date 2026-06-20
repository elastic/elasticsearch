---
navigation_title: Source-only repository
applies_to:
  stack: all
---

# Source-only repository settings [repository-source-settings]

You can use a `source` repository to take [source-only snapshots](docs-content://deploy-manage/tools/snapshot-and-restore/source-only-repository.md) that delegate storage to another registered repository type. This page lists settings that apply to the `source` repository type when you create or update it via the [Create or update a snapshot repository](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-snapshot-create-repository) API.

## Repository settings [repository-source-repository-settings]

The `source` repository type supports a list of settings. You supply them in the `settings` object when you register or update the repository.

For example, the following request registers `my_src_only_repository` with the following settings:

```console
PUT _snapshot/my_src_only_repository
{
  "type": "source", <1>
  "settings": {
    "delegate_type": "fs", <2>
    "location": "my_backup_repository" <3>
  }
}
```

1. In `source` repository types, snapshots are source-only and {{es}} writes stored fields and related metadata through a delegated repository instead of snapshotting full index data.
2. `delegate_type` selects the inner repository implementation. `fs` means snapshot blobs are stored with the shared filesystem repository type.
3. `location` belongs to the delegated `fs` repository (not a separate `source`-only key). It is the same `location` setting as described in [Shared file system repository settings](/reference/elasticsearch/configuration-reference/fs-repository-settings.md). You can add other keys from the delegated type’s reference in the same `settings` object, plus `source`-level options listed below (for example `compress` or throttling).

The following settings are supported:

`chunk_size`
:   (Optional, [byte value](/reference/elasticsearch/rest-apis/api-conventions.md#byte-units)) Maximum size of files in snapshots. In snapshots, files larger than this are broken down into chunks of this size or smaller. Defaults to `null` (unlimited file size).

`compress`
:   (Optional, Boolean) If `true`, metadata files, such as index mappings and settings, are compressed in snapshots. Data files are not compressed. Defaults to `true`.

`delegate_type`
:   (Optional, string) Delegated repository type. For valid values, see the [`type` parameter](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-snapshot-create-repository#put-snapshot-repo-api-request-type). This must be set when you create a `source` repository.

    `source` repositories can use `settings` properties for their delegated repository type.

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
