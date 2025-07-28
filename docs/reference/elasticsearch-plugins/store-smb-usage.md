---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/store-smb-usage.html
---

# Working around a bug in Windows SMB and Java on windows [store-smb-usage]

When using a shared file system based on the SMB protocol (like Azure File Service) to store indices, the way Lucene opens index segment files is with a write only flag. This is the *correct* way to open the files, as they will only be used for writes and allows different FS implementations to optimize for it. Sadly, in windows with SMB, this disables the cache manager, causing writes to be slow. This has been described in [LUCENE-6176](https://issues.apache.org/jira/browse/LUCENE-6176), but it affects each and every Java program out there!. This need and must be fixed outside of ES and/or Lucene, either in windows or OpenJDK. For now, we are providing an experimental support to open the files with read flag, but this should be considered experimental and the correct way to fix it is in OpenJDK or Windows.

The Store SMB plugin provides two storage types optimized for SMB:

`smb_mmap_fs`
:   a SMB specific implementation of the default [mmap fs](/reference/elasticsearch/index-settings/store.md#mmapfs)

`smb_simple_fs`
:   :::{admonition} Deprecated in 7.15
    smb_simple_fs is deprecated and will be removed in 8.0. Use smb_nio_fs or other file systems instead.
    :::

`smb_nio_fs`
:   a SMB specific implementation of the default [nio fs](/reference/elasticsearch/index-settings/store.md#niofs)

To use one of these specific storage types, you need to install the Store SMB plugin and restart the node. Then configure Elasticsearch to set the storage type you want.

This can be configured for all indices by adding this to the `elasticsearch.yml` file:

```yaml
index.store.type: smb_nio_fs
```

Note that settings will be applied for newly created indices.

It can also be set on a per-index basis at index creation time:

```console
PUT my-index-000001
{
   "settings": {
       "index.store.type": "smb_mmap_fs"
   }
}
```

