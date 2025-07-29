---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-store.html
navigation_title: Store
---

# Index store settings [index-modules-store]

The store module allows you to control how index data is stored and accessed on disk.

::::{note}
This is a low-level setting. Some store implementations have poor concurrency or disable optimizations for heap memory usage. We recommend sticking to the defaults.
::::



## File system storage types [file-system]

There are different file system implementations or *storage types*. By default, Elasticsearch will pick the best implementation based on the operating environment.

The storage type can also be explicitly set for all indices by configuring the store type in the `config/elasticsearch.yml` file:

```yaml
index.store.type: hybridfs
```

It is a *static* setting that can be set on a per-index basis at index creation time:

```console
PUT /my-index-000001
{
  "settings": {
    "index.store.type": "hybridfs"
  }
}
```

::::{warning}
This is an expert-only setting and may be removed in the future.
::::


The following sections lists all the different storage types supported.

`fs`
:   Default file system implementation. This will pick the best implementation depending on the operating environment, which is currently `hybridfs` on all supported systems but is subject to change.

$$$simplefs$$$`simplefs`
:   :::{admonition} Deprecated in 7.15
    simplefs is deprecated and will be removed in 8.0. Use niofs or other file systems instead. Elasticsearch 7.15 or later uses niofs for the simplefs store type as it offers superior or equivalent performance to simplefs.
    :::

    The Simple FS type is a straightforward implementation of file system storage (maps to Lucene `SimpleFsDirectory`) using a random access file. This implementation has poor concurrent performance (multiple threads will bottleneck) and disables some optimizations for heap memory usage.

$$$niofs$$$`niofs`
:   The NIO FS type stores the shard index on the file system (maps to Lucene `NIOFSDirectory`) using NIO. It allows multiple threads to read from the same file concurrently. It is not recommended on Windows because of a bug in the SUN Java implementation and disables some optimizations for heap memory usage.

$$$mmapfs$$$`mmapfs`
:   The MMap FS type stores the shard index on the file system (maps to Lucene `MMapDirectory`) by mapping a file into memory (mmap). Memory mapping uses up a portion of the virtual memory address space in your process equal to the size of the file being mapped. Before using this class, be sure you have allowed plenty of [virtual address space](docs-content://deploy-manage/deploy/self-managed/vm-max-map-count.md).

$$$hybridfs$$$`hybridfs`
:   The `hybridfs` type is a hybrid of `niofs` and `mmapfs`, which chooses the best file system type for each type of file based on the read access pattern. Currently only the Lucene term dictionary, norms and doc values files are memory mapped. All other files are opened using Lucene `NIOFSDirectory`. Similarly to `mmapfs` be sure you have allowed plenty of [virtual address space](docs-content://deploy-manage/deploy/self-managed/vm-max-map-count.md).

$$$allow-mmap$$$
You can restrict the use of the `mmapfs` and the related `hybridfs` store type via the setting `node.store.allow_mmap`. This is a boolean setting indicating whether or not memory-mapping is allowed. The default is to allow it. This setting is useful, for example, if you are in an environment where you can not control the ability to create a lot of memory maps so you need disable the ability to use memory-mapping.


