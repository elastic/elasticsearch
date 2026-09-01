---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-store.html
navigation_title: Store
applies_to:
  stack: all
---

# Index store settings [index-modules-store]

:::{include} _snippets/serverless-availability.md
:::

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



$$$direct-io-vector-merge$$$
### Direct I/O for vector merges [direct-io-vector-merge]

`index.store.fs.direct_io.vector_merge` {applies_to}`stack: preview 9.6` {applies_to}`serverless: unavailable`
:   (Static, boolean) Whether the raw vector data of `dense_vector` fields is read and written with direct I/O while merging. Defaults to `false`.

A merge streams the raw vectors of every source segment once and writes the merged result once. Read through the page cache, those bytes displace whatever the cache was holding for searches. With direct I/O the merge reads its sources and writes its output without going through the page cache, so the cache keeps serving what searches actually use.

This is a trade, and the right side of it depends on the node:

* On a node whose index does not fit in RAM, the page cache is under constant pressure and every merge evicts pages that searches then fault back in. Direct I/O merges remove that eviction at no cost to merge throughput; merging is bound by computation, not by reads.
* On a node with RAM to spare, the source segments of a merge are usually still cached from being written, and a page-cache merge reads them for free. Direct I/O merges pay the device for every byte instead, for no benefit, since nothing was going to be evicted. Leave the setting off on such nodes.

The setting is independent of the field-level [`on_disk_rescore`](/reference/elasticsearch/mapping-reference/dense-vector.md#dense-vector-index-options) option, which controls only how vectors are read while rescoring. The two combine freely: rescoring through the page cache with direct I/O merges, or direct I/O rescoring with page-cache merges, are both valid configurations.

Only the `hybridfs` store type supports direct I/O; on other store types the setting has no effect. Where the JDK or filesystem does not support direct I/O, merges fall back to the page cache: a warning is logged when the shard opens if direct I/O cannot be initialized at all, and a file that cannot be opened with direct I/O is opened normally. As a static setting it can be set when an index is created or on a closed index, and it can be set in `elasticsearch.yml` as a node-wide default. It is stored in the index metadata, so snapshots, clones and shrink or split targets carry it with the index; check it when restoring onto nodes with more RAM than the source had.

The setting applies to the raw vector data of every `dense_vector` index type. Segments written by earlier vector formats, before direct I/O support, are read through the page cache when they are merged; the merged segment is written with the current format and is covered from then on. With the scalar-quantized types (`int8_hnsw`, `int4_hnsw`) only the merged output is written with direct I/O today, while their merge reads still go through the page cache.
