---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-blocks.html
navigation_title: Index block
---

# Index block settings [index-modules-blocks]

Index blocks limit the kind of operations that are available on a certain index. The blocks come in different flavours, allowing to block write, read, or metadata operations. The blocks can be set / removed using dynamic index settings, or can be added using a dedicated API, which also ensures for write blocks that, once successfully returning to the user, all shards of the index are properly accounting for the block, for example that all in-flight writes to an index have been completed after adding the write block.


## Index block settings [index-block-settings]

The following *dynamic* index settings determine the blocks present on an index:

$$$index-blocks-read-only$$$

`index.blocks.read_only`
:   Set to `true` to make the index and index metadata read only, `false` to allow writes and metadata changes.

`index.blocks.read_only_allow_delete`
:   Similar to `index.blocks.write`, except that you can delete the index when this block is in place. Do not set or remove this block yourself. The [disk-based shard allocator](/reference/elasticsearch/configuration-reference/cluster-level-shard-allocation-routing-settings.md#disk-based-shard-allocation) sets and removes this block automatically according to the available disk space.

    Deleting documents from an index to release resources - rather than deleting the index itself - increases the index size temporarily, and therefore may not be possible when nodes are low on disk space. When `index.blocks.read_only_allow_delete` is set to `true`, deleting documents is not permitted. However, deleting the index entirely requires very little extra disk space and frees up the disk space consumed by the index almost immediately so this is still permitted.

    ::::{important}
    {{es}} adds the read-only-allow-delete index block automatically when the disk utilization exceeds the flood stage watermark, and removes this block automatically when the disk utilization falls under the high watermark. See [Disk-based shard allocation](/reference/elasticsearch/configuration-reference/cluster-level-shard-allocation-routing-settings.md#disk-based-shard-allocation) for more information about watermarks, and [Fix watermark errors](docs-content://troubleshoot/elasticsearch/fix-watermark-errors.md) for help with resolving watermark issues.
    ::::


`index.blocks.read`
:   Set to `true` to disable read operations against the index.

$$$index-blocks-write$$$

`index.blocks.write`
:   Set to `true` to disable data write operations against the index. Unlike `read_only`, this setting does not affect metadata. For instance, you can adjust the settings of an index with a `write` block, but you cannot adjust the settings of an index with a `read_only` block.

`index.blocks.metadata`
:   Set to `true` to disable index metadata reads and writes.

