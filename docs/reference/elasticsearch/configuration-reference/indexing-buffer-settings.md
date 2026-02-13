---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/indexing-buffer.html
applies_to:
  deployment:
    self:
---

# Indexing buffer settings [indexing-buffer]

The indexing buffer is used to store newly indexed documents. When it fills up, the documents in the buffer are written to a segment on disk. It is divided between all shards on the node.

The following settings are *static* and must be configured on every data node in the cluster:

`indices.memory.index_buffer_size`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Accepts either a percentage or a byte size value. It defaults to `10%`, meaning that `10%` of the total heap allocated to a node will be used as the indexing buffer size shared across all shards.

`indices.memory.min_index_buffer_size`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) If the `index_buffer_size` is specified as a percentage, then this setting can be used to specify an absolute minimum. Defaults to `48mb`.

`indices.memory.max_index_buffer_size`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) If the `index_buffer_size` is specified as a percentage, then this setting can be used to specify an absolute maximum. Defaults to unbounded.

