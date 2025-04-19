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


## Add index block API [add-index-block]

Adds an index block to an index.

```console
PUT /my-index-000001/_block/write
```


### {{api-request-title}} [add-index-block-api-request]

`PUT /<index>/_block/<block>`


### {{api-path-parms-title}} [add-index-block-api-path-params]

`<index>`
:   (Optional, string) Comma-separated list or wildcard expression of index names used to limit the request.

    By default, you must explicitly name the indices you are adding blocks to. To allow the adding of blocks to indices with `_all`, `*`, or other wildcard expressions, change the `action.destructive_requires_name` setting to `false`. You can update this setting in the `elasticsearch.yml` file or using the [cluster update settings](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings) API.


`<block>`
:   (Required, string) Block type to add to the index.

    **Valid values**:

    `metadata`
    :   Disable metadata changes, such as closing the index.

    `read`
    :   Disable read operations.

    `read_only`
    :   Disable write operations and metadata changes.

    `write`
    :   Disable write operations. However, metadata changes are still allowed.

    ::::



### {{api-query-parms-title}} [add-index-block-api-query-params]

`allow_no_indices`
:   (Optional, Boolean) If `false`, the request returns an error if any wildcard expression, [index alias](docs-content://manage-data/data-store/aliases.md), or `_all` value targets only missing or closed indices. This behavior applies even if the request targets other open indices. For example, a request targeting `foo*,bar*` returns an error if an index starts with `foo` but no index starts with `bar`.

    Defaults to `true`.


`expand_wildcards`
:   (Optional, string) Type of index that wildcard patterns can match. If the request can target data streams, this argument determines whether wildcard expressions match hidden data streams. Supports comma-separated values, such as `open,hidden`. Valid values are:

`all`
:   Match any data stream or index, including [hidden](/reference/elasticsearch/rest-apis/api-conventions.md#multi-hidden) ones.

`open`
:   Match open, non-hidden indices. Also matches any non-hidden data stream.

`closed`
:   Match closed, non-hidden indices. Also matches any non-hidden data stream. Data streams cannot be closed.

`hidden`
:   Match hidden data streams and hidden indices. Must be combined with `open`, `closed`, or both.

`none`
:   Wildcard patterns are not accepted.

Defaults to `open`.


`ignore_unavailable`
:   (Optional, Boolean) If `false`, the request returns an error if it targets a missing or closed index. Defaults to `false`.

`master_timeout`
:   (Optional, [time units](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) Period to wait for the master node. If the master node is not available before the timeout expires, the request fails and returns an error. Defaults to `30s`. Can also be set to `-1` to indicate that the request should never timeout.

`timeout`
:   (Optional, [time units](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) Period to wait for a response from all relevant nodes in the cluster after updating the cluster metadata. If no response is received before the timeout expires, the cluster metadata update still applies but the response will indicate that it was not completely acknowledged. Defaults to `30s`. Can also be set to `-1` to indicate that the request should never timeout.


### {{api-examples-title}} [add-index-block-api-example]

The following example shows how to add an index block:

```console
PUT /my-index-000001/_block/write
```

The API returns following response:

```console-result
{
  "acknowledged" : true,
  "shards_acknowledged" : true,
  "indices" : [ {
    "name" : "my-index-000001",
    "blocked" : true
  } ]
}
```

