---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules.html
navigation_title: General
---
# Index modules [index-modules]

Index modules are modules created per index and control all aspects related to an index.

## Static index settings [_static_index_settings]

The following list contains all *static* index settings that are not associated with any specific index module:

$$$index-number-of-shards$$$

`index.number_of_shards`
:   The number of primary shards that an index should have. Defaults to `1`. This setting can only be set at index creation time. It cannot be changed on a closed index.

    ::::{note}
    The number of shards are limited to `1024` per index. This limitation is a safety limit to prevent accidental creation of indices that can destabilize a cluster due to resource allocation. The limit can be modified by specifying `export ES_JAVA_OPTS="-Des.index.max_number_of_shards=128"` system property on every node that is part of the cluster.
    ::::


$$$index-number-of-routing-shards$$$

`index.number_of_routing_shards`
:   :::::{admonition}
Integer value used with [`index.number_of_shards`](index-modules.md#index-number-of-shards) to route documents to a primary shard. See [`_routing` field](/reference/elasticsearch/mapping-reference/mapping-routing-field.md).

{{es}} uses this value when splitting an index. For example, a 5 shard index with `number_of_routing_shards` set to `30` (`5 x 2 x 3`) could be split by a factor of `2` or `3`. In other words, it could be split as follows:

* `5` → `10` → `30`  (split by 2, then by 3)
* `5` → `15` → `30` (split by 3, then by 2)
* `5` → `30` (split by 6)

This setting’s default value depends on the number of primary shards in the index. The default is designed to allow you to split by factors of 2 up to a maximum of 1024 shards.

::::{note}
In {{es}} 7.0.0 and later versions, this setting affects how documents are distributed across shards. When reindexing an older index with custom routing, you must explicitly set `index.number_of_routing_shards` to maintain the same document distribution. See the [related breaking change](https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html#_document_distribution_changes).
::::


:::::



$$$index-codec$$$ `index.codec`
:   The `default` value compresses stored data with LZ4 compression, but this can be set to `best_compression` which uses [ZSTD](https://en.wikipedia.org/wiki/Zstd) for a higher compression ratio, at the expense of slower stored fields read performance. If you are updating the compression type, the new one will be applied after segments are merged. Segment merging can be forced using [force merge](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-forcemerge). Experiments with indexing log datasets have shown that `best_compression` gives up to ~28% lower storage usage and similar indexing throughput (sometimes a bit slower or faster depending on other used options) compared to `default` while affecting get by id latencies between ~10% and ~33%. The higher get by id latencies is not a concern for many use cases like logging or metrics, since these don’t really rely on get by id functionality (Get APIs or searching by _id).

$$$index-mode-setting$$$ `index.mode`
:   The `index.mode` setting is used to control settings applied in specific domains like ingestion of time series data or logs. Different mutually exclusive modes exist, which are used to apply settings or default values controlling indexing of documents, sorting and other parameters whose value affects indexing or query performance.

        **Example**

      ```console
      PUT my-index-000001
      {
        "settings": {
          "index":{
            "mode":"standard" # This index uses the `standard` index mode
          }
        }
      }
      ```
    **Supported values**

    The `index.mode` setting supports the following values:
       - `null`:   Default value (same as `standard`).
       -  `standard`:   Standard indexing with default settings.
       -  `lookup`: Index that can be used for [LOOKUP JOIN](/reference/query-languages/esql/esql-lookup-join.md) in ES|QL. Limited to 1 shard.
       - `time_series`:   *(data streams only)* Index mode optimized for storage of metrics. For more information, see [Time series index settings](time-series.md).
       - `logsdb`: *(data streams only)* Index mode optimized for [logs](docs-content://manage-data/data-store/data-streams/logs-data-stream.md).

$$$routing-partition-size$$$ `index.routing_partition_size`
:   The number of shards a custom routing value can go to. Defaults to 1 and can only be set at index creation time. This value must be less than the `index.number_of_routing_shards` unless the `index.number_of_routing_shards` value is also 1. for more details about how this setting is used, refer to [](/reference/elasticsearch/mapping-reference/mapping-routing-field.md#routing-index-partition).

$$$ccr-index-soft-deletes$$$

`index.soft_deletes.enabled`
:   :::{admonition} Deprecated in 7.6.0
    This setting was deprecated in 7.6.0.
    :::

    Indicates whether soft deletes are enabled on the index. Soft deletes can only be configured at index creation and only on indices created on or after {{es}} 6.5.0. Defaults to `true`.

$$$ccr-index-soft-deletes-retention-period$$$

`index.soft_deletes.retention_lease.period`
:   The maximum period to retain a shard history retention lease before it is considered expired. Shard history retention leases ensure that soft deletes are retained during merges on the Lucene index. If a soft delete is merged away before it can be replicated to a follower the following process will fail due to incomplete history on the leader. Defaults to `12h`.

$$$load-fixed-bitset-filters-eagerly$$$ `index.load_fixed_bitset_filters_eagerly`
:   Indicates whether [cached filters](/reference/query-languages/query-dsl/query-filter-context.md) are pre-loaded for nested queries. Possible values are `true` (default) and `false`.

$$$index-shard-check-on-startup$$$ `index.shard.check_on_startup`
:   ::::{warning}
    Expert users only. This setting enables some very expensive processing at shard startup and is only ever useful while diagnosing a problem in your cluster. If you do use it, you should do so only temporarily and remove it once it is no longer needed.
    ::::

    {{es}} automatically performs integrity checks on the contents of shards at various points during their lifecycle. For instance, it verifies the checksum of every file transferred when recovering a replica or taking a snapshot. It also verifies the integrity of many important files when opening a shard, which happens when starting up a node and when finishing a shard recovery or relocation. You can therefore manually verify the integrity of a whole shard while it is running by taking a snapshot of it into a fresh repository or by recovering it onto a fresh node.

    This setting determines whether {{es}} performs additional integrity checks while opening a shard. If these checks detect corruption then they will prevent the shard from being opened. It accepts the following values:

    `false`
    :   Don’t perform additional checks for corruption when opening a shard. This is the default and recommended behaviour.

    `checksum`
    :   Verify that the checksum of every file in the shard matches its contents. This will detect cases where the data read from disk differ from the data that {{es}} originally wrote, for instance due to undetected disk corruption or other hardware failures. These checks require reading the entire shard from disk which takes substantial time and IO bandwidth and may affect cluster performance by evicting important data from your filesystem cache.

    `true`
    :   Performs the same checks as `checksum` and also checks for logical inconsistencies in the shard, which could for instance be caused by the data being corrupted while it was being written due to faulty RAM or other hardware failures. These checks require reading the entire shard from disk which takes substantial time and IO bandwidth, and then performing various checks on the contents of the shard which take substantial time, CPU and memory.

:::::




## Dynamic index settings [dynamic-index-settings]

Below is a list of all *dynamic* index settings that are not associated with any specific index module:

$$$dynamic-index-number-of-replicas$$$

`index.number_of_replicas`
:   The number of replicas each primary shard has. Defaults to 1.

    ```
    WARNING: Configuring it to 0 may lead to temporary availability loss
    during node restarts or permanent data loss in case of data corruption.
    ```


$$$dynamic-index-auto-expand-replicas$$$

`index.auto_expand_replicas`
:   Auto-expand the number of replicas based on the number of data nodes in the cluster. Set to a dash delimited lower and upper bound (e.g. `0-5`) or use `all` for the upper bound (e.g. `0-all`). Defaults to `false` (i.e. disabled). Note that the auto-expanded number of replicas only takes [allocation filtering](shard-allocation.md) rules into account, but ignores other allocation rules such as [total shards per node](total-shards-per-node.md), and this can lead to the cluster health becoming `YELLOW` if the applicable rules prevent all the replicas from being allocated.

    If the upper bound is `all` then shard allocation awareness and `cluster.routing.allocation.same_shard.host` are ignored for this index. For more information about this setting, refer to [](/reference/elasticsearch/configuration-reference/cluster-level-shard-allocation-routing-settings.md)


$$$dynamic-index-search-idle-after$$$

`index.search.idle.after`
:   How long a shard can not receive a search or get request until it’s considered search idle. (default is `30s`)

$$$index-refresh-interval-setting$$$

`index.refresh_interval`
:   How often to perform a refresh operation, which makes recent changes to the index visible to search. Defaults to `1s`. Can be set to `-1` to disable refresh. If this setting is not explicitly set, shards that haven’t seen search traffic for at least `index.search.idle.after` seconds will not receive background refreshes until they receive a search request. Searches that hit an idle shard where a refresh is pending will trigger a refresh as part of the search operation for that shard only. This behavior aims to automatically optimize bulk indexing in the default case when no searches are performed. In order to opt out of this behavior an explicit value of `1s` should set as the refresh interval.

$$$index-max-result-window$$$

`index.max_result_window`
:   The maximum value of `from + size` for searches to this index. Defaults to `10000`. Search requests take heap memory and time proportional to `from + size` and this limits that memory. See [Scroll](/reference/elasticsearch/rest-apis/paginate-search-results.md#scroll-search-results) or [Search After](/reference/elasticsearch/rest-apis/paginate-search-results.md#search-after) for a more efficient alternative to raising this.

`index.max_inner_result_window`
:   The maximum value of `from + size` for inner hits definition and top hits aggregations to this index. Defaults to `100`. Inner hits and top hits aggregation take heap memory and time proportional to `from + size` and this limits that memory.

`index.max_rescore_window`
:   The maximum value of `window_size` for `rescore` requests in searches of this index. Defaults to `index.max_result_window` which defaults to `10000`. Search requests take heap memory and time proportional to `max(window_size, from + size)` and this limits that memory.

`index.max_docvalue_fields_search`
:   The maximum number of `docvalue_fields` that are allowed in a query. Defaults to `100`. Doc-value fields are costly since they might incur a per-field per-document seek.

`index.max_script_fields`
:   The maximum number of `script_fields` that are allowed in a query. Defaults to `32`.

$$$index-max-ngram-diff$$$

`index.max_ngram_diff`
:   The maximum allowed difference between min_gram and max_gram for NGramTokenizer and NGramTokenFilter. Defaults to `1`.

$$$index-max-shingle-diff$$$

`index.max_shingle_diff`
:   The maximum allowed difference between max_shingle_size and min_shingle_size for the [`shingle` token filter](/reference/text-analysis/analysis-shingle-tokenfilter.md). Defaults to `3`.

`index.max_refresh_listeners`
:   Maximum number of refresh listeners available on each shard of the index. These listeners are used to implement `refresh=wait_for`.

`index.analyze.max_token_count`
:   The maximum number of tokens that can be produced using _analyze API. Defaults to `10000`.

$$$index-max-analyzed-offset$$$

`index.highlight.max_analyzed_offset`
:   The maximum number of characters that will be analyzed for a highlight request. This setting is only applicable when highlighting is requested on a text that was indexed without offsets or term vectors. Defaults to `1000000`.

$$$index-max-terms-count$$$

`index.max_terms_count`
:   The maximum number of terms that can be used in Terms Query. Defaults to `65536`.

$$$index-max-regex-length$$$

`index.max_regex_length`
:   The maximum length of value that can be used in `regexp` or `prefix` query. Defaults to `1000`.

$$$index-query-default-field$$$

`index.query.default_field`
:   (string or array of strings) Wildcard (`*`) patterns matching one or more fields. The following query types search these matching fields by default:

* [More like this](/reference/query-languages/query-dsl/query-dsl-mlt-query.md)
* [Multi-match](/reference/query-languages/query-dsl/query-dsl-multi-match-query.md)
* [Query string](/reference/query-languages/query-dsl/query-dsl-query-string-query.md)
* [Simple query string](/reference/query-languages/query-dsl/query-dsl-simple-query-string-query.md)

Defaults to `*`, which matches all fields eligible for [term-level queries](/reference/query-languages/query-dsl/term-level-queries.md), excluding metadata fields.


$$$index-routing-allocation-enable-setting$$$

`index.routing.allocation.enable`
:   Controls shard allocation for this index. It can be set to:

    * `all` (default) - Allows shard allocation for all shards.
    * `primaries` - Allows shard allocation only for primary shards.
    * `new_primaries` - Allows shard allocation only for newly-created primary shards.
    * `none` - No shard allocation is allowed.


`index.routing.rebalance.enable`
:   Enables shard rebalancing for this index. It can be set to:

    * `all` (default) - Allows shard rebalancing for all shards.
    * `primaries` - Allows shard rebalancing only for primary shards.
    * `replicas` - Allows shard rebalancing only for replica shards.
    * `none` - No shard rebalancing is allowed.


`index.gc_deletes`
:   The length of time that a deleted document's version number remains available for further versioned operations. Defaults to `60s`.

$$$index-default-pipeline$$$

`index.default_pipeline`
:   Default ingest pipeline for the index. Index requests will fail if the default pipeline is set and the pipeline does not exist. The default may be overridden using the `pipeline` parameter. The special pipeline name `_none` indicates no default ingest pipeline will run.

$$$index-final-pipeline$$$

`index.final_pipeline`
:   Final ingest pipeline for the index. Indexing requests will fail if the final pipeline is set and the pipeline does not exist. The final pipeline always runs after the request pipeline (if specified) and the default pipeline (if it exists). The special pipeline name `_none` indicates no final ingest pipeline will run.

    ::::{note}
    You can’t use a final pipeline to change the `_index` field. If the pipeline attempts to change the `_index` field, the indexing request will fail.
    ::::


$$$index-hidden$$$ `index.hidden`
:   Indicates whether the index should be hidden by default. Hidden indices are not returned by default when using a wildcard expression. This behavior is controlled per request through the use of the `expand_wildcards` parameter. Possible values are `true` and `false` (default).

$$$index-dense-vector-hnsw-filter-heuristic$$$ `index.dense_vector.hnsw_filter_heuristic`
:   The heuristic to utilize when executing a filtered search against vectors in an HNSW graph. This setting is in technical preview may be changed or removed in a future release. It can be set to:

* `acorn` (default) - Only vectors that match the filter criteria are searched. This is the fastest option, and generally provides faster searches at similar recall to `fanout`, but `num_candidates` might need to be increased for exceptionally high recall requirements.
* `fanout` - All vectors are compared with the query vector, but only those passing the criteria are added to the search results. Can be slower than `acorn`, but may yield higher recall.

$$$index-esql-stored-fields-sequential-proportion$$$

`index.esql.stored_fields_sequential_proportion`
:   Tuning parameter for deciding when {{esql}} will load [Stored fields](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#stored-fields) using a strategy tuned for loading dense sequence of documents. Allows values between 0.0 and 1.0 and defaults to 0.2. Indices with documents smaller than 10kb may see speed improvements loading `text` fields by setting this lower.
