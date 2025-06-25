---
navigation_title: Serverless index settings
applies_to:
  serverless: all
---

# Index settings available in {{serverless-full}} projects

[{{serverless-full}}](docs-content://deploy-manage/deploy/elastic-cloud/serverless.md) manages most index settings for you. This page lists the {{es}} index settings available in {{serverless-short}} projects.

### General settings

* [`index.codec`](./index-modules.md#index-codec)
* [`index.default_pipeline`](./index-modules.md#index-default-pipeline)  
* [`index.dense_vector.hnsw_filter_heuristic`](./index-modules.md#index-dense-vector-hnsw-filter-heuristic)
* [`index.final_pipeline`](./index-modules.md#index-final-pipeline)  
* [`index.hidden`](./index-modules.md#index-hidden)
* [`index.mode`](./index-modules.md#index-mode-setting)
* [`index.query.default_field`](./index-modules.md#index-query-default-field)  
* [`index.refresh_interval`](./index-modules.md#index-refresh-interval-setting)  

### Index sorting settings

* [`index.sort.field`](./sorting.md#index-sort-field)
* [`index.sort.missing`](./sorting.md#index-sort-missing)
* [`index.sort.mode`](./sorting.md#index-sort-mode)
* [`index.sort.order`](./sorting.md#index-sort-order)

### Index blocks settings

* [`index.blocks.read_only`](./index-block.md#index-blocks-read-only)  
* [`index.blocks.read`](./index-block.md#index-blocks-read)  
* [`index.blocks.write`](./index-block.md#index-blocks-write)  
* [`index.blocks.metadata`](./index-block.md#index-blocks-metadata)  

### Field and mapping related settings

* [`index.mapping.coerce`](../mapping-reference/coerce.md#coerce-setting)
* [`index.mapping.ignore_above`](../mapping-reference/index-mapping-ignore-above.md)
* [`index.mapping.ignore_malformed`](../mapping-reference/ignore-malformed.md#ignore-malformed-setting)
* [`index.mapping.source.mode`](../mapping-reference/mapping-source-field.md)
* [`index.mapping.synthetic_source_keep`](../mapping-reference/mapping-source-field.md)
* [`index.mapping.total_fields.limit`](./mapping-limit.md#total-fields-limit)
* [`index.mapping.total_fields.ignore_dynamic_beyond_limit`](./mapping-limit.md#ignore-dynamic-beyond-limit)

### Data stream lifecycle settings

* [`index.lifecycle.origination_date`](../configuration-reference/data-stream-lifecycle-settings.md#index-data-stream-lifecycle-origination-date)

### Time series settings

* [`index.time_series.start_time`](./time-series.md#index-time-series-start-time)
* [`index.time_series.end_time`](./time-series.md#index-time-series-end-time)
* [`index.look_ahead_time`](./time-series.md#index-look-ahead-time)
* [`index.look_back_time`](./time-series.md#index-look-back-time)
* [`index.routing_path`](./time-series.md#index-routing-path)

### Similarity and analyzers

* [`index.similarity.*`](../mapping-reference/similarity.md)
* [`index.analysis.*`](../mapping-reference/analyzer.md)