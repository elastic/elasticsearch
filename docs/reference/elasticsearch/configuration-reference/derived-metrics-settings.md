---
navigation_title: "Derived metrics settings"
applies_to:
  stack: all
---

# Derived metrics settings in {{es}} [derived-metrics-settings]

Derived metrics let a data stream produce compact operational metrics from the documents written to it, into a managed time series data stream. They are configured per data stream through `data_stream_options.derived_metrics`; the settings on this page are node level and bound what that configuration may cost.

Every setting here is static and must be set in `elasticsearch.yml`.

## Flushing [derived-metrics-flush-settings]

$$$derived-metrics-flush-interval$$$

`data_streams.derived_metrics.flush_interval`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting), [time unit value](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) How often a node checks for interval buckets that have closed and can be written to their destination. Defaults to `1s`, and must be at least `1s`.

$$$derived-metrics-flush-grace-period$$$

`data_streams.derived_metrics.flush_grace_period`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting), [time unit value](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) How long a bucket stays open after its interval ends, so that writes still in flight when the boundary passes are counted in the interval they belong to. Defaults to `5s`.

    Together with the metric's interval this determines how fresh derived metrics are: a bucket is complete and written `interval + flush_grace_period` after it opens, regardless of when the documents arrived.

## Limits [derived-metrics-limit-settings]

Series count is the one thing that grows with the data, because dimension values come from documents. These settings bound it. Everything refused by one of them is counted and reported by the [derived metrics stats API](/reference/elasticsearch/rest-apis/index.md).

$$$derived-metrics-max-interval-buckets$$$

`data_streams.derived_metrics.max_interval_buckets`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting), integer) How many interval buckets one metric may collect into at the same time. Defaults to `4`, and may be at most `8`.

    A metric configured with `"time_source": "event"`, which is the default for user-defined metrics, is counted in the interval its own `@timestamp` falls in rather than the interval it was written in. Buckets are created as data arrives, so a producer running behind collects normally in its own intervals rather than being measured against the receiving node's clock.

    This setting bounds how many such intervals a metric holds at once. When a new interval is needed and none is free, the interval holding the oldest data is dropped, and what it had collected is lost.

    Dropping rather than writing the bucket out is what keeps output volume a function of series and interval rather than of write rate. A producer whose data arrives at many unrelated moments would otherwise force a document per document.

    Two counters report it. `es.derived_metrics.buckets.dropped.total` says data was lost. `es.derived_metrics.buckets.shortfall.current` says how many intervals were given up in the worst single flush, which is roughly how far short of the moments it needed the metric came: raise this setting by about that much. Both are broken down per data stream by the [derived metrics stats API](/reference/elasticsearch/rest-apis/index.md).

    Ordinary data does not reach this. A producer running behind, however far behind, collects into one interval at a time and each is written out normally once it goes quiet. What consumes slots is several producers at unrelated lags, or a backlog replayed faster than intervals can close.

$$$derived-metrics-max-series-per-node$$$

`data_streams.derived_metrics.max_series_per_node`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting), integer) How many distinct series one node may hold across every data stream and metric. Defaults to `10000`. Once reached, observations that would create a new series are refused; series that already exist keep accumulating.

$$$derived-metrics-max-series-per-stream$$$

`data_streams.derived_metrics.max_series_per_stream`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting), integer) How many series any single source data stream may hold on a node. Defaults to the value of `max_series_per_node`, which means the node budget is first come, first served. Lower it to stop one high-cardinality stream consuming the budget of every other stream on the node.

$$$derived-metrics-max-histogram-series-per-node$$$

`data_streams.derived_metrics.max_histogram_series_per_node`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting), integer) How many histogram series one node may hold. Defaults to `2000`. A histogram series holds a whole distribution and costs roughly forty times a counter or gauge series, so it is bounded separately; a histogram series spends this budget **and** `max_series_per_node`.

$$$derived-metrics-max-dimension-cardinality$$$

`data_streams.derived_metrics.max_dimension_cardinality`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting), integer) How many distinct values one dimension of one metric may take before the metric stops breaking down by it. Defaults to `1000`.

    Past this limit the dimension is replaced by `_too_many_values` for that metric, so the metric stays bounded and can still be aggregated, and only the breakdown by that one dimension is lost. Set to `0` to keep estimating cardinality without ever collapsing a dimension.

$$$derived-metrics-histogram-buckets$$$

`data_streams.derived_metrics.histogram_buckets`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting), integer) The bucket capacity of each histogram series, which trades a distribution's resolution against its size. Defaults to `160`, and must be at least `4`. Storage grows towards this ceiling only as a distribution needs it.

## Memory [derived-metrics-memory-settings]

Buffered series are accounted against a dedicated `derived_metrics` circuit breaker, whose limit defaults to 5% of the heap and which is reported by the [node stats API](/reference/elasticsearch/rest-apis/index.md) alongside every other breaker.

$$$derived-metrics-memory-pressure-policy$$$

`data_streams.derived_metrics.memory_pressure_policy`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting), string) What a node does when it can hold no more, either because a limit above was reached or because the circuit breaker refused the memory a new series needed. One of:

    `flush_early`
    :   Write out what has been collected so far and carry on. Nothing is lost, because the partial results of one bucket are combined at query time exactly as results from different nodes already are. The cost is more documents while the pressure lasts. This is the default.

    `drop`
    :   Discard the observation. Document volume stays flat and timestamps stay exactly on bucket boundaries, at the cost of losing data.

## Emission [derived-metrics-emission-settings]

$$$derived-metrics-indexing-pressure-ceiling$$$

`data_streams.derived_metrics.indexing_pressure_ceiling`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting), float) The share of the node's [indexing pressure](/reference/elasticsearch/configuration-reference/indexing-pressure-settings.md) budget above which derived metrics stop writing. Defaults to `0.7`.

    Derived metrics are written through the same node-wide budget as the documents they are derived from, so a node already under indexing pressure gives up its metrics rather than pushing its own writes closer to rejection.

$$$derived-metrics-bulk-size$$$

`data_streams.derived_metrics.bulk_size`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting), integer) How many documents are written per bulk request to a destination. Defaults to `1000`.

$$$derived-metrics-max-in-flight-bulks$$$

`data_streams.derived_metrics.max_in_flight_bulks`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting), integer) How much emission may be outstanding at once, counted as this many `bulk_size` documents. Defaults to `8`. Writing is not waited on, so this is what stops a destination that cannot keep up from accumulating an unbounded queue.

$$$derived-metrics-thread-pool-size$$$

`data_streams.derived_metrics.thread_pool.size`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting), integer) The size of the thread pool that builds and writes derived metric documents. Defaults to an eighth of the node's allocated processors. Derived metrics never use the `write` thread pool for this work.

$$$derived-metrics-thread-pool-queue-size$$$

`data_streams.derived_metrics.thread_pool.queue_size`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting), integer) How much work may queue for that pool. Defaults to `128`. The queue is bounded deliberately: work beyond it is shed and counted rather than allowed to accumulate.
