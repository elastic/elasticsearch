---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/tsds-index-settings.html
navigation_title: Time series
---

# Time series index settings [tsds-index-settings]

Backing indices in a [time series data stream (TSDS)](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md) support the following index settings.

$$$index-mode$$$

`index.mode`
:   (Static, string) Mode for the index. Valid values are [`time_series`](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#time-series-mode) and `null` (no mode). Defaults to `null`.

$$$index-time-series-start-time$$$

`index.time_series.start_time`
:   (Static, string) Earliest `@timestamp` value (inclusive) accepted by the index. Only indices with an `index.mode` of [`time_series`](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#time-series-mode) support this setting. For more information, refer to [Time-bound indices](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#time-bound-indices).

$$$index-time-series-end-time$$$

`index.time_series.end_time`
:   (Dynamic, string) Latest `@timestamp` value (exclusive) accepted by the index. Only indices with an `index.mode` of `time_series` support this setting. For more information, refer to [Time-bound indices](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#time-bound-indices).

$$$index-look-ahead-time$$$

`index.look_ahead_time`
:   (Static, [time units](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) Interval used to calculate the `index.time_series.end_time` for a TSDS’s write index. Defaults to `30m` (30 minutes). Accepts `1m` (one minute) to `2h` (two hours). Only indices with an `index.mode` of `time_series` support this setting. For more information, refer to [Look-ahead time](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#tsds-look-ahead-time). Additionally this setting can not be less than `time_series.poll_interval` cluster setting.

::::{note}
Increasing the `look_ahead_time` will also increase the amount of time {{ilm-cap}} waits before being able to proceed with executing the actions that expect the index to not receive any writes anymore. For more information, refer to [Time-bound indices](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#time-bound-indices).
::::


$$$index-look-back-time$$$

`index.look_back_time`
:   (Static, [time units](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) Interval used to calculate the `index.time_series.start_time` for a TSDS’s first backing index when a tsdb data stream is created. Defaults to `2h` (2 hours). Accepts `1m` (one minute) to `7d` (seven days). Only indices with an `index.mode` of `time_series` support this setting. For more information, refer to [Look-back time](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#tsds-look-back-time).

$$$index-routing-path$$$ `index.routing_path`
:   (Static, string or array of strings) Plain `keyword` fields used to route documents in a TSDS to index shards. Supports wildcards (`*`). Only indices with an `index.mode` of `time_series` support this setting. Defaults to an empty list, except for data streams then defaults to the list of [dimension fields](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#time-series-dimension) with a `time_series_dimension` value of `true` defined in your component and index templates. For more information, refer to [Dimension-based routing](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#dimension-based-routing).

$$$index-mapping-dimension-fields-limit$$$

`index.mapping.dimension_fields.limit`
:   (Dynamic, integer) Maximum number of [time series dimensions](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#time-series-dimension) for the index. Defaults to `32768`.

