---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/tsds-index-settings.html
navigation_title: Time series
applies_to:
  stack: all
---

# Time series index settings [tsds-index-settings]

:::{include} _snippets/serverless-availability.md
:::

Backing indices in a [time series data stream (TSDS)](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md) support the following index settings.

$$$index-mode$$$

`index.mode` {applies_to}`serverless: all`
:   (Static, string) Mode for the index. Valid values are [`time_series`](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#time-series-mode) and `null` (no mode). Defaults to `null`.

$$$index-time-series-start-time$$$

`index.time_series.start_time` {applies_to}`serverless: all`
:   (Static, string) Earliest `@timestamp` value (inclusive) accepted by the index. Only indices with an `index.mode` of [`time_series`](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#time-series-mode) support this setting. For more information, refer to [Time-bound indices](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#time-bound-indices).

$$$index-time-series-end-time$$$

`index.time_series.end_time` {applies_to}`serverless: all`
:   (Dynamic, string) Latest `@timestamp` value (exclusive) accepted by the index. Only indices with an `index.mode` of `time_series` support this setting. For more information, refer to [Time-bound indices](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#time-bound-indices).

$$$index-look-ahead-time$$$

`index.look_ahead_time` {applies_to}`serverless: all`
:   (Static, [time units](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) Interval used to calculate the `index.time_series.end_time` for a TSDS’s write index. Defaults to `30m` (30 minutes). Accepts `1m` (one minute) to `2h` (two hours). Only indices with an `index.mode` of `time_series` support this setting. For more information, refer to [Look-ahead time](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#tsds-look-ahead-time). Additionally this setting can not be less than `time_series.poll_interval` cluster setting.

::::{note}
Increasing the `look_ahead_time` will also increase the amount of time {{ilm-cap}} waits before being able to proceed with executing the actions that expect the index to not receive any writes anymore. For more information, refer to [Time-bound indices](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#time-bound-indices).
::::


$$$index-look-back-time$$$

`index.look_back_time` {applies_to}`serverless: all`
:   (Static, [time units](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) Interval used to calculate the `index.time_series.start_time` for a TSDS’s first backing index when a tsdb data stream is created. Defaults to `2h` (2 hours). Accepts `1m` (one minute) to `7d` (seven days). Only indices with an `index.mode` of `time_series` support this setting. For more information, refer to [Look-back time](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#tsds-look-back-time).

$$$index-routing-path$$$ `index.routing_path` {applies_to}`serverless: all`
:   (Static, string or array of strings) Time series dimension fields used to route documents in a TSDS to index shards.
Supports wildcards (`*`).
Only indices with an `index.mode` of `time_series` support this setting.

:   Defaults value:
:   Indices that are not part of a time series data stream have no default value and require the routing path to be defined explicitly.
If a time series data stream is used that is eligible for the `index.dimensions`-based routing (see [`index.dimensions_tsid_strategy_enabled`](#index-dimensions-tsid-strategy-enabled)),
the `index.routing_path` will be empty.
For time series data streams where the `index.dimensions`-based routing does not apply,
this defaults to the list of [dimension fields](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#time-series-dimension) with a `time_series_dimension` value of `true` as defined in your component and index templates.

:   Manually setting a value disables the `index.dimensions`-based routing strategy (see [`index.dimensions_tsid_strategy_enabled`](#index-dimensions-tsid-strategy-enabled)).
For more information, refer to [Dimension-based routing](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#dimension-based-routing).


$$$index-dimensions-tsid-strategy-enabled$$$

`index.dimensions_tsid_strategy_enabled` {applies_to}`stack: ga 9.2` {applies_to}`serverless: all`
:   (Static, boolean) Controls if the `_tsid` can be created using the `index.dimensions` index setting.
This is an internal setting that will be automatically populated and updated for eligible time series data streams and is not user-configurable.
This strategy offers an improved ingestion performance that avoids processing dimensions multiple times for the purposes of shard routing and creating the `_tsid`.
When used, `index.routing_path` will not be set and shard routing uses the full `_tsid`,
which can help to avoid shard hot-spotting.

:   If set to `false`,
or `index.routing_path` is configured manually,
or in case the index isn't eligible (see below),
shard routing will be based  on the `index.routing_path` instead.

:   Defaults to `true`.

:   This optimized `_tsid` creation strategy is only available for data streams and if there are no dynamic templates that set `time_series_dimension: true`.
Trying to add such a dynamic template to existing backing indices after the fact will fail the update mapping request and you will need to roll over the data stream instead.

$$$index-mapping-dimension-fields-limit$$$

`index.mapping.dimension_fields.limit`
:   (Dynamic, integer) Maximum number of [time series dimensions](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#time-series-dimension) for the index. Defaults to `32768`.

