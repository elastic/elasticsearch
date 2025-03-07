---
navigation_title: "Data stream lifecycle settings"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/data-stream-lifecycle-settings.html
---

# Data stream lifecycle settings in {{es}} [data-stream-lifecycle-settings]


These are the settings available for configuring [data stream lifecycle](docs-content://manage-data/lifecycle/data-stream.md).

## Cluster level settings [_cluster_level_settings]

$$$data-streams-lifecycle-retention-max$$$

`data_streams.lifecycle.retention.max`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting), [time unit value](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) The maximum retention period that will apply to all user data streams managed by the data stream lifecycle. The max retention will also override the retention of a data stream whose configured retention exceeds the max retention. It should be greater than `10s`.

$$$data-streams-lifecycle-retention-default$$$

`data_streams.lifecycle.retention.default`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting), [time unit value](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) The retention period that will apply to all user data streams managed by the data stream lifecycle that do not have retention configured. It should be greater than `10s` and less or equals than [`data_streams.lifecycle.retention.max`](#data-streams-lifecycle-retention-max).

$$$data-streams-lifecycle-poll-interval$$$

`data_streams.lifecycle.poll_interval`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting), [time unit value](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) How often {{es}} checks what is the next action for all data streams with a built-in lifecycle. Defaults to `5m`.

$$$cluster-lifecycle-default-rollover$$$

`cluster.lifecycle.default.rollover`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting), string) This property accepts a key value pair formatted string and configures the conditions that would trigger a data stream to [rollover](docs-content://manage-data/lifecycle/index-lifecycle-management/rollover.md) when it has `lifecycle` configured. This property is an implementation detail and subject to change. Currently, it defaults to `max_age=auto,max_primary_shard_size=50gb,min_docs=1,max_primary_shard_docs=200000000`, this means that your data stream will rollover if any of the following conditions are met:

    * Either any primary shard reaches the size of 50GB,
    * or any primary shard contains 200.000.000 documents
    * or the index reaches a certain age which depends on the retention time of your data stream,
    * **and** has at least one document.


$$$data-streams-lifecycle-target-merge-factor$$$

`data_streams.lifecycle.target.merge.policy.merge_factor`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting), integer) Data stream lifecycle implements [tail merging](docs-content://manage-data/lifecycle/data-stream.md#data-streams-lifecycle-how-it-works) by updating the lucene merge policy factor for the target backing index. The merge factor is both the number of segments that should be merged together, and the maximum number of segments that we expect to find on a given tier. This setting controls what value does [Data stream lifecycle](docs-content://manage-data/lifecycle/data-stream.md) configures on the target index. It defaults to `16`. The value will be visible under the `index.merge.policy.merge_factor` index setting on the target index.

$$$data-streams-lifecycle-target-floor-segment$$$

`data_streams.lifecycle.target.merge.policy.floor_segment`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Data stream lifecycle implements [tail merging](docs-content://manage-data/lifecycle/data-stream.md#data-streams-lifecycle-how-it-works) by updating the lucene merge policy floor segment for the target backing index. This floor segment size is a way to prevent indices from having a long tail of very small segments. This setting controls what value does [data stream lifecycle](docs-content://manage-data/lifecycle/data-stream.md) configures on the target index. It defaults to `100MB`.

$$$data-streams-lifecycle-signalling-error-retry-interval$$$

`data_streams.lifecycle.signalling.error_retry_interval`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting), integer) Represents the number of retries data stream lifecycle has to perform for an index in an error step in order to signal that the index is not progressing (i.e. it’s stuck in an error step). The current signalling mechanism is a log statement at the `error` level however, the signalling mechanism can be extended in the future. Defaults to 10 retries.


## Index level settings [_index_level_settings]

The following index-level settings are typically configured on the backing indices of a data stream.

$$$index-lifecycle-prefer-ilm$$$

`index.lifecycle.prefer_ilm`
:   ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-settings), boolean) This setting determines which feature is managing the backing index of a data stream if, and only if, the backing index has an [{{ilm}}](docs-content://manage-data/lifecycle/index-lifecycle-management.md) ({{ilm-init}}) policy and the data stream has also a built-in lifecycle. When `true` this index is managed by {{ilm-init}}, when `false` the backing index is managed by the data stream lifecycle. Defaults to `true`.

$$$index-data-stream-lifecycle-origination-date$$$

`index.lifecycle.origination_date`
:   ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-settings), long) If specified, this is the timestamp used to calculate the backing index generation age after this backing index has been [rolled over](docs-content://manage-data/lifecycle/index-lifecycle-management/rollover.md). The generation age is used to determine data retention, consequently, you can use this setting if you create a backing index that contains older data and want to ensure that the retention period or other parts of the lifecycle will be applied based on the data’s original timestamp and not the timestamp they got indexed. Specified as a Unix epoch value in milliseconds.
