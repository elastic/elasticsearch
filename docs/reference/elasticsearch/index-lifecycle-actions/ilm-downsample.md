---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/ilm-downsample.html
---

# Downsample [ilm-downsample]

Phases allowed: hot, warm, cold.

Aggregates a time series (TSDS) index and stores pre-computed statistical summaries (`min`, `max`, `sum`, and `value_count`) for each metric field grouped by a configured time interval. For example, a TSDS index that contains metrics sampled every 10 seconds can be downsampled to an hourly index. All documents within an hour interval are summarized and stored as a single document and stored in the downsample index.

This action corresponds to the [downsample API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-downsample).

The name of the resulting downsample index is `downsample-<original-index-name>-<random-uuid>`. If {{ilm-init}} performs the `downsample` action on a backing index for a data stream, the downsample index becomes a backing index for the same stream and the source index is deleted.

To use the `downsample` action in the `hot` phase, the `rollover` action **must** be present. If no rollover action is configured, {{ilm-init}} will reject the policy.

## Options [ilm-downsample-options]

`fixed_interval`
:   (Required, string) The [fixed time interval](docs-content://manage-data/lifecycle/rollup/understanding-groups.md#rollup-understanding-group-intervals) into which the data will be downsampled.

`force_merge_index` {applies_to}`stack: ga 9.3`
:   (Optional, boolean) When true, the downsampled index will be [force merged](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-forcemerge) to one [segment](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-segments). Defaults to `true`.

`sampling_method` {applies_to}`stack: ga 9.3`
:   (Optional, string) The sampling method that will be used to sample metrics; there are two methods available `aggregate` and
the `last_value`. Defaults to `aggregate`.

## Example [ilm-downsample-ex]

```console
PUT _ilm/policy/datastream_policy
{
  "policy": {
    "phases": {
      "hot": {
        "actions": {
          "rollover": {
            "max_docs": 1
          },
          "downsample": {
  	          "fixed_interval": "1h",
  	          "force_merge_index": false
  	      }
        }
      }
    }
  }
}
```


