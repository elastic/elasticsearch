---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/ilm-downsample.html
---

# Downsample [ilm-downsample]

Phases allowed: hot, warm, cold.

Aggregates a time series (TSDS) index and stores pre-computed statistical summaries (`min`, `max`, `sum`, `value_count` and `avg`) for each metric field grouped by a configured time interval. For example, a TSDS index that contains metrics sampled every 10 seconds can be downsampled to an hourly index. All documents within an hour interval are summarized and stored as a single document and stored in the downsample index.

This action corresponds to the  [downsample API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-downsample).

The name of the resulting downsample index is `downsample-<original-index-name>-<random-uuid>`. If {{ilm-init}} performs the `downsample` action on a backing index for a data stream, the downsample index becomes a backing index for the same stream and the source index is deleted.

To use the `downsample` action in the `hot` phase, the `rollover` action **must** be present. If no rollover action is configured, {{ilm-init}} will reject the policy.

## Options [ilm-downsample-options]

`fixed_interval`
:   (Required, string) The [fixed time interval](docs-content://manage-data/lifecycle/rollup/understanding-groups.md#rollup-understanding-group-intervals) into which the data will be downsampled.


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
  	          "fixed_interval": "1h"
  	      }
        }
      }
    }
  }
}
```


