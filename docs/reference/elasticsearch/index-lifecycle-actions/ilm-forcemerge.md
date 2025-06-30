---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/ilm-forcemerge.html
---

# Force merge [ilm-forcemerge]

Phases allowed: hot, warm.

[Force merges](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-forcemerge) the index into the specified maximum number of [segments](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-segments). This operation is performed on a best effort basis. For example, shards that are relocating during a `forcemerge` will not be merged.

To use the `forcemerge` action in the `hot` phase, the `rollover` action **must** be present. If no rollover action is configured, {{ilm-init}} will reject the policy.

:::::{admonition} Performance considerations
:name: ilm-forcemerge-performance

Force merge is a resource-intensive operation. If too many force merges are triggered at once, it can negatively impact your cluster. For example, this can happen when you 
* modify an existing {{ilm-init}} policy's phase `min_age` such that indices trigger into force-merging phase faster.
* apply an {{ilm-init}} policy that includes a force merge action to existing indices. If the indices meet the `min_age` criteria, they can immediately proceed through multiple actions. You can prevent this by increasing the `min_age` or setting `index.lifecycle.origination_date` to change how the index age is calculated.
* run the [{{ilm-init}} Move Step API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ilm-move-to-step) against multiple indices.

If you experience a force merge task queue backlog, you might need to increase the size of the force merge threadpool so indices can be force merged in parallel. To do this, configure the `thread_pool.force_merge.size` [cluster setting](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-get-settings). 

::::{important}
Note that `thread_pool.force_merge.size` is an advanced setting. Adjusting it can cause cascading performance impacts. Monitor cluster performance and increment the size of the thread pool slowly to reduce the backlog.
::::

Force merging will be performed by the node hosting the shard. The [node's role](docs-content://deploy-manage/distributed-architecture/clusters-nodes-shards/node-settings.md#node-roles) frequently matches the [data tier](docs-content://manage-data/lifecycle/data-tiers.md) of the {{ilm-init}}'s phase of the index, you may choose to adjust this behavior. For example: 
* A force merge in the `hot` phase will use hot nodes. Merges may be faster on this potentially higher performance hardware but may have the tradeoff of impacting ingestion. 
* A force merge in the `warm` phase will use warm nodes. Merges may take longer to perform on potentially lower performance hardware but will avoid impacting ingestion in the `hot` tier.
* A force merge in the `cold` or `frozen` phase [{{ilm-init}} Searchable Snapshots](./ilm-searchable-snapshot.md) using `force_merge_index` happens on the preceeding data tier.

We recommend that merges be targetted against SSD and not HDD disks.
:::::


## Options [ilm-forcemerge-options]

`max_num_segments`
:   (Required, integer) Number of segments to merge to. To fully merge the index, set to `1`.

`index_codec`
:   (Optional, string) Codec used to compress the document store. The only accepted value is `best_compression`, which uses [ZSTD](https://en.wikipedia.org/wiki/Zstd) for a higher compression ratio but slower stored fields performance. To use the default LZ4 codec, omit this argument.

    ::::{warning}
    If using `best_compression`, {{ilm-init}} will [close](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-close) and then [re-open](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-open) the index prior to the force merge. While closed, the index will be unavailable for read or write operations.
    ::::



## Example [ilm-forcemerge-action-ex]

```console
PUT _ilm/policy/my_policy
{
  "policy": {
    "phases": {
      "warm": {
        "actions": {
          "forcemerge" : {
            "max_num_segments": 1
          }
        }
      }
    }
  }
}
```


