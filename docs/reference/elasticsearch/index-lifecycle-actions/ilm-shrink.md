---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/ilm-shrink.html
---

# Shrink [ilm-shrink]

Phases allowed: hot, warm.

[Blocks writes](/reference/elasticsearch/index-settings/index-block.md#index-blocks-write) on a source index and shrinks it into a new index with fewer primary shards. The name of the resulting index is `shrink-<random-uuid>-<original-index-name>`. This action corresponds to the [shrink API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-shrink).

After the `shrink` action, any aliases that pointed to the source index point to the new shrunken index. If {{ilm-init}} performs the `shrink` action on a backing index for a data stream, the shrunken index replaces the source index in the stream. You cannot perform the `shrink` action on a write index.

To use the `shrink` action in the `hot` phase, the `rollover` action **must** be present. If no rollover action is configured, {{ilm-init}} will reject the policy.

The shrink action will unset the index’s `index.routing.allocation.total_shards_per_node` setting, meaning that there will be no limit. This is to ensure that all shards of the index can be copied to a single node. This setting change will persist on the index even after the step completes.

::::{important}
If the shrink action is used on a [follower index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ccr-follow), policy execution waits until the leader index rolls over (or is [otherwise marked complete](docs-content://manage-data/lifecycle/index-lifecycle-management/skip-rollover.md)), then converts the follower index into a regular index with the [unfollow](/reference/elasticsearch/index-lifecycle-actions/ilm-unfollow.md) action before performing the shrink operation.
::::


## Shrink options [ilm-shrink-options]

`number_of_shards`
:   (Optional, integer) Number of shards to shrink to. Must be a factor of the number of shards in the source index. This parameter conflicts with `max_primary_shard_size`, only one of them may be set.

`max_primary_shard_size`
:   (Optional, [byte units](/reference/elasticsearch/rest-apis/api-conventions.md#byte-units)) The max primary shard size for the target index. Used to find the optimum number of shards for the target index. When this parameter is set, each shard’s storage in the target index will not be greater than the parameter. The shards count of the target index will still be a factor of the source index’s shards count, but if the parameter is less than the single shard size in the source index, the shards count for the target index will be equal to the source index’s shards count. For example, when this parameter is set to 50gb, if the source index has 60 primary shards with totaling 100gb, then the target index will have 2 primary shards, with each shard size of 50gb; if the source index has 60 primary shards with totaling 1000gb, then the target index will have 20 primary shards; if the source index has 60 primary shards with totaling 4000gb, then the target index will still have 60 primary shards. This parameter conflicts with `number_of_shards` in the `settings`, only one of them may be set.

`allow_write_after_shrink`
:   (Optional, boolean) If true, the shrunken index is made writable by removing the [write block](/reference/elasticsearch/index-settings/index-block.md#index-blocks-write). Defaults to false.


## Example [ilm-shrink-ex]

### Set the number of shards of the new shrunken index explicitly [ilm-shrink-shards-ex]

```console
PUT _ilm/policy/my_policy
{
  "policy": {
    "phases": {
      "warm": {
        "actions": {
          "shrink" : {
            "number_of_shards": 1
          }
        }
      }
    }
  }
}
```


### Calculate the optimal number of primary shards for a shrunken index [ilm-shrink-size-ex]

The following policy uses the `max_primary_shard_size` parameter to automatically calculate the new shrunken index’s primary shard count based on the source index’s storage size.

```console
PUT _ilm/policy/my_policy
{
  "policy": {
    "phases": {
      "warm": {
        "actions": {
          "shrink" : {
            "max_primary_shard_size": "50gb"
          }
        }
      }
    }
  }
}
```



## Shard allocation for shrink [ilm-shrink-shard-allocation]

During a `shrink` action, {{ilm-init}} allocates the source index’s primary shards to one node. After shrinking the index, {{ilm-init}} reallocates the shrunken index’s shards to the appropriate nodes based on your allocation rules.

These allocation steps can fail for several reasons, including:

* A node is removed during the `shrink` action.
* No node has enough disk space to host the source index’s shards.
* {{es}} cannot reallocate the shrunken index due to conflicting allocation rules.

When one of the allocation steps fails, {{ilm-init}} waits for the period set in [`index.lifecycle.step.wait_time_threshold`](/reference/elasticsearch/configuration-reference/index-lifecycle-management-settings.md#index-lifecycle-step-wait-time-threshold), which defaults to 12 hours. This threshold period lets the cluster resolve any issues causing the allocation failure.

If the threshold period passes and {{ilm-init}} has not yet shrunk the index, {{ilm-init}} attempts to allocate the source index’s primary shards to another node. If {{ilm-init}} shrunk the index but could not reallocate the shrunken index’s shards during the threshold period, {{ilm-init}} deletes the shrunken index and re-attempts the entire `shrink` action.


