---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/ilm-searchable-snapshot.html
---

# Searchable snapshot [ilm-searchable-snapshot]

Phases allowed: hot, cold, frozen.

Takes a snapshot of the managed index in the configured repository and mounts it as a [{{search-snap}}](docs-content://deploy-manage/tools/snapshot-and-restore/searchable-snapshots.md). If the index is part of a [data stream](docs-content://manage-data/data-store/data-streams.md), the mounted index replaces the original index in the stream.

The `searchable_snapshot` action requires [data tiers](docs-content://manage-data/lifecycle/data-tiers.md). The action uses the [`index.routing.allocation.include._tier_preference`](/reference/elasticsearch/index-settings/data-tier-allocation.md#tier-preference-allocation-filter) setting to mount the index directly to the phase’s corresponding data tier. In the frozen phase, the action mounts a [partially mounted index](docs-content://deploy-manage/tools/snapshot-and-restore/searchable-snapshots.md#partially-mounted) prefixed with `partial-` to the frozen tier. In other phases, the action mounts a [fully mounted index](docs-content://deploy-manage/tools/snapshot-and-restore/searchable-snapshots.md#fully-mounted) prefixed with `restored-` to the corresponding data tier.

In the frozen tier, the action will ignore the setting [`index.routing.allocation.total_shards_per_node`](/reference/elasticsearch/index-settings/total-shards-per-node.md#total-shards-per-node), if it was present in the original index, to account for the difference in the number of nodes between the frozen and the other tiers. To set [`index.routing.allocation.total_shards_per_node`](/reference/elasticsearch/index-settings/total-shards-per-node.md#total-shards-per-node) for searchable snapshots, set the `total_shards_per_node` option in the frozen phase’s `searchable_snapshot` action within the ILM policy.

::::{warning}
Don’t include the `searchable_snapshot` action in both the hot and cold phases. This can result in indices failing to automatically migrate to the cold tier during the cold phase.
::::


If the `searchable_snapshot` action is used in the hot phase the subsequent phases cannot include the `shrink` or `forcemerge` actions.

This action cannot be performed on a data stream’s write index. Attempts to do so will fail. To convert the index to a searchable snapshot, first [manually roll over](docs-content://manage-data/data-store/data-streams/use-data-stream.md#manually-roll-over-a-data-stream) the data stream. This creates a new write index. Because the index is no longer the stream’s write index, the action can then convert it to a searchable snapshot. Using a policy that makes use of the [rollover](/reference/elasticsearch/index-lifecycle-actions/ilm-rollover.md) action in the hot phase will avoid this situation and the need for a manual rollover for future managed indices.

::::{important}
Mounting and relocating the shards of {{search-snap}} indices involves copying the shard contents from the snapshot repository. This may incur different costs from the copying between nodes that happens with regular indices. These costs are typically lower but, in some environments, may be higher. See [Reduce costs with {{search-snaps}}](docs-content://deploy-manage/tools/snapshot-and-restore/searchable-snapshots.md#searchable-snapshots-costs) for more details.
::::


By default, this snapshot is deleted by the [delete action](/reference/elasticsearch/index-lifecycle-actions/ilm-delete.md) in the delete phase. To keep the snapshot, set `delete_searchable_snapshot` to `false` in the delete action. This snapshot retention runs off the index lifecycle management (ILM) policies and is not effected by snapshot lifecycle management (SLM) policies.

## Options [ilm-searchable-snapshot-options]

`snapshot_repository`
:   (Required, string) [Repository](docs-content://deploy-manage/tools/snapshot-and-restore/self-managed.md) used to store the snapshot.

`replicate_for`
:   (Optional, TimeValue) By default, searchable snapshot indices are mounted without replicas. Using this will result in a searchable snapshot index being mounted with a single replica for the time period specified, after which the replica will be removed. This option is only permitted on the first searchable snapshot action of a policy.

`force_merge_index`
:   (Optional, Boolean) Force merges the managed index to one segment. Defaults to `true`. If the managed index was already force merged using the [force merge action](/reference/elasticsearch/index-lifecycle-actions/ilm-forcemerge.md) in a previous action the `searchable snapshot` action force merge step will be a no-op.

    ::::{note}
    Shards that are relocating during a `forcemerge` will not be merged. The `searchable_snapshot` action will continue executing even if not all shards are force merged.
    ::::

    This force merging occurs in the phase that the index is in **prior** to the `searchable_snapshot` action. For example, if using a `searchable_snapshot` action in the `hot` phase, the force merge will be performed on the hot nodes. If using a `searchable_snapshot` action in the `cold` phase, the force merge will be performed on whatever tier the index is **prior** to the `cold` phase (either `hot` or `warm`).

`total_shards_per_node`
:   The maximum number of shards (replicas and primaries) that will be allocated to a single node for the searchable snapshot index. Defaults to unbounded.


## Examples [ilm-searchable-snapshot-ex]

```console
PUT _ilm/policy/my_policy
{
  "policy": {
    "phases": {
      "cold": {
        "actions": {
          "searchable_snapshot" : {
            "snapshot_repository" : "backing_repo"
          }
        }
      }
    }
  }
}
```

### Mount a searchable snapshot with replicas for fourteen days [ilm-searchable-snapshot-replicate-for-ex]

This policy mounts a searchable snapshot in the hot phase with a single replica and maintains that replica for fourteen days. After that time has elapsed, the searchable snapshot index will remain (with no replicas) for another fourteen days, at which point it will proceed into the delete phase and will be deleted.

```console
PUT _ilm/policy/my_policy
{
  "policy": {
    "phases": {
      "hot": {
        "actions": {
          "rollover" : {
            "max_primary_shard_size": "50gb"
          },
          "searchable_snapshot" : {
            "snapshot_repository" : "backing_repo",
            "replicate_for": "14d"
          }
        }
      },
      "delete": {
        "min_age": "28d",
        "actions": {
          "delete" : { }
        }
      }
    }
  }
}
```

::::{note}
If the `replicate_for` option is specified, its value must be less than the minimum age of the next phase in the policy.
::::




