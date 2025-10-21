---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/ilm-actions.html
---

# Index lifecycle actions [ilm-actions]

:::{note}
This section provides detailed **reference information** for Index lifecycle actions.

Refer to [Index lifecycle management](docs-content://manage-data/lifecycle/index-lifecycle-management.md) in the **Manage data** section for overview, getting started and conceptual information.
:::

[Allocate](/reference/elasticsearch/index-lifecycle-actions/ilm-allocate.md)
:   Move shards to nodes with different performance characteristics and reduce the number of replicas.

[Delete](/reference/elasticsearch/index-lifecycle-actions/ilm-delete.md)
:   Permanently remove the index.

[Force merge](/reference/elasticsearch/index-lifecycle-actions/ilm-forcemerge.md)
:   Reduce the number of index segments and purge deleted documents.

[Migrate](/reference/elasticsearch/index-lifecycle-actions/ilm-migrate.md)
:   Move the index shards to the [data tier](docs-content://manage-data/lifecycle/data-tiers.md) that corresponds to the current {{ilm-init}} phase.

[Read only](/reference/elasticsearch/index-lifecycle-actions/ilm-readonly.md)
:   Block write operations to the index.

[Rollover](/reference/elasticsearch/index-lifecycle-actions/ilm-rollover.md)
:   Remove the index as the write index for the rollover alias and start indexing to a new index.

[Downsample](/reference/elasticsearch/index-lifecycle-actions/ilm-downsample.md)
:   Aggregates an index’s time series data and stores the results in a new read-only index. For example, you can downsample hourly data into daily or weekly summaries.

[Searchable snapshot](/reference/elasticsearch/index-lifecycle-actions/ilm-searchable-snapshot.md)
:   Take a snapshot of the managed index in the configured repository and mount it as a searchable snapshot.

[Set priority](/reference/elasticsearch/index-lifecycle-actions/ilm-set-priority.md)
:   Lower the priority of an index as it moves through the lifecycle to ensure that hot indices are recovered first.

[Shrink](/reference/elasticsearch/index-lifecycle-actions/ilm-shrink.md)
:   Reduce the number of primary shards by shrinking the index into a new index.

[Unfollow](/reference/elasticsearch/index-lifecycle-actions/ilm-unfollow.md)
:   Convert a follower index to a regular index. Performed automatically before a rollover, shrink, or searchable snapshot action.

[Wait for snapshot](/reference/elasticsearch/index-lifecycle-actions/ilm-wait-for-snapshot.md)
:   Ensure that a snapshot exists before deleting the index.













