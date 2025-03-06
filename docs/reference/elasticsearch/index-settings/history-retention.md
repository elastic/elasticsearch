---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-history-retention.html
navigation_title: History retention
---

# History retention settings [index-modules-history-retention]

{{es}} sometimes needs to replay some of the operations that were performed on a shard. For instance, if a replica is briefly offline then it may be much more efficient to replay the few operations it missed while it was offline than to rebuild it from scratch. Similarly, {{ccr}} works by performing operations on the leader cluster and then replaying those operations on the follower cluster.

At the Lucene level there are really only two write operations that {{es}} performs on an index: a new document may be indexed, or an existing document may be deleted. Updates are implemented by atomically deleting the old document and then indexing the new document. A document indexed into Lucene already contains all the information needed to replay that indexing operation, but this is not true of document deletions. To solve this, {{es}} uses a feature called *soft deletes* to preserve recent deletions in the Lucene index so that they can be replayed.

{{es}} only preserves certain recently-deleted documents in the index because a soft-deleted document still takes up some space. Eventually {{es}} will fully discard these soft-deleted documents to free up that space so that the index does not grow larger and larger over time. Fortunately {{es}} does not need to be able to replay every operation that has ever been performed on a shard, because it is always possible to make a full copy of a shard on a remote node. However, copying the whole shard may take much longer than replaying a few missing operations, so {{es}} tries to retain all of the operations it expects to need to replay in future.

{{es}} keeps track of the operations it expects to need to replay in future using a mechanism called *shard history retention leases*. Each shard copy that might need operations to be replayed must first create a shard history retention lease for itself. For example, this shard copy might be a replica of a shard or it might be a shard of a follower index when using {{ccr}}. Each retention lease keeps track of the sequence number of the first operation that the corresponding shard copy has not received. As the shard copy receives new operations, it increases the sequence number contained in its retention lease to indicate that it will not need to replay those operations in future. {{es}} discards soft-deleted operations once they are not being held by any retention lease.

If a shard copy fails then it stops updating its shard history retention lease, which means that {{es}} will preserve all new operations so they can be replayed when the failed shard copy recovers. However, retention leases only last for a limited amount of time. If the shard copy does not recover quickly enough then its retention lease may expire. This protects {{es}} from retaining history forever if a shard copy fails permanently, because once a retention lease has expired {{es}} can start to discard history again. If a shard copy recovers after its retention lease has expired then {{es}} will fall back to copying the whole index since it can no longer simply replay the missing history. The expiry time of a retention lease defaults to `12h` which should be long enough for most reasonable recovery scenarios.


## History retention settings [_history_retention_settings]

`index.soft_deletes.enabled`
:   [7.6.0] Indicates whether soft deletes are enabled on the index. Soft deletes can only be configured at index creation and only on indices created on or after {{es}} 6.5.0. Defaults to `true`.

`index.soft_deletes.retention_lease.period`
:   The maximum period to retain a shard history retention lease before it is considered expired. Shard history retention leases ensure that soft deletes are retained during merges on the Lucene index. If a soft delete is merged away before it can be replicated to a follower the following process will fail due to incomplete history on the leader. Defaults to `12h`.

