/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

public interface GlobalCheckpointSyncer {
    /**
     * Synchronize the global checkpoints across the replication group. This is used when indexing traffic stops and the primary's global
     * checkpoint reaches the max seqno, because in this state the replicas will have an older global checkpoint as carried by the earlier
     * indexing traffic, and may not receive any further updates without the explicit sync that this method triggers.
     * <p>
     * It's also used if {@link org.elasticsearch.index.translog.Translog.Durability#ASYNC} is selected, because in that case indexing
     * traffic does not advance the persisted global checkpoint.
     * <p>
     * In production this triggers a {@link org.elasticsearch.index.seqno.GlobalCheckpointSyncAction}.
     *
     * @param shardId The ID of the shard to synchronize.
     */
    void syncGlobalCheckpoints(ShardId shardId);
}
