/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.refresh;

import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 * A request that is sent to the promotable replicas of a primary shard
 */
public class ShardRefreshReplicaRequest extends ReplicationRequest<ShardRefreshReplicaRequest> {

    public static final long NO_FLUSH_PERFORMED = Long.MIN_VALUE;

    /**
     * Holds the flushed generation of the primary shard. This will be used by {@link TransportShardRefreshAction} to construct a
     * {@link UnpromotableShardRefreshRequest} to broadcast to the unpromotable replicas. The flushedGeneration is not serialized to
     * maintain backwards compatibility for the refresh requests to promotable replicas which do not need the refresh result. For this
     * reason, the field is package-private.
     */
    final long flushedGeneration;

    public ShardRefreshReplicaRequest(StreamInput in) throws IOException {
        super(in);
        flushedGeneration = NO_FLUSH_PERFORMED;
    }

    public ShardRefreshReplicaRequest(ShardId shardId, long flushedGeneration) {
        super(shardId);
        this.flushedGeneration = flushedGeneration;
    }

    @Override
    public String toString() {
        return "ShardRefreshReplicaRequest{" + shardId + "}";
    }

}
