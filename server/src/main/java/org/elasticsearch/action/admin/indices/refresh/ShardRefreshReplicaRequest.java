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
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 * A request that is sent to the promotable replicas of a primary shard
 */
public class ShardRefreshReplicaRequest extends ReplicationRequest<ShardRefreshReplicaRequest> {

    /**
     * Holds the refresh result of the primary shard. This will be used by {@link TransportShardRefreshAction} to construct a
     * {@link UnpromotableShardRefreshRequest} to broadcast to the unpromotable replicas. The refresh result is not serialized to maintain
     * backwards compatibility for the refresh requests to promotable replicas which do not need the refresh result. For this reason, the
     * field is package-private.
     */
    final Engine.RefreshResult primaryRefreshResult;

    public ShardRefreshReplicaRequest(StreamInput in) throws IOException {
        super(in);
        primaryRefreshResult = Engine.RefreshResult.NO_REFRESH;
    }

    public ShardRefreshReplicaRequest(ShardId shardId, Engine.RefreshResult primaryRefreshResult) {
        super(shardId);
        this.primaryRefreshResult = primaryRefreshResult;
    }

    @Override
    public String toString() {
        return "ShardRefreshReplicaRequest{" + shardId + "}";
    }

}
