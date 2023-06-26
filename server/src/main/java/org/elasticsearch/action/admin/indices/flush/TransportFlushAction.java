/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.flush;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportBroadcastReplicationAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;

/**
 * Flush Action.
 */
public class TransportFlushAction extends TransportBroadcastReplicationAction<
    FlushRequest,
    FlushResponse,
    ShardFlushRequest,
    ReplicationResponse> {

    @Inject
    public TransportFlushAction(
        ClusterService clusterService,
        TransportService transportService,
        NodeClient client,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            FlushAction.NAME,
            FlushRequest::new,
            clusterService,
            transportService,
            client,
            actionFilters,
            indexNameExpressionResolver,
            TransportShardFlushAction.TYPE,
            ThreadPool.Names.FLUSH
        );
    }

    @Override
    protected ShardFlushRequest newShardRequest(FlushRequest request, ShardId shardId) {
        return new ShardFlushRequest(request, shardId);
    }

    @Override
    protected FlushResponse newResponse(
        int successfulShards,
        int failedShards,
        int totalNumCopies,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        return new FlushResponse(totalNumCopies, successfulShards, failedShards, shardFailures);
    }
}
