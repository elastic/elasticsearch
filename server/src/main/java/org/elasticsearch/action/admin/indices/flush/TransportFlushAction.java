
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.flush;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportBroadcastReplicationAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;

/**
 * Flush Action.
 */
public class TransportFlushAction extends TransportBroadcastReplicationAction<
    FlushRequest,
    BroadcastResponse,
    ShardFlushRequest,
    ReplicationResponse> {

    @Inject
    public TransportFlushAction(
        ClusterService clusterService,
        TransportService transportService,
        NodeClient client,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ProjectResolver projectResolver
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
            transportService.getThreadPool().executor(ThreadPool.Names.FLUSH),
            projectResolver
        );
    }

    @Override
    protected ShardFlushRequest newShardRequest(FlushRequest request, ShardId shardId) {
        return new ShardFlushRequest(request, shardId);
    }

    @Override
    protected BroadcastResponse newResponse(
        int successfulShards,
        int failedShards,
        int totalNumCopies,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        return new BroadcastResponse(totalNumCopies, successfulShards, failedShards, shardFailures);
    }
}
