/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.refresh;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.replication.BasicReplicationRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportBroadcastReplicationAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.TransportService;

import java.util.List;

/**
 * Refresh action.
 */
public class TransportRefreshAction
    extends TransportBroadcastReplicationAction<RefreshRequest, RefreshResponse, BasicReplicationRequest, ReplicationResponse> {

    @Inject
    public TransportRefreshAction(ClusterService clusterService, TransportService transportService, ActionFilters actionFilters,
                                  IndexNameExpressionResolver indexNameExpressionResolver, NodeClient client) {
        super(RefreshAction.NAME, RefreshRequest::new, clusterService, transportService, client, actionFilters,
            indexNameExpressionResolver, TransportShardRefreshAction.TYPE);
    }

    @Override
    protected ReplicationResponse newShardResponse() {
        return new ReplicationResponse();
    }

    @Override
    protected BasicReplicationRequest newShardRequest(RefreshRequest request, ShardId shardId) {
        BasicReplicationRequest replicationRequest = new BasicReplicationRequest(shardId);
        replicationRequest.waitForActiveShards(ActiveShardCount.NONE);
        return replicationRequest;
    }

    @Override
    protected RefreshResponse newResponse(int successfulShards, int failedShards, int totalNumCopies,
                                          List<DefaultShardOperationFailedException> shardFailures) {
        return new RefreshResponse(totalNumCopies, successfulShards, failedShards, shardFailures);
    }
}
