/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.refresh;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.BasicReplicationRequest;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class TransportShardRefreshAction extends TransportReplicationAction<
    BasicReplicationRequest,
    BasicReplicationRequest,
    ReplicationResponse> {

    private static final Logger logger = LogManager.getLogger(TransportShardRefreshAction.class);

    public static final String NAME = RefreshAction.NAME + "[s]";
    public static final ActionType<ReplicationResponse> TYPE = new ActionType<>(NAME, ReplicationResponse::new);
    public static final String SOURCE_API = "api";

    private Engine.RefreshResult primaryRefreshResult = Engine.RefreshResult.NO_REFRESH;
    private final NodeClient client;

    @Inject
    public TransportShardRefreshAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
        NodeClient client,
        ActionFilters actionFilters
    ) {
        super(
            settings,
            NAME,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            actionFilters,
            BasicReplicationRequest::new,
            BasicReplicationRequest::new,
            ThreadPool.Names.REFRESH
        );
        this.client = client;
        new TransportUnpromotableShardRefreshAction(clusterService, transportService, actionFilters, indicesService, client);
    }

    @Override
    protected ReplicationResponse newResponseInstance(StreamInput in) throws IOException {
        return new ReplicationResponse(in);
    }

    @Override
    protected void shardOperationOnPrimary(
        BasicReplicationRequest shardRequest,
        IndexShard primary,
        ActionListener<PrimaryResult<BasicReplicationRequest, ReplicationResponse>> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            primaryRefreshResult = primary.refresh(SOURCE_API);
            logger.trace("{} refresh request executed on primary", primary.shardId());
            return new PrimaryResult<>(shardRequest, new ReplicationResponse());
        });
    }

    @Override
    protected void shardOperationOnReplica(BasicReplicationRequest request, IndexShard replica, ActionListener<ReplicaResult> listener) {
        ActionListener.completeWith(listener, () -> {
            replica.refresh(SOURCE_API);
            logger.trace("{} refresh request executed on replica", replica.shardId());
            return new ReplicaResult();
        });
    }

    @Override
    protected ReplicationOperation.Replicas<BasicReplicationRequest> newReplicasProxy() {
        return new UnpromotableReplicasRefreshProxy();
    }

    protected class UnpromotableReplicasRefreshProxy extends ReplicasProxy {

        @Override
        public void onPrimaryOperationComplete(
            BasicReplicationRequest replicaRequest,
            IndexShardRoutingTable indexShardRoutingTable,
            ActionListener<Void> listener
        ) {
            assert primaryRefreshResult.refreshed() : "primary has not refreshed";
            ShardId primary = replicaRequest.shardId();
            UnpromotableShardRefreshRequest unpromotableReplicaRequest = new UnpromotableShardRefreshRequest(
                indexShardRoutingTable,
                primaryRefreshResult.generation()
            );
            transportService.sendRequest(
                transportService.getLocalNode(),
                TransportUnpromotableShardRefreshAction.NAME,
                unpromotableReplicaRequest,
                new ActionListenerResponseHandler<>(
                    listener.delegateFailure((l, r) -> l.onResponse(null)),
                    (in) -> ActionResponse.Empty.INSTANCE,
                    ThreadPool.Names.REFRESH
                )
            );
        }
    }
}
