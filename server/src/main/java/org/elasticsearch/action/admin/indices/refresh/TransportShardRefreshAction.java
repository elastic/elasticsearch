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
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.replication.BasicReplicationRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class TransportShardRefreshAction extends TransportReplicationAction<
    BasicReplicationRequest,
    BasicReplicationRequest,
    ReplicationResponse> {

    private static final Logger logger = LogManager.getLogger(TransportShardRefreshAction.class);

    public static final String NAME = RefreshAction.NAME + "[s]";
    public static final ActionType<ReplicationResponse> TYPE = new ActionType<>(NAME, ReplicationResponse::new);
    public static final String SOURCE_API = "api";

    @Inject
    public TransportShardRefreshAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
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
        new TransportUnpromotableShardRefreshAction(transportService, actionFilters, indicesService);
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
        try (var listeners = new RefCountingListener(listener.map(v -> new PrimaryResult<>(shardRequest, new ReplicationResponse())))) {
            var refreshResult = primary.refresh(SOURCE_API);
            logger.trace("{} refresh request executed on primary", primary.shardId());

            // Forward the request to all nodes that hold unpromotable replica shards
            final ClusterState clusterState = clusterService.state();
            final Task parentTaskId = taskManager.getTask(shardRequest.getParentTask().getId());
            clusterState.routingTable()
                .shardRoutingTable(shardRequest.shardId())
                .assignedShards()
                .stream()
                .filter(Predicate.not(ShardRouting::isPromotableToPrimary))
                .map(ShardRouting::currentNodeId)
                .collect(Collectors.toUnmodifiableSet())
                .forEach(nodeId -> {
                    final DiscoveryNode node = clusterState.nodes().get(nodeId);
                    UnpromotableShardRefreshRequest request = new UnpromotableShardRefreshRequest(
                        primary.shardId(),
                        refreshResult.generation()
                    );
                    logger.trace("forwarding refresh request [{}] to node [{}]", request, node);
                    transportService.sendChildRequest(
                        node,
                        TransportUnpromotableShardRefreshAction.NAME,
                        request,
                        parentTaskId,
                        TransportRequestOptions.EMPTY,
                        new ActionListenerResponseHandler<>(
                            listeners.acquire(ignored -> {}),
                            (in) -> TransportResponse.Empty.INSTANCE,
                            ThreadPool.Names.REFRESH
                        )
                    );
                });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    protected void shardOperationOnReplica(BasicReplicationRequest request, IndexShard replica, ActionListener<ReplicaResult> listener) {
        ActionListener.completeWith(listener, () -> {
            replica.refresh(SOURCE_API);
            logger.trace("{} refresh request executed on replica", replica.shardId());
            return new ReplicaResult();
        });
    }
}
