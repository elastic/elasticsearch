/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.cache.clear;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

/**
 * Indices clear cache action.
 */
public class TransportClearIndicesCacheAction extends TransportBroadcastByNodeAction<
    ClearIndicesCacheRequest,
    BroadcastResponse,
    TransportBroadcastByNodeAction.EmptyResult> {

    public static final ActionType<BroadcastResponse> TYPE = new ActionType<>("indices:admin/cache/clear");
    private final IndicesService indicesService;
    private final ProjectResolver projectResolver;

    @Inject
    public TransportClearIndicesCacheAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            TYPE.name(),
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            ClearIndicesCacheRequest::new,
            transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT),
            false
        );
        this.indicesService = indicesService;
        this.projectResolver = projectResolver;
    }

    @Override
    protected EmptyResult readShardResult(StreamInput in) throws IOException {
        return EmptyResult.INSTANCE;
    }

    @Override
    protected ResponseFactory<BroadcastResponse, TransportBroadcastByNodeAction.EmptyResult> getResponseFactory(
        ClearIndicesCacheRequest request,
        ClusterState clusterState
    ) {
        return (totalShards, successfulShards, failedShards, responses, shardFailures) -> new BroadcastResponse(
            totalShards,
            successfulShards,
            failedShards,
            shardFailures
        );
    }

    @Override
    protected ClearIndicesCacheRequest readRequestFrom(StreamInput in) throws IOException {
        return new ClearIndicesCacheRequest(in);
    }

    @Override
    protected void shardOperation(
        ClearIndicesCacheRequest request,
        ShardRouting shardRouting,
        Task task,
        ActionListener<EmptyResult> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            indicesService.clearIndexShardCache(
                shardRouting.shardId(),
                request.queryCache(),
                request.fieldDataCache(),
                request.requestCache(),
                request.fields()
            );
            return EmptyResult.INSTANCE;
        });
    }

    /**
     * The indices clear cache request works against *all* shards.
     */
    @Override
    protected ShardsIterator shards(ClusterState clusterState, ClearIndicesCacheRequest request, String[] concreteIndices) {
        return clusterState.routingTable(projectResolver.getProjectId()).allShards(concreteIndices);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, ClearIndicesCacheRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, ClearIndicesCacheRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(projectResolver.getProjectId(), ClusterBlockLevel.METADATA_WRITE, concreteIndices);
    }
}
