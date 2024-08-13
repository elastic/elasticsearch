/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransportFieldUsageAction extends TransportBroadcastByNodeAction<
    FieldUsageStatsRequest,
    FieldUsageStatsResponse,
    FieldUsageShardResponse> {

    private final IndicesService indicesService;

    @Inject
    public TransportFieldUsageAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indexServices,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            FieldUsageStatsAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            FieldUsageStatsRequest::new,
            transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT)
        );
        this.indicesService = indexServices;
    }

    @Override
    protected FieldUsageShardResponse readShardResult(StreamInput in) throws IOException {
        return new FieldUsageShardResponse(in);
    }

    @Override
    protected ResponseFactory<FieldUsageStatsResponse, FieldUsageShardResponse> getResponseFactory(
        FieldUsageStatsRequest request,
        ClusterState clusterState
    ) {
        return (totalShards, successfulShards, failedShards, fieldUsages, shardFailures) -> {
            final Map<String, List<FieldUsageShardResponse>> combined = new HashMap<>();
            for (FieldUsageShardResponse response : fieldUsages) {
                combined.computeIfAbsent(response.shardRouting.shardId().getIndexName(), i -> new ArrayList<>()).add(response);
            }
            return new FieldUsageStatsResponse(totalShards, successfulShards, shardFailures.size(), shardFailures, combined);
        };
    }

    @Override
    protected FieldUsageStatsRequest readRequestFrom(StreamInput in) throws IOException {
        return new FieldUsageStatsRequest(in);
    }

    @Override
    protected void shardOperation(
        FieldUsageStatsRequest request,
        ShardRouting shardRouting,
        Task task,
        ActionListener<FieldUsageShardResponse> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            final ShardId shardId = shardRouting.shardId();
            final IndexShard shard = indicesService.indexServiceSafe(shardId.getIndex()).getShard(shardId.id());
            return new FieldUsageShardResponse(
                shard.getShardUuid(),
                shardRouting,
                shard.getShardCreationTime(),
                shard.fieldUsageStats(request.fields())
            );
        });
    }

    @Override
    protected ShardsIterator shards(ClusterState clusterState, FieldUsageStatsRequest request, String[] concreteIndices) {
        return clusterState.routingTable().allActiveShards(concreteIndices);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, FieldUsageStatsRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, FieldUsageStatsRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }
}
