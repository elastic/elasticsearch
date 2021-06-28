/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.TransportBroadcastAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.search.stats.FieldUsageStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class TransportFieldUsageAction extends TransportBroadcastAction<FieldUsageStatsRequest, FieldUsageStatsResponse,
    FieldUsageShardRequest, FieldUsageShardResponse> {

    private final IndicesService indicesService;

    @Inject
    public TransportFieldUsageAction(ClusterService clusterService,
                                     TransportService transportService,
                                     IndicesService indexServices, ActionFilters actionFilters,
                                     IndexNameExpressionResolver indexNameExpressionResolver) {
        super(FieldUsageStatsAction.NAME, clusterService, transportService, actionFilters, indexNameExpressionResolver,
            FieldUsageStatsRequest::new, FieldUsageShardRequest::new, ThreadPool.Names.SAME);
        this.indicesService = indexServices;
    }

    @Override
    protected void doExecute(Task task, FieldUsageStatsRequest request, ActionListener<FieldUsageStatsResponse> listener) {
        super.doExecute(task, request, listener);
    }

    @Override
    protected FieldUsageShardRequest newShardRequest(int numShards, ShardRouting shard, FieldUsageStatsRequest request) {
        return new FieldUsageShardRequest(shard.shardId(), request);
    }

    @Override
    protected FieldUsageShardResponse readShardResponse(StreamInput in) throws IOException {
        return new FieldUsageShardResponse(in);
    }

    @Override
    protected FieldUsageShardResponse shardOperation(FieldUsageShardRequest request, Task task) throws IOException {
        final ShardId shardId = request.shardId();
        final IndexShard shard = indicesService.indexServiceSafe(shardId.getIndex()).getShard(shardId.id());
        return new FieldUsageShardResponse(shardId, shard.fieldUsageStats(request.fields()));
    }

    @Override
    protected FieldUsageStatsResponse newResponse(FieldUsageStatsRequest request,
                                                  AtomicReferenceArray<?> shardsResponses,
                                                  ClusterState clusterState) {
        int successfulShards = 0;
        final List<DefaultShardOperationFailedException> shardFailures = new ArrayList<>();
        final Map<String, FieldUsageStats> combined = new HashMap<>();
        for (int i = 0; i < shardsResponses.length(); i++) {
            final Object r = shardsResponses.get(i);
            if (r instanceof FieldUsageShardResponse) {
                ++successfulShards;
                FieldUsageShardResponse resp = (FieldUsageShardResponse) r;
                combined.merge(resp.getIndex(), resp.stats, FieldUsageStats::add);
            } else if (r instanceof DefaultShardOperationFailedException) {
                shardFailures.add((DefaultShardOperationFailedException) r);
            } else {
                assert false : "unknown response [" + r + "]";
                throw new IllegalStateException("unknown response [" + r + "]");
            }
        }
        return new FieldUsageStatsResponse(
            shardsResponses.length(),
            successfulShards,
            shardFailures.size(),
            shardFailures,
            combined);
    }

    @Override
    protected GroupShardsIterator<ShardIterator> shards(ClusterState clusterState,
                                                        FieldUsageStatsRequest request,
                                                        String[] concreteIndices) {
        final GroupShardsIterator<ShardIterator> groups = clusterService
            .operationRouting()
            .searchShards(clusterState, concreteIndices, null, null);
        for (ShardIterator group : groups) {
            // fails fast if any non-active groups
            if (group.size() == 0) {
                throw new NoShardAvailableActionException(group.shardId());
            }
        }
        return groups;
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, FieldUsageStatsRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, FieldUsageStatsRequest request,
                                                      String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }
}
