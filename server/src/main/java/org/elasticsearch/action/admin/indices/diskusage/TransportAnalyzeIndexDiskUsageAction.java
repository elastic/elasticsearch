/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.diskusage;

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
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class TransportAnalyzeIndexDiskUsageAction extends TransportBroadcastAction<
    AnalyzeIndexDiskUsageRequest, AnalyzeIndexDiskUsageResponse,
    AnalyzeDiskUsageShardRequest, AnalyzeDiskUsageShardResponse> {
    private final IndicesService indicesService;

    @Inject
    public TransportAnalyzeIndexDiskUsageAction(ClusterService clusterService,
                                                TransportService transportService,
                                                IndicesService indexServices, ActionFilters actionFilters,
                                                IndexNameExpressionResolver indexNameExpressionResolver) {
        super(AnalyzeIndexDiskUsageAction.NAME, clusterService, transportService, actionFilters, indexNameExpressionResolver,
            AnalyzeIndexDiskUsageRequest::new, AnalyzeDiskUsageShardRequest::new, ThreadPool.Names.ANALYZE);
        this.indicesService = indexServices;
    }

    @Override
    protected void doExecute(Task task, AnalyzeIndexDiskUsageRequest request, ActionListener<AnalyzeIndexDiskUsageResponse> listener) {
        super.doExecute(task, request, listener);
    }

    @Override
    protected AnalyzeDiskUsageShardRequest newShardRequest(int numShards, ShardRouting shard, AnalyzeIndexDiskUsageRequest request) {
        return new AnalyzeDiskUsageShardRequest(shard.shardId(), request);
    }

    @Override
    protected AnalyzeDiskUsageShardResponse readShardResponse(StreamInput in) throws IOException {
        return new AnalyzeDiskUsageShardResponse(in);
    }

    @Override
    protected AnalyzeDiskUsageShardResponse shardOperation(AnalyzeDiskUsageShardRequest request, Task task) throws IOException {
        final ShardId shardId = request.shardId();
        assert task instanceof CancellableTask : "AnalyzeDiskUsageShardRequest must create a cancellable task";
        final CancellableTask cancellableTask = (CancellableTask) task;
        final Runnable checkForCancellation = cancellableTask::ensureNotCancelled;
        final IndexShard shard = indicesService.indexServiceSafe(shardId.getIndex()).getShard(shardId.id());
        try (Engine.IndexCommitRef commitRef = shard.acquireLastIndexCommit(request.flush)) {
            final IndexDiskUsageStats stats = IndexDiskUsageAnalyzer.analyze(shardId, commitRef.getIndexCommit(), checkForCancellation);
            return new AnalyzeDiskUsageShardResponse(shardId, stats);
        }
    }

    @Override
    protected AnalyzeIndexDiskUsageResponse newResponse(AnalyzeIndexDiskUsageRequest request,
                                                        AtomicReferenceArray<?> shardsResponses,
                                                        ClusterState clusterState) {
        int successfulShards = 0;
        final List<DefaultShardOperationFailedException> shardFailures = new ArrayList<>();
        final Map<String, IndexDiskUsageStats> combined = new HashMap<>();
        for (int i = 0; i < shardsResponses.length(); i++) {
            final Object r = shardsResponses.get(i);
            if (r instanceof AnalyzeDiskUsageShardResponse) {
                ++successfulShards;
                AnalyzeDiskUsageShardResponse resp = (AnalyzeDiskUsageShardResponse) r;
                combined.compute(resp.getIndex(), (k, v) -> v == null ? resp.stats : v.add(resp.stats));
            } else if (r instanceof DefaultShardOperationFailedException) {
                shardFailures.add((DefaultShardOperationFailedException) r);
            } else {
                assert false : "unknown response [" + r + "]";
                throw new IllegalStateException("unknown response [" + r + "]");
            }
        }
        return new AnalyzeIndexDiskUsageResponse(
            shardsResponses.length(),
            successfulShards,
            shardFailures.size(),
            shardFailures,
            combined);
    }

    @Override
    protected GroupShardsIterator<ShardIterator> shards(ClusterState clusterState,
                                                        AnalyzeIndexDiskUsageRequest request,
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
    protected ClusterBlockException checkGlobalBlock(ClusterState state, AnalyzeIndexDiskUsageRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, AnalyzeIndexDiskUsageRequest request,
                                                      String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }
}
