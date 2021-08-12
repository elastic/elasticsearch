/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.forcemerge;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * ForceMerge index/indices action.
 */
public class TransportForceMergeAction
        extends TransportBroadcastByNodeAction<ForceMergeRequest, ForceMergeResponse, TransportBroadcastByNodeAction.EmptyResult> {

    private final IndicesService indicesService;
    private final ThreadPool threadPool;

    @Inject
    public TransportForceMergeAction(ClusterService clusterService, TransportService transportService, IndicesService indicesService,
                                   ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(ForceMergeAction.NAME, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                ForceMergeRequest::new, ThreadPool.Names.SAME);
        this.indicesService = indicesService;
        this.threadPool = transportService.getThreadPool();
    }

    @Override
    protected EmptyResult readShardResult(StreamInput in) throws IOException {
        return EmptyResult.readEmptyResultFrom(in);
    }

    @Override
    protected ForceMergeResponse newResponse(ForceMergeRequest request, int totalShards, int successfulShards, int failedShards,
                                             List<EmptyResult> responses, List<DefaultShardOperationFailedException> shardFailures,
                                             ClusterState clusterState) {
        return new ForceMergeResponse(totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected ForceMergeRequest readRequestFrom(StreamInput in) throws IOException {
        return new ForceMergeRequest(in);
    }

    @Override
    protected void shardOperation(ForceMergeRequest request, ShardRouting shardRouting, Task task,
                                  ActionListener<TransportBroadcastByNodeAction.EmptyResult> listener) {
        assert (task instanceof CancellableTask) == false; // TODO: add cancellation handling here once the task supports it
        threadPool.executor(ThreadPool.Names.FORCE_MERGE).execute(ActionRunnable.supply(listener,
            () -> {
                if ( request.shardNumber() == -1 || request.shardNumber() == shardRouting.shardId().id() ) {
                    IndexShard indexShard = indicesService.indexServiceSafe(shardRouting.shardId().getIndex())
                        .getShard(shardRouting.shardId().id());
                    indexShard.forceMerge(request);
                }
                return EmptyResult.INSTANCE;
            }));
    }

    /**
     * The refresh request works against *all* shards.
     */
    @Override
    protected ShardsIterator shards(ClusterState clusterState, ForceMergeRequest request, String[] concreteIndices) {
        return clusterState.routingTable().allShards(concreteIndices);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, ForceMergeRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, ForceMergeRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, concreteIndices);
    }
}
