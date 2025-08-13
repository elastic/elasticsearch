/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.segments;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
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
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class TransportIndicesSegmentsAction extends TransportBroadcastByNodeAction<
    IndicesSegmentsRequest,
    IndicesSegmentResponse,
    ShardSegments> {

    private final IndicesService indicesService;
    private final ProjectResolver projectResolver;

    @Inject
    public TransportIndicesSegmentsAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ProjectResolver projectResolver,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            IndicesSegmentsAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            IndicesSegmentsRequest::new,
            transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT)
        );
        this.indicesService = indicesService;
        this.projectResolver = projectResolver;
    }

    /**
     * Segments goes across *all* active shards.
     */
    @Override
    protected ShardsIterator shards(ClusterState clusterState, IndicesSegmentsRequest request, String[] concreteIndices) {
        return clusterState.routingTable(projectResolver.getProjectId()).allShards(concreteIndices);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, IndicesSegmentsRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, IndicesSegmentsRequest countRequest, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(projectResolver.getProjectId(), ClusterBlockLevel.METADATA_READ, concreteIndices);
    }

    @Override
    protected ShardSegments readShardResult(StreamInput in) throws IOException {
        return new ShardSegments(in);
    }

    @Override
    protected ResponseFactory<IndicesSegmentResponse, ShardSegments> getResponseFactory(
        IndicesSegmentsRequest request,
        ClusterState clusterState
    ) {
        return (totalShards, successfulShards, failedShards, results, shardFailures) -> new IndicesSegmentResponse(
            results.toArray(new ShardSegments[0]),
            totalShards,
            successfulShards,
            failedShards,
            shardFailures
        );
    }

    @Override
    protected IndicesSegmentsRequest readRequestFrom(StreamInput in) throws IOException {
        return new IndicesSegmentsRequest(in);
    }

    @Override
    protected void shardOperation(
        IndicesSegmentsRequest request,
        ShardRouting shardRouting,
        Task task,
        ActionListener<ShardSegments> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            assert task instanceof CancellableTask;
            IndexService indexService = indicesService.indexServiceSafe(shardRouting.index());
            IndexShard indexShard = indexService.getShard(shardRouting.id());
            return new ShardSegments(indexShard.routingEntry(), indexShard.segments(request.isIncludeVectorFormatsInfo()));
        });
    }
}
