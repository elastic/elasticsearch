/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search.load;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.PlainShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Transport action responsible for collecting shard-level search load statistics across the cluster.
 * <p>
 * This action broadcasts requests to all replica shards of the specified indices (scoped by a project)
 * and aggregates their metrics into a final response.
 * </p>
 *
 * Extends {@link TransportBroadcastByNodeAction} to handle fan-out to all replica shards.
 */
public class TransportShardSearchLoadStatsAction extends TransportBroadcastByNodeAction<
    TransportShardSearchLoadStatsAction.Request,
    ShardSearchLoadStatsResponse,
    ShardSearchLoadStats> {

    private static final Logger logger = LogManager.getLogger(TransportShardSearchLoadStatsAction.class);

    private final IndicesService indicesService;
    private final ProjectResolver projectResolver;

    /**
     * Constructs a new {@code TransportShardSearchLoadStatsAction}.
     *
     * @param clusterService the cluster service
     * @param transportService the transport service for communication
     * @param indicesService service to access index and shard-level operations
     * @param actionFilters filters applied to the action
     * @param projectResolver resolves the project context for scoping operations
     * @param indexNameExpressionResolver resolves index name expressions
     */
    @Inject
    public TransportShardSearchLoadStatsAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ShardSearchLoadStatsAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            Request::new,
            transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT)
        );
        this.indicesService = indicesService;
        this.projectResolver = projectResolver;
    }

    /**
     * Returns a {@link ShardsIterator} over non-primary shards for the given indices.
     * <p>
     * This method retrieves all shard routings for the specified indices in the cluster state,
     * filters out the primary shards, and returns an iterator over the remaining shard routings
     * (i.e., the replica shards).
     *
     * @param clusterState     the current state of the cluster, used to retrieve routing information
     * @param request          the incoming request (currently unused in this method, but may be relevant in overrides)
     * @param concreteIndices  the list of index names to resolve shard routings for
     * @return a {@link ShardsIterator} over the non-primary (replica) shards of the specified indices
     */
    @Override
    protected ShardsIterator shards(ClusterState clusterState, Request request, String[] concreteIndices) {
        List<ShardRouting> shardRoutingList = new ArrayList<>(
            clusterState.routingTable(projectResolver.getProjectId()).allShards(concreteIndices).getShardRoutings()
        );
        shardRoutingList.removeIf(ShardRouting::primary);
        return new PlainShardsIterator(shardRoutingList);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, Request request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, Request request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(projectResolver.getProjectId(), ClusterBlockLevel.METADATA_READ, concreteIndices);
    }

    @Override
    protected ShardSearchLoadStats readShardResult(StreamInput in) throws IOException {
        return new ShardSearchLoadStats(in);
    }

    /**
     * Returns the factory used to construct the final response from individual shard responses.
     */
    @Override
    protected ResponseFactory<ShardSearchLoadStatsResponse, ShardSearchLoadStats> getResponseFactory(
        Request request,
        ClusterState clusterState
    ) {
        return (totalShards, successfulShards, failedShards, responses, shardFailures) -> new ShardSearchLoadStatsResponse(
            responses.toArray(new ShardSearchLoadStats[0]),
            totalShards,
            successfulShards,
            failedShards,
            shardFailures
        );
    }

    /**
     * Reads the request object from the stream.
     */
    @Override
    protected Request readRequestFrom(StreamInput in) throws IOException {
        return new Request(in);
    }

    /**
     * Executes the shard-level operation for collecting search load statistics.
     *
     * @param request the original request
     * @param shardRouting routing info of the shard
     * @param task the parent task
     * @param listener listener to notify on result
     */
    @Override
    protected void shardOperation(Request request, ShardRouting shardRouting, Task task, ActionListener<ShardSearchLoadStats> listener) {
        ActionListener.completeWith(listener, () -> {
            assert task instanceof CancellableTask;

            ShardId shardId = shardRouting.shardId();
            IndexShard indexShard = indicesService.indexServiceSafe(shardId.getIndex()).getShard(shardId.id());

            return new ShardSearchLoadStats(
                shardId.getIndex().getName(),
                shardId.getId(),
                indexShard.getSearchLoadRate()
            );
        });
    }

    /**
     * A broadcast request for collecting shard-level search load statistics.
     */
    public static class Request extends BroadcastRequest<Request> {

        /**
         * Constructs a new request.
         */
        public Request() {
            super((String[]) null);
        }

        /**
         * Deserializes the request from the stream input.
         *
         * @param in the stream input
         * @throws IOException if an I/O exception occurs
         */
        public Request(StreamInput in) throws IOException {
            super(in);
        }

        /**
         * Creates a cancellable task to track execution of this request.
         */
        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }
    }
}
