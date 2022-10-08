/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.broadcast.node;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.transport.TransportService;

import java.util.List;

/**
 * Base class for all {@link AbstractTransportBroadcastByNodeAction} that do not need to retain the cluster state from the beginning of
 * processing a request to building the final response.
 */
public abstract class TransportBroadcastByNodeAction<
    Request extends BroadcastRequest<Request>,
    Response extends BroadcastResponse,
    ShardOperationResult extends Writeable> extends AbstractTransportBroadcastByNodeAction<Request, Response, ShardOperationResult> {

    protected TransportBroadcastByNodeAction(
        String actionName,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Writeable.Reader<Request> request,
        String executor
    ) {
        super(actionName, clusterService, transportService, actionFilters, indexNameExpressionResolver, request, executor);
    }

    protected TransportBroadcastByNodeAction(
        String actionName,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Writeable.Reader<Request> request,
        String executor,
        boolean canTripCircuitBreaker
    ) {
        super(
            actionName,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            request,
            executor,
            canTripCircuitBreaker
        );
    }

    @Override
    protected final boolean retainState() {
        return false;
    }

    @Override
    protected final Response newResponse(
        Request request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<ShardOperationResult> shardOperationResults,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        assert clusterState == null;
        return newResponse(request, totalShards, successfulShards, failedShards, shardOperationResults, shardFailures);
    }

    /**
     * Same as {@link #newResponse(BroadcastRequest, int, int, int, List, List, ClusterState)} but without the {@code clusterState}
     * argument.
     *
     * @param request          the underlying request
     * @param totalShards      the total number of shards considered for execution of the operation
     * @param successfulShards the total number of shards for which execution of the operation was successful
     * @param failedShards     the total number of shards for which execution of the operation failed
     * @param results          the per-node aggregated shard-level results
     * @param shardFailures    the exceptions corresponding to shard operation failures
     * @return the response
     */
    protected abstract Response newResponse(
        Request request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<ShardOperationResult> results,
        List<DefaultShardOperationFailedException> shardFailures
    );
}
