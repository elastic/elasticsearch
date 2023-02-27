/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.PlainShardIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.ExecutorSelector;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

/**
 * Performs the get operation.
 */
public class TransportGetAction extends TransportSingleShardAction<GetRequest, GetResponse> {

    private final IndicesService indicesService;
    private final ExecutorSelector executorSelector;

    @Inject
    public TransportGetAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ExecutorSelector executorSelector
    ) {
        super(
            GetAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            GetRequest::new,
            ThreadPool.Names.GET
        );
        this.indicesService = indicesService;
        this.executorSelector = executorSelector;
        // register the internal TransportGetFromTranslogAction
        new TransportGetFromTranslogAction(transportService, indicesService, actionFilters);
    }

    @Override
    protected boolean resolveIndex(GetRequest request) {
        return true;
    }

    @Override
    protected ShardIterator shards(ClusterState state, InternalRequest request) {
        final ShardIterator iterator = clusterService.operationRouting()
            .getShards(
                clusterService.state(),
                request.concreteIndex(),
                request.request().id(),
                request.request().routing(),
                request.request().preference()
            );
        if (iterator == null) {
            return null;
        }
        return new PlainShardIterator(iterator.shardId(), iterator.getShardRoutings().stream().filter(ShardRouting::isSearchable).toList());
    }

    @Override
    protected void resolveRequest(ClusterState state, InternalRequest request) {
        // update the routing (request#index here is possibly an alias)
        request.request().routing(state.metadata().resolveIndexRouting(request.request().routing(), request.request().index()));
    }

    @Override
    protected void asyncShardOperation(GetRequest request, ShardId shardId, ActionListener<GetResponse> listener) throws IOException {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());
        if (request.realtime()) { // we are not tied to a refresh cycle here anyway
            super.asyncShardOperation(request, shardId, listener);
        } else {
            indexShard.awaitShardSearchActive(b -> {
                try {
                    super.asyncShardOperation(request, shardId, listener);
                } catch (Exception ex) {
                    listener.onFailure(ex);
                }
            });
        }
    }

    @Override
    protected GetResponse shardOperation(GetRequest request, ShardId shardId) throws IOException {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());

        if (request.refresh() && request.realtime() == false) {
            indexShard.refresh("refresh_flag_get");
        }
        var shardRouting = clusterService.state().getRoutingNodes().node(clusterService.localNode().getId()).getByShardId(shardId);
        if (shardRouting == null) {
            throw new ShardNotFoundException(shardId, "shard is no longer assigned to current node");
        }

        if (request.realtime() && shardRouting.isPromotableToPrimary() == false) {
            // TODO: this shouldn't be blocking like this
            PlainActionFuture<TransportGetFromTranslogAction.Response> listener = new PlainActionFuture<>();
            var node = clusterService.state()
                .nodes()
                .get(clusterService.state().routingTable().shardRoutingTable(shardId).primaryShard().currentNodeId());
            transportService.sendRequest(
                node,
                TransportGetFromTranslogAction.NAME,
                request,
                new ActionListenerResponseHandler<>(listener, TransportGetFromTranslogAction.Response::new)
            );
            var response = FutureUtils.get(listener);
            if (response.getResult() != null) {
                return new GetResponse(response.getResult());
            }
            PlainActionFuture<Long> segmentGenerationListener = new PlainActionFuture<>();
            indexShard.waitForSegmentGeneration(response.segmentGeneration(), segmentGenerationListener);
            FutureUtils.get(segmentGenerationListener);
        }

        GetResult result = indexShard.getService()
            .get(
                request.id(),
                request.storedFields(),
                request.realtime(),
                request.version(),
                request.versionType(),
                request.fetchSourceContext(),
                request.isForceSyntheticSource()
            );
        return new GetResponse(result);
    }

    @Override
    protected Writeable.Reader<GetResponse> getResponseReader() {
        return GetResponse::new;
    }

    @Override
    protected String getExecutor(GetRequest request, ShardId shardId) {
        final ClusterState clusterState = clusterService.state();
        if (clusterState.metadata().getIndexSafe(shardId.getIndex()).isSystem()) {
            return executorSelector.executorForGet(shardId.getIndexName());
        } else if (indicesService.indexServiceSafe(shardId.getIndex()).getIndexSettings().isSearchThrottled()) {
            return ThreadPool.Names.SEARCH_THROTTLED;
        } else {
            return super.getExecutor(request, shardId);
        }
    }
}
