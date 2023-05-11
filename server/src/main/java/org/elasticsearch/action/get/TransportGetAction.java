/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
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
        ShardIterator iterator = clusterService.operationRouting()
            .getShards(
                clusterService.state(),
                request.concreteIndex(),
                request.request().id(),
                request.request().routing(),
                request.request().preference()
            );
        return clusterService.operationRouting().useOnlyPromotableShardsForStateless(iterator);
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
            asyncGet(request, shardId, listener);
        } else {
            indexShard.awaitShardSearchActive(b -> {
                try {
                    asyncGet(request, shardId, listener);
                } catch (Exception ex) {
                    listener.onFailure(ex);
                }
            });
        }
    }

    @Override
    protected GetResponse shardOperation(GetRequest request, ShardId shardId) throws IOException {
        var indexShard = getIndexShard(shardId);
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

    private void asyncGet(GetRequest request, ShardId shardId, ActionListener<GetResponse> listener) throws IOException {
        if (request.refresh() && request.realtime() == false) {
            threadPool.executor(getExecutor(request, shardId)).execute(ActionRunnable.wrap(listener, l -> {
                var indexShard = getIndexShard(shardId);
                indexShard.externalRefresh("refresh_flag_get", l.map(r -> shardOperation(request, shardId)));
            }));
        } else {
            super.asyncShardOperation(request, shardId, listener);
        }
    }

    private IndexShard getIndexShard(ShardId shardId) {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        return indexService.getShard(shardId.id());
    }
}
