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
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportActions;
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

import static org.elasticsearch.core.Strings.format;

public class TransportShardMultiGetAction extends TransportSingleShardAction<MultiGetShardRequest, MultiGetShardResponse> {

    private static final String ACTION_NAME = MultiGetAction.NAME + "[shard]";
    public static final ActionType<MultiGetShardResponse> TYPE = new ActionType<>(ACTION_NAME, MultiGetShardResponse::new);

    private final IndicesService indicesService;
    private final ExecutorSelector executorSelector;

    @Inject
    public TransportShardMultiGetAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ExecutorSelector executorSelector
    ) {
        super(
            ACTION_NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            MultiGetShardRequest::new,
            ThreadPool.Names.GET
        );
        this.indicesService = indicesService;
        this.executorSelector = executorSelector;
    }

    @Override
    protected boolean isSubAction() {
        return true;
    }

    @Override
    protected Writeable.Reader<MultiGetShardResponse> getResponseReader() {
        return MultiGetShardResponse::new;
    }

    @Override
    protected boolean resolveIndex(MultiGetShardRequest request) {
        return true;
    }

    @Override
    protected ShardIterator shards(ClusterState state, InternalRequest request) {
        ShardIterator iterator = clusterService.operationRouting()
            .getShards(state, request.request().index(), request.request().shardId(), request.request().preference());
        return clusterService.operationRouting().useOnlyPromotableShardsForStateless(iterator);
    }

    @Override
    protected void asyncShardOperation(MultiGetShardRequest request, ShardId shardId, ActionListener<MultiGetShardResponse> listener)
        throws IOException {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());
        if (request.realtime()) { // we are not tied to a refresh cycle here anyway
            asyncShardMultiGet(request, shardId, listener);
        } else {
            indexShard.ensureShardSearchActive(b -> {
                try {
                    asyncShardMultiGet(request, shardId, listener);
                } catch (Exception ex) {
                    listener.onFailure(ex);
                }
            });
        }
    }

    @Override
    protected MultiGetShardResponse shardOperation(MultiGetShardRequest request, ShardId shardId) {
        var indexShard = getIndexShard(shardId);
        MultiGetShardResponse response = new MultiGetShardResponse();
        for (int i = 0; i < request.locations.size(); i++) {
            MultiGetRequest.Item item = request.items.get(i);
            try {
                GetResult getResult = indexShard.getService()
                    .get(
                        item.id(),
                        item.storedFields(),
                        request.realtime(),
                        item.version(),
                        item.versionType(),
                        item.fetchSourceContext(),
                        request.isForceSyntheticSource()
                    );
                response.add(request.locations.get(i), new GetResponse(getResult));
            } catch (RuntimeException e) {
                if (TransportActions.isShardNotAvailableException(e)) {
                    throw e;
                } else {
                    logger.debug(() -> format("%s failed to execute multi_get for [%s]", shardId, item.id()), e);
                    response.add(request.locations.get(i), new MultiGetResponse.Failure(request.index(), item.id(), e));
                }
            } catch (IOException e) {
                logger.debug(() -> format("%s failed to execute multi_get for [%s]", shardId, item.id()), e);
                response.add(request.locations.get(i), new MultiGetResponse.Failure(request.index(), item.id(), e));
            }
        }

        return response;
    }

    @Override
    protected String getExecutor(MultiGetShardRequest request, ShardId shardId) {
        final ClusterState clusterState = clusterService.state();
        if (clusterState.metadata().index(shardId.getIndex()).isSystem()) {
            return executorSelector.executorForGet(shardId.getIndexName());
        } else if (indicesService.indexServiceSafe(shardId.getIndex()).getIndexSettings().isSearchThrottled()) {
            return ThreadPool.Names.SEARCH_THROTTLED;
        } else {
            return super.getExecutor(request, shardId);
        }
    }

    private void asyncShardMultiGet(MultiGetShardRequest request, ShardId shardId, ActionListener<MultiGetShardResponse> listener)
        throws IOException {
        if (request.refresh() && request.realtime() == false) {
            threadPool.executor(getExecutor(request, shardId)).execute(ActionRunnable.wrap(listener, l -> {
                var indexShard = getIndexShard(shardId);
                indexShard.externalRefresh("refresh_flag_mget", l.map(r -> shardOperation(request, shardId)));
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
