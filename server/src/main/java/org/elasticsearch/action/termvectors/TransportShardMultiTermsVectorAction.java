/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.termvectors;

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
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.termvectors.TermVectorsService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.Executor;

import static org.elasticsearch.core.Strings.format;

public class TransportShardMultiTermsVectorAction extends TransportSingleShardAction<
    MultiTermVectorsShardRequest,
    MultiTermVectorsShardResponse> {

    private final IndicesService indicesService;

    private static final String ACTION_NAME = MultiTermVectorsAction.NAME + "[shard]";
    public static final ActionType<MultiTermVectorsShardResponse> TYPE = new ActionType<>(ACTION_NAME);

    @Inject
    public TransportShardMultiTermsVectorAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ACTION_NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            MultiTermVectorsShardRequest::new,
            threadPool.executor(ThreadPool.Names.GET)
        );
        this.indicesService = indicesService;
    }

    @Override
    protected boolean isSubAction() {
        return true;
    }

    @Override
    protected Writeable.Reader<MultiTermVectorsShardResponse> getResponseReader() {
        return MultiTermVectorsShardResponse::new;
    }

    @Override
    protected boolean resolveIndex(MultiTermVectorsShardRequest request) {
        return false;
    }

    @Override
    protected ShardIterator shards(ClusterState state, InternalRequest request) {
        ShardIterator shards = clusterService.operationRouting()
            .getShards(state, request.concreteIndex(), request.request().shardId(), request.request().preference());
        return clusterService.operationRouting().useOnlyPromotableShardsForStateless(shards);
    }

    @Override
    protected MultiTermVectorsShardResponse shardOperation(MultiTermVectorsShardRequest request, ShardId shardId) {
        final MultiTermVectorsShardResponse response = new MultiTermVectorsShardResponse();
        final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        final IndexShard indexShard = indexService.getShard(shardId.id());
        for (int i = 0; i < request.locations.size(); i++) {
            TermVectorsRequest termVectorsRequest = request.requests.get(i);
            try {
                TermVectorsResponse termVectorsResponse = TermVectorsService.getTermVectors(indexShard, termVectorsRequest);
                response.add(request.locations.get(i), termVectorsResponse);
            } catch (RuntimeException e) {
                if (TransportActions.isShardNotAvailableException(e)) {
                    throw e;
                } else {
                    logger.debug(() -> format("%s failed to execute multi term vectors for [%s]", shardId, termVectorsRequest.id()), e);
                    response.add(
                        request.locations.get(i),
                        new MultiTermVectorsResponse.Failure(request.index(), termVectorsRequest.id(), e)
                    );
                }
            }
        }

        return response;
    }

    @Override
    protected Executor getExecutor(MultiTermVectorsShardRequest request, ShardId shardId) {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        return indexService.getIndexSettings().isSearchThrottled()
            ? threadPool.executor(ThreadPool.Names.SEARCH_THROTTLED)
            : super.getExecutor(request, shardId);
    }
}
