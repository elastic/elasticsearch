/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.get;

import org.apache.lucene.index.Term;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.get.ShardGetService;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.ExecutorSelector;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.io.UncheckedIOException;

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
        return clusterService.operationRouting()
            .getShards(state, request.request().index(), request.request().shardId(), request.request().preference());
    }

    @Override
    protected void asyncShardOperation(MultiGetShardRequest request, ShardId shardId, ActionListener<MultiGetShardResponse> listener)
        throws IOException {
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
    protected MultiGetShardResponse shardOperation(MultiGetShardRequest request, ShardId shardId) {
        MultiGetShardResponse response = new MultiGetShardResponse();
        if (request.items.isEmpty()){
            return response;
        }
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());

        if (request.refresh() && request.realtime() == false) {
            indexShard.refresh("refresh_flag_mget");
        }
        // TODO: NOCOMMIT
        ShardGetService getService = indexShard.getService();
        try (Engine.Searcher searcher = indexShard.acquireSearcher("mget")) {
            final GetResultAndPosition[] getAndPositions = new GetResultAndPosition[request.locations.size()];
            for (int i = 0; i < request.locations.size(); i++) {
                final MultiGetRequest.Item item = request.items.get(i);
                final long startTime = System.nanoTime();
                final VersionsAndSeqNoResolver.DocIdAndVersion docIdAndVersion =
                    VersionsAndSeqNoResolver.lookupId(searcher.getDirectoryReader(), new Term(IdFieldMapper.NAME, Uid.encodeId(item.id())));
                getAndPositions[i] = new GetResultAndPosition(docIdAndVersion, i, System.nanoTime() - startTime);
            }
            long sortTimes = System.nanoTime();
            ArrayUtil.introSort(getAndPositions);
            sortTimes = (System.nanoTime() - sortTimes) / getAndPositions.length;
            for (GetResultAndPosition get : getAndPositions) {
                int i = get.location;
                final MultiGetRequest.Item item = request.items.get(i);
                GetResult getResult = getService.getFromSearcher(searcher, item.id(), get.get, get.lookupTimes + sortTimes);
                response.add(request.locations.get(i), new GetResponse(getResult));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return response;
    }

    private record GetResultAndPosition( VersionsAndSeqNoResolver.DocIdAndVersion get, int location, long lookupTimes) implements Comparable<GetResultAndPosition> {
        int docId() {
            if (get == null) {
                return Integer.MAX_VALUE;
            } else {
                return get.docBase + get.docId;
            }
        }
        @Override
        public int compareTo(GetResultAndPosition o) {
            return Integer.compare(this.docId(), o.docId());
        }
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
}
