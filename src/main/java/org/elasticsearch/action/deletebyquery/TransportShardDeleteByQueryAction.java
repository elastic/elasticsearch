/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.deletebyquery;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.internal.DefaultSearchContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchLocalRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 *
 */
public class TransportShardDeleteByQueryAction extends TransportShardReplicationOperationAction<ShardDeleteByQueryRequest, ShardDeleteByQueryRequest, ShardDeleteByQueryResponse> {

    public final static String DELETE_BY_QUERY_API = "delete_by_query";

    private static final String ACTION_NAME = DeleteByQueryAction.NAME + "[s]";

    private final ScriptService scriptService;
    private final PageCacheRecycler pageCacheRecycler;
    private final BigArrays bigArrays;

    @Inject
    public TransportShardDeleteByQueryAction(Settings settings, TransportService transportService,
                                             ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool,
                                             ShardStateAction shardStateAction, ScriptService scriptService,
                                             PageCacheRecycler pageCacheRecycler, BigArrays bigArrays, ActionFilters actionFilters) {
        super(settings, ACTION_NAME, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters,
                ShardDeleteByQueryRequest.class, ShardDeleteByQueryRequest.class, ThreadPool.Names.INDEX);
        this.scriptService = scriptService;
        this.pageCacheRecycler = pageCacheRecycler;
        this.bigArrays = bigArrays;
    }

    @Override
    protected boolean checkWriteConsistency() {
        return true;
    }

    @Override
    protected ShardDeleteByQueryResponse newResponseInstance() {
        return new ShardDeleteByQueryResponse();
    }

    @Override
    protected boolean resolveIndex() {
        return false;
    }

    @Override
    protected Tuple<ShardDeleteByQueryResponse, ShardDeleteByQueryRequest> shardOperationOnPrimary(ClusterState clusterState, PrimaryOperationRequest shardRequest) {
        ShardDeleteByQueryRequest request = shardRequest.request;
        IndexService indexService = indicesService.indexServiceSafe(shardRequest.shardId.getIndex());
        IndexShard indexShard = indexService.shardSafe(shardRequest.shardId.id());

        SearchContext.setCurrent(new DefaultSearchContext(0, new ShardSearchLocalRequest(request.types(), request.nowInMillis()), null,
                indexShard.acquireSearcher(DELETE_BY_QUERY_API), indexService, indexShard, scriptService,
                pageCacheRecycler, bigArrays, threadPool.estimatedTimeInMillisCounter()));
        try {
            Engine.DeleteByQuery deleteByQuery = indexShard.prepareDeleteByQuery(request.source(), request.filteringAliases(), Engine.Operation.Origin.PRIMARY, request.types());
            SearchContext.current().parsedQuery(new ParsedQuery(deleteByQuery.query()));
            indexShard.deleteByQuery(deleteByQuery);
        } finally {
            try (SearchContext searchContext = SearchContext.current()) {
                SearchContext.removeCurrent();
            }
        }
        return new Tuple<>(new ShardDeleteByQueryResponse(), shardRequest.request);
    }


    @Override
    protected void shardOperationOnReplica(ShardId shardId, ShardDeleteByQueryRequest request) {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.shardSafe(shardId.id());

        SearchContext.setCurrent(new DefaultSearchContext(0, new ShardSearchLocalRequest(request.types(), request.nowInMillis()), null,
                indexShard.acquireSearcher(DELETE_BY_QUERY_API, true), indexService, indexShard, scriptService,
                pageCacheRecycler, bigArrays, threadPool.estimatedTimeInMillisCounter()));
        try {
            Engine.DeleteByQuery deleteByQuery = indexShard.prepareDeleteByQuery(request.source(), request.filteringAliases(), Engine.Operation.Origin.REPLICA, request.types());
            SearchContext.current().parsedQuery(new ParsedQuery(deleteByQuery.query()));
            indexShard.deleteByQuery(deleteByQuery);
        } finally {
            try (SearchContext searchContext = SearchContext.current()) {
                SearchContext.removeCurrent();
            }
        }
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, InternalRequest request) {
        GroupShardsIterator group = clusterService.operationRouting().deleteByQueryShards(clusterService.state(), request.concreteIndex(), request.request().routing());
        for (ShardIterator shardIt : group) {
            if (shardIt.shardId().id() == request.request().shardId()) {
                return shardIt;
            }
        }
        throw new ElasticsearchIllegalStateException("No shards iterator found for shard [" + request.request().shardId() + "]");
    }
}
