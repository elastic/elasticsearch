/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.search.Filter;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 *
 */
public class TransportShardDeleteByQueryAction extends TransportShardReplicationOperationAction<ShardDeleteByQueryRequest, ShardDeleteByQueryRequest, ShardDeleteByQueryResponse> {

    private final ScriptService scriptService;
    private final CacheRecycler cacheRecycler;

    @Inject
    public TransportShardDeleteByQueryAction(Settings settings, TransportService transportService,
                                             ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool,
                                             ShardStateAction shardStateAction, ScriptService scriptService, CacheRecycler cacheRecycler) {
        super(settings, transportService, clusterService, indicesService, threadPool, shardStateAction);
        this.scriptService = scriptService;
        this.cacheRecycler = cacheRecycler;
    }

    @Override
    protected boolean checkWriteConsistency() {
        return true;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.INDEX;
    }

    @Override
    protected ShardDeleteByQueryRequest newRequestInstance() {
        return new ShardDeleteByQueryRequest();
    }

    @Override
    protected ShardDeleteByQueryRequest newReplicaRequestInstance() {
        return new ShardDeleteByQueryRequest();
    }

    @Override
    protected ShardDeleteByQueryResponse newResponseInstance() {
        return new ShardDeleteByQueryResponse();
    }

    @Override
    protected String transportAction() {
        return DeleteByQueryAction.NAME + "/shard";
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, ShardDeleteByQueryRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, ShardDeleteByQueryRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, request.index());
    }

    @Override
    protected PrimaryResponse<ShardDeleteByQueryResponse, ShardDeleteByQueryRequest> shardOperationOnPrimary(ClusterState clusterState, PrimaryOperationRequest shardRequest) {
        ShardDeleteByQueryRequest request = shardRequest.request;
        IndexService indexService = indicesService.indexServiceSafe(shardRequest.request.index());
        IndexShard indexShard = indexService.shardSafe(shardRequest.shardId);

        SearchContext.setCurrent(new SearchContext(0, new ShardSearchRequest().types(request.types()), null,
                indexShard.acquireSearcher("delete_by_query", IndexShard.Mode.WRITE), indexService, indexShard, scriptService, cacheRecycler));
        try {
            Engine.DeleteByQuery deleteByQuery = indexShard.prepareDeleteByQuery(request.querySource(), request.filteringAliases(), request.types())
                    .origin(Engine.Operation.Origin.PRIMARY);
            SearchContext.current().parsedQuery(new ParsedQuery(deleteByQuery.query(), ImmutableMap.<String, Filter>of()));
            indexShard.deleteByQuery(deleteByQuery);
        } finally {
            SearchContext searchContext = SearchContext.current();
            searchContext.clearAndRelease();
            SearchContext.removeCurrent();
        }
        return new PrimaryResponse<ShardDeleteByQueryResponse, ShardDeleteByQueryRequest>(shardRequest.request, new ShardDeleteByQueryResponse(), null);
    }


    @Override
    protected void shardOperationOnReplica(ReplicaOperationRequest shardRequest) {
        ShardDeleteByQueryRequest request = shardRequest.request;
        IndexService indexService = indicesService.indexServiceSafe(shardRequest.request.index());
        IndexShard indexShard = indexService.shardSafe(shardRequest.shardId);

        SearchContext.setCurrent(new SearchContext(0, new ShardSearchRequest().types(request.types()), null,
                indexShard.acquireSearcher("delete_by_query", IndexShard.Mode.WRITE), indexService, indexShard, scriptService, cacheRecycler));
        try {
            Engine.DeleteByQuery deleteByQuery = indexShard.prepareDeleteByQuery(request.querySource(), request.filteringAliases(), request.types())
                    .origin(Engine.Operation.Origin.REPLICA);
            SearchContext.current().parsedQuery(new ParsedQuery(deleteByQuery.query(), ImmutableMap.<String, Filter>of()));
            indexShard.deleteByQuery(deleteByQuery);
        } finally {
            SearchContext searchContext = SearchContext.current();
            searchContext.clearAndRelease();
            SearchContext.removeCurrent();
        }
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, ShardDeleteByQueryRequest request) {
        GroupShardsIterator group = clusterService.operationRouting().deleteByQueryShards(clusterService.state(), request.index(), request.routing());
        for (ShardIterator shardIt : group) {
            if (shardIt.shardId().id() == request.shardId()) {
                return shardIt;
            }
        }
        throw new ElasticSearchIllegalStateException("No shards iterator found for shard [" + request.shardId() + "]");
    }
}
