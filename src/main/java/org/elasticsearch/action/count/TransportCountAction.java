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

package org.elasticsearch.action.count;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.TransportBroadcastOperationAction;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QueryPhaseExecutionException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.google.common.collect.Lists.newArrayList;

/**
 *
 */
public class TransportCountAction extends TransportBroadcastOperationAction<CountRequest, CountResponse, ShardCountRequest, ShardCountResponse> {

    private final IndicesService indicesService;

    private final ScriptService scriptService;

    private final CacheRecycler cacheRecycler;

    @Inject
    public TransportCountAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                IndicesService indicesService, ScriptService scriptService, CacheRecycler cacheRecycler) {
        super(settings, threadPool, clusterService, transportService);
        this.indicesService = indicesService;
        this.scriptService = scriptService;
        this.cacheRecycler = cacheRecycler;
    }

    @Override
    protected void doExecute(CountRequest request, ActionListener<CountResponse> listener) {
        request.nowInMillis = System.currentTimeMillis();
        super.doExecute(request, listener);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SEARCH;
    }

    @Override
    protected String transportAction() {
        return CountAction.NAME;
    }

    @Override
    protected CountRequest newRequest() {
        return new CountRequest();
    }

    @Override
    protected ShardCountRequest newShardRequest() {
        return new ShardCountRequest();
    }

    @Override
    protected ShardCountRequest newShardRequest(ShardRouting shard, CountRequest request) {
        String[] filteringAliases = clusterService.state().metaData().filteringAliases(shard.index(), request.indices());
        return new ShardCountRequest(shard.index(), shard.id(), filteringAliases, request);
    }

    @Override
    protected ShardCountResponse newShardResponse() {
        return new ShardCountResponse();
    }

    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, CountRequest request, String[] concreteIndices) {
        Map<String, Set<String>> routingMap = clusterState.metaData().resolveSearchRouting(request.routing(), request.indices());
        return clusterService.operationRouting().searchShards(clusterState, request.indices(), concreteIndices, routingMap, request.preference());
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, CountRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, CountRequest countRequest, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.READ, concreteIndices);
    }

    @Override
    protected CountResponse newResponse(CountRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;
        long count = 0;
        List<ShardOperationFailedException> shardFailures = null;
        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                // simply ignore non active shards
            } else if (shardResponse instanceof BroadcastShardOperationFailedException) {
                failedShards++;
                if (shardFailures == null) {
                    shardFailures = newArrayList();
                }
                shardFailures.add(new DefaultShardOperationFailedException((BroadcastShardOperationFailedException) shardResponse));
            } else {
                count += ((ShardCountResponse) shardResponse).getCount();
                successfulShards++;
            }
        }
        return new CountResponse(count, shardsResponses.length(), successfulShards, failedShards, shardFailures);
    }

    @Override
    protected ShardCountResponse shardOperation(ShardCountRequest request) throws ElasticSearchException {
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        IndexShard indexShard = indexService.shardSafe(request.shardId());

        SearchShardTarget shardTarget = new SearchShardTarget(clusterService.localNode().id(), request.index(), request.shardId());
        SearchContext context = new SearchContext(0,
                new ShardSearchRequest().types(request.types())
                        .filteringAliases(request.filteringAliases())
                        .nowInMillis(request.nowInMillis()),
                shardTarget, indexShard.acquireSearcher("count"), indexService, indexShard,
                scriptService, cacheRecycler);
        SearchContext.setCurrent(context);

        try {
            // TODO: min score should move to be "null" as a value that is not initialized...
            if (request.minScore() != -1) {
                context.minimumScore(request.minScore());
            }
            BytesReference querySource = request.querySource();
            if (querySource != null && querySource.length() > 0) {
                try {
                    QueryParseContext.setTypes(request.types());
                    context.parsedQuery(indexService.queryParserService().parse(querySource));
                } finally {
                    QueryParseContext.removeTypes();
                }
            }
            context.preProcess();
            try {
                long count = Lucene.count(context.searcher(), context.query());
                return new ShardCountResponse(request.index(), request.shardId(), count);
            } catch (Exception e) {
                throw new QueryPhaseExecutionException(context, "failed to execute count", e);
            }
        } finally {
            // this will also release the index searcher
            context.release();
            SearchContext.removeCurrent();
        }
    }
}
