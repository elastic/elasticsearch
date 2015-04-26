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

package org.elasticsearch.action.exists;


import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.TransportBroadcastOperationAction;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.DefaultSearchContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchLocalRequest;
import org.elasticsearch.search.query.QueryPhaseExecutionException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.action.exists.ExistsRequest.DEFAULT_MIN_SCORE;

public class TransportExistsAction extends TransportBroadcastOperationAction<ExistsRequest, ExistsResponse, ShardExistsRequest, ShardExistsResponse> {

    private final IndicesService indicesService;
    private final ScriptService scriptService;
    private final PageCacheRecycler pageCacheRecycler;
    private final BigArrays bigArrays;

    @Inject
    public TransportExistsAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                IndicesService indicesService, ScriptService scriptService,
                                PageCacheRecycler pageCacheRecycler, BigArrays bigArrays, ActionFilters actionFilters) {
        super(settings, ExistsAction.NAME, threadPool, clusterService, transportService, actionFilters,
                ExistsRequest.class, ShardExistsRequest.class, ThreadPool.Names.SEARCH);
        this.indicesService = indicesService;
        this.scriptService = scriptService;
        this.pageCacheRecycler = pageCacheRecycler;
        this.bigArrays = bigArrays;
    }

    @Override
    protected void doExecute(ExistsRequest request, ActionListener<ExistsResponse> listener) {
        request.nowInMillis = System.currentTimeMillis();
        new ExistsAsyncBroadcastAction(request, listener).start();
    }

    @Override
    protected ShardExistsRequest newShardRequest(int numShards, ShardRouting shard, ExistsRequest request) {
        String[] filteringAliases = clusterService.state().metaData().filteringAliases(shard.index(), request.indices());
        return new ShardExistsRequest(shard.shardId(), filteringAliases, request);
    }

    @Override
    protected ShardExistsResponse newShardResponse() {
        return new ShardExistsResponse();
    }

    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, ExistsRequest request, String[] concreteIndices) {
        Map<String, Set<String>> routingMap = clusterState.metaData().resolveSearchRouting(request.routing(), request.indices());
        return clusterService.operationRouting().searchShards(clusterState, request.indices(), concreteIndices, routingMap, request.preference());
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, ExistsRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, ExistsRequest countRequest, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.READ, concreteIndices);
    }

    @Override
    protected ExistsResponse newResponse(ExistsRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;
        boolean exists = false;
        List<ShardOperationFailedException> shardFailures = null;

        // if docs do exist, the last response will have exists = true (since we early terminate the shard requests)
        for (int i = shardsResponses.length() - 1; i >= 0 ; i--) {
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
                successfulShards++;
                if ((exists = ((ShardExistsResponse) shardResponse).exists())) {
                    successfulShards = shardsResponses.length() - failedShards;
                    break;
                }
            }
        }
        return new ExistsResponse(exists, shardsResponses.length(), successfulShards, failedShards, shardFailures);
    }

    @Override
    protected ShardExistsResponse shardOperation(ShardExistsRequest request) throws ElasticsearchException {
        IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        IndexShard indexShard = indexService.shardSafe(request.shardId().id());

        SearchShardTarget shardTarget = new SearchShardTarget(clusterService.localNode().id(), request.shardId().getIndex(), request.shardId().id());
        SearchContext context = new DefaultSearchContext(0,
                new ShardSearchLocalRequest(request.types(), request.nowInMillis(), request.filteringAliases()),
                shardTarget, indexShard.acquireSearcher("exists"), indexService, indexShard,
                scriptService, pageCacheRecycler, bigArrays, threadPool.estimatedTimeInMillisCounter());
        SearchContext.setCurrent(context);

        try {
            if (request.minScore() != DEFAULT_MIN_SCORE) {
                context.minimumScore(request.minScore());
            }
            BytesReference source = request.querySource();
            if (source != null && source.length() > 0) {
                try {
                    QueryParseContext.setTypes(request.types());
                    context.parsedQuery(indexService.queryParserService().parseQuery(source));
                } finally {
                    QueryParseContext.removeTypes();
                }
            }
            context.preProcess();
            try {
                Lucene.EarlyTerminatingCollector existsCollector = Lucene.createExistsCollector();
                Lucene.exists(context.searcher(), context.query(), existsCollector);
                return new ShardExistsResponse(request.shardId(), existsCollector.exists());
            } catch (Exception e) {
                throw new QueryPhaseExecutionException(context, "failed to execute exists", e);
            }
        } finally {
            // this will also release the index searcher
            context.close();
            SearchContext.removeCurrent();
        }
    }

    /**
     * An async broadcast action that early terminates shard request
     * upon any shard response reporting matched doc existence
     */
    final private class ExistsAsyncBroadcastAction extends AsyncBroadcastAction  {

        final AtomicBoolean processed = new AtomicBoolean(false);

        ExistsAsyncBroadcastAction(ExistsRequest request, ActionListener<ExistsResponse> listener) {
            super(request, listener);
        }

        @Override
        protected void onOperation(ShardRouting shard, int shardIndex, ShardExistsResponse response) {
            super.onOperation(shard, shardIndex, response);
            if (response.exists()) {
                finishHim();
            }
        }

        @Override
        protected void performOperation(final ShardIterator shardIt, final ShardRouting shard, final int shardIndex) {
            if (processed.get()) {
                return;
            }
            super.performOperation(shardIt, shard, shardIndex);
        }

        @Override
        protected void finishHim() {
            if (processed.compareAndSet(false, true)) {
                super.finishHim();
            }
        }
    }
}
