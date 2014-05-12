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

package org.elasticsearch.action.admin.indices.validate.query;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.TransportBroadcastOperationAction;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.internal.DefaultSearchContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.google.common.collect.Lists.newArrayList;

/**
 *
 */
public class TransportValidateQueryAction extends TransportBroadcastOperationAction<ValidateQueryRequest, ValidateQueryResponse, ShardValidateQueryRequest, ShardValidateQueryResponse> {

    private final IndicesService indicesService;

    private final ScriptService scriptService;

    private final CacheRecycler cacheRecycler;

    private final PageCacheRecycler pageCacheRecycler;

    private final BigArrays bigArrays;

    @Inject
    public TransportValidateQueryAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService, IndicesService indicesService, ScriptService scriptService, CacheRecycler cacheRecycler, PageCacheRecycler pageCacheRecycler, BigArrays bigArrays) {
        super(settings, threadPool, clusterService, transportService);
        this.indicesService = indicesService;
        this.scriptService = scriptService;
        this.cacheRecycler = cacheRecycler;
        this.pageCacheRecycler = pageCacheRecycler;
        this.bigArrays = bigArrays;
    }

    @Override
    protected void doExecute(ValidateQueryRequest request, ActionListener<ValidateQueryResponse> listener) {
        request.nowInMillis = System.currentTimeMillis();
        super.doExecute(request, listener);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SEARCH;
    }

    @Override
    protected String transportAction() {
        return ValidateQueryAction.NAME;
    }

    @Override
    protected ValidateQueryRequest newRequest() {
        return new ValidateQueryRequest();
    }

    @Override
    protected ShardValidateQueryRequest newShardRequest() {
        return new ShardValidateQueryRequest();
    }

    @Override
    protected ShardValidateQueryRequest newShardRequest(int numShards, ShardRouting shard, ValidateQueryRequest request) {
        String[] filteringAliases = clusterService.state().metaData().filteringAliases(shard.index(), request.indices());
        return new ShardValidateQueryRequest(shard.index(), shard.id(), filteringAliases, request);
    }

    @Override
    protected ShardValidateQueryResponse newShardResponse() {
        return new ShardValidateQueryResponse();
    }

    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, ValidateQueryRequest request, String[] concreteIndices) {
        // Hard-code routing to limit request to a single shard, but still, randomize it...
        Map<String, Set<String>> routingMap = clusterState.metaData().resolveSearchRouting(Integer.toString(ThreadLocalRandom.current().nextInt(1000)), request.indices());
        return clusterService.operationRouting().searchShards(clusterState, request.indices(), concreteIndices, routingMap, "_local");
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, ValidateQueryRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, ValidateQueryRequest countRequest, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.READ, concreteIndices);
    }

    @Override
    protected ValidateQueryResponse newResponse(ValidateQueryRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;
        boolean valid = true;
        List<ShardOperationFailedException> shardFailures = null;
        List<QueryExplanation> queryExplanations = null;
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
                ShardValidateQueryResponse validateQueryResponse = (ShardValidateQueryResponse) shardResponse;
                valid = valid && validateQueryResponse.isValid();
                if (request.explain()) {
                    if (queryExplanations == null) {
                        queryExplanations = newArrayList();
                    }
                    queryExplanations.add(new QueryExplanation(
                            validateQueryResponse.getIndex(),
                            validateQueryResponse.isValid(),
                            validateQueryResponse.getExplanation(),
                            validateQueryResponse.getError()
                    ));
                }
                successfulShards++;
            }
        }
        return new ValidateQueryResponse(valid, queryExplanations, shardsResponses.length(), successfulShards, failedShards, shardFailures);
    }

    @Override
    protected ShardValidateQueryResponse shardOperation(ShardValidateQueryRequest request) throws ElasticsearchException {
        IndexQueryParserService queryParserService = indicesService.indexServiceSafe(request.index()).queryParserService();
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        IndexShard indexShard = indexService.shardSafe(request.shardId());

        boolean valid;
        String explanation = null;
        String error = null;

        DefaultSearchContext searchContext = new DefaultSearchContext(0,
                new ShardSearchRequest().types(request.types()).nowInMillis(request.nowInMillis())
                        .filteringAliases(request.filteringAliases()),
                null, indexShard.acquireSearcher("validate_query"), indexService, indexShard,
                scriptService, cacheRecycler, pageCacheRecycler, bigArrays
        );
        SearchContext.setCurrent(searchContext);
        try {
            if (request.source() != null && request.source().length() > 0) {
                searchContext.parsedQuery(queryParserService.parseQuery(request.source()));
            }
            searchContext.preProcess();

            valid = true;
            if (request.explain()) {
                explanation = searchContext.query().toString();
            }
        } catch (QueryParsingException e) {
            valid = false;
            error = e.getDetailedMessage();
        } catch (AssertionError e) {
            valid = false;
            error = e.getMessage();
        } finally {
            SearchContext.current().close();
            SearchContext.removeCurrent();
        }

        return new ShardValidateQueryResponse(request.index(), request.shardId(), valid, explanation, error);
    }
}
