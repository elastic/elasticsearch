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

package org.elasticsearch.action.termvectors.dfs;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.TransportBroadcastAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Get the dfs only with no fetch phase. This is for internal use only.
 */
public class TransportDfsOnlyAction extends TransportBroadcastAction<DfsOnlyRequest, DfsOnlyResponse, ShardDfsOnlyRequest, ShardDfsOnlyResponse> {

    public static final String NAME = "internal:index/termvectors/dfs";

    private final SearchService searchService;

    private final SearchPhaseController searchPhaseController;

    @Inject
    public TransportDfsOnlyAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                  ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, SearchService searchService, SearchPhaseController searchPhaseController) {
        super(settings, NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                DfsOnlyRequest::new, ShardDfsOnlyRequest::new, ThreadPool.Names.SEARCH);
        this.searchService = searchService;
        this.searchPhaseController = searchPhaseController;
    }

    @Override
    protected void doExecute(DfsOnlyRequest request, ActionListener<DfsOnlyResponse> listener) {
        request.nowInMillis = System.currentTimeMillis();
        super.doExecute(request, listener);
    }

    @Override
    protected ShardDfsOnlyRequest newShardRequest(int numShards, ShardRouting shard, DfsOnlyRequest request) {
        String[] filteringAliases = indexNameExpressionResolver.filteringAliases(clusterService.state(), shard.index(), request.indices());
        return new ShardDfsOnlyRequest(shard, numShards, filteringAliases, request.nowInMillis, request);
    }

    @Override
    protected ShardDfsOnlyResponse newShardResponse() {
        return new ShardDfsOnlyResponse();
    }

    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, DfsOnlyRequest request, String[] concreteIndices) {
        Map<String, Set<String>> routingMap = indexNameExpressionResolver.resolveSearchRouting(clusterState, request.routing(), request.indices());
        return clusterService.operationRouting().searchShards(clusterState, concreteIndices, routingMap, request.preference());
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, DfsOnlyRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, DfsOnlyRequest countRequest, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.READ, concreteIndices);
    }

    @Override
    protected DfsOnlyResponse newResponse(DfsOnlyRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;
        List<ShardOperationFailedException> shardFailures = null;
        AtomicArray<DfsSearchResult> dfsResults = new AtomicArray<>(shardsResponses.length());
        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                // simply ignore non active shards
            } else if (shardResponse instanceof BroadcastShardOperationFailedException) {
                failedShards++;
                if (shardFailures == null) {
                    shardFailures = new ArrayList<>();
                }
                shardFailures.add(new DefaultShardOperationFailedException((BroadcastShardOperationFailedException) shardResponse));
            } else {
                dfsResults.set(i, ((ShardDfsOnlyResponse) shardResponse).getDfsSearchResult());
                successfulShards++;
            }
        }
        AggregatedDfs dfs = searchPhaseController.aggregateDfs(dfsResults);
        return new DfsOnlyResponse(dfs, shardsResponses.length(), successfulShards, failedShards, shardFailures, buildTookInMillis(request));
    }

    @Override
    protected ShardDfsOnlyResponse shardOperation(ShardDfsOnlyRequest request) {
        DfsSearchResult dfsSearchResult = searchService.executeDfsPhase(request.getShardSearchRequest());
        searchService.freeContext(dfsSearchResult.id());
        return new ShardDfsOnlyResponse(request.shardId(), dfsSearchResult);
    }

    /**
     * Builds how long it took to execute the dfs request.
     */
    protected final long buildTookInMillis(DfsOnlyRequest request) {
        // protect ourselves against time going backwards
        // negative values don't make sense and we want to be able to serialize that thing as a vLong
        return Math.max(1, System.currentTimeMillis() - request.nowInMillis);
    }

}
