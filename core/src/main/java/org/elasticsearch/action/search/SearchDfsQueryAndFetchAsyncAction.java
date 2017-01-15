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

package org.elasticsearch.action.search;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.search.query.QuerySearchRequest;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

class SearchDfsQueryAndFetchAsyncAction extends AbstractSearchAsyncAction<DfsSearchResult> {

    private final AtomicArray<QueryFetchSearchResult> queryFetchResults;
    private final SearchPhaseController searchPhaseController;
    SearchDfsQueryAndFetchAsyncAction(Logger logger, SearchTransportService searchTransportService,
                                      Function<String, DiscoveryNode> nodeIdToDiscoveryNode,
                                      Map<String, AliasFilter> aliasFilter, Map<String, Float> concreteIndexBoosts,
                                      SearchPhaseController searchPhaseController, Executor executor, SearchRequest request,
                                      ActionListener<SearchResponse> listener,  GroupShardsIterator shardsIts,
                                      long startTime, long clusterStateVersion, SearchTask task) {
        super(logger, searchTransportService, nodeIdToDiscoveryNode, aliasFilter, concreteIndexBoosts, executor,
                request, listener, shardsIts, startTime, clusterStateVersion, task);
        this.searchPhaseController = searchPhaseController;
        queryFetchResults = new AtomicArray<>(firstResults.length());
    }

    @Override
    protected String firstPhaseName() {
        return "dfs";
    }

    @Override
    protected void sendExecuteFirstPhase(DiscoveryNode node, ShardSearchTransportRequest request,
                                         ActionListener<DfsSearchResult> listener) {
        searchTransportService.sendExecuteDfs(node, request, task, listener);
    }

    @Override
    protected void moveToSecondPhase() {
        final AggregatedDfs dfs = searchPhaseController.aggregateDfs(firstResults);
        final AtomicInteger counter = new AtomicInteger(firstResults.asList().size());

        for (final AtomicArray.Entry<DfsSearchResult> entry : firstResults.asList()) {
            DfsSearchResult dfsResult = entry.value;
            DiscoveryNode node = nodeIdToDiscoveryNode.apply(dfsResult.shardTarget().getNodeId());
            QuerySearchRequest querySearchRequest = new QuerySearchRequest(request, dfsResult.id(), dfs);
            executeSecondPhase(entry.index, dfsResult, counter, node, querySearchRequest);
        }
    }

    void executeSecondPhase(final int shardIndex, final DfsSearchResult dfsResult, final AtomicInteger counter,
                            final DiscoveryNode node, final QuerySearchRequest querySearchRequest) {
        searchTransportService.sendExecuteFetch(node, querySearchRequest, task, new ActionListener<QueryFetchSearchResult>() {
            @Override
            public void onResponse(QueryFetchSearchResult result) {
                result.shardTarget(dfsResult.shardTarget());
                queryFetchResults.set(shardIndex, result);
                if (counter.decrementAndGet() == 0) {
                    finishHim();
                }
            }

            @Override
            public void onFailure(Exception t) {
                try {
                    onSecondPhaseFailure(t, querySearchRequest, shardIndex, dfsResult, counter);
                } finally {
                    // the query might not have been executed at all (for example because thread pool rejected execution)
                    // and the search context that was created in dfs phase might not be released.
                    // release it again to be in the safe side
                    sendReleaseSearchContext(querySearchRequest.id(), node);
                }
            }
        });
    }

    void onSecondPhaseFailure(Exception e, QuerySearchRequest querySearchRequest, int shardIndex, DfsSearchResult dfsResult,
                              AtomicInteger counter) {
        if (logger.isDebugEnabled()) {
            logger.debug((Supplier<?>) () -> new ParameterizedMessage("[{}] Failed to execute query phase", querySearchRequest.id()), e);
        }
        this.addShardFailure(shardIndex, dfsResult.shardTarget(), e);
        successfulOps.decrementAndGet();
        if (counter.decrementAndGet() == 0) {
            finishHim();
        }
    }

    private void finishHim() {
        getExecutor().execute(new ActionRunnable<SearchResponse>(listener) {
            @Override
            public void doRun() throws IOException {
                sortedShardDocs = searchPhaseController.sortDocs(true, queryFetchResults);
                final InternalSearchResponse internalResponse = searchPhaseController.merge(true, sortedShardDocs, queryFetchResults,
                    queryFetchResults);
                String scrollId = null;
                if (request.scroll() != null) {
                    scrollId = TransportSearchHelper.buildScrollId(request.searchType(), firstResults);
                }
                listener.onResponse(new SearchResponse(internalResponse, scrollId, expectedSuccessfulOps, successfulOps.get(),
                    buildTookInMillis(), buildShardFailures()));
            }

            @Override
            public void onFailure(Exception e) {
                ReduceSearchPhaseException failure = new ReduceSearchPhaseException("query_fetch", "", e, buildShardFailures());
                if (logger.isDebugEnabled()) {
                    logger.debug("failed to reduce search", failure);
                }
                super.onFailure(e);
            }
        });

    }
}
