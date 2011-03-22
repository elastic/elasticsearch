/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.action.search.type;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.*;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.action.SearchServiceListener;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.internal.InternalSearchRequest;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.action.search.type.TransportSearchHelper.*;

/**
 * @author kimchy (shay.banon)
 */
public class TransportSearchDfsQueryAndFetchAction extends TransportSearchTypeAction {

    @Inject public TransportSearchDfsQueryAndFetchAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                                         TransportSearchCache transportSearchCache, SearchServiceTransportAction searchService, SearchPhaseController searchPhaseController) {
        super(settings, threadPool, clusterService, transportSearchCache, searchService, searchPhaseController);
    }

    @Override protected void doExecute(SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        new AsyncAction(searchRequest, listener).start();
    }

    private class AsyncAction extends BaseAsyncAction<DfsSearchResult> {

        private final Collection<DfsSearchResult> dfsResults = searchCache.obtainDfsResults();

        private final Map<SearchShardTarget, QueryFetchSearchResult> queryFetchResults = searchCache.obtainQueryFetchResults();


        private AsyncAction(SearchRequest request, ActionListener<SearchResponse> listener) {
            super(request, listener);
        }

        @Override protected String firstPhaseName() {
            return "dfs";
        }

        @Override protected void sendExecuteFirstPhase(DiscoveryNode node, InternalSearchRequest request, SearchServiceListener<DfsSearchResult> listener) {
            searchService.sendExecuteDfs(node, request, listener);
        }

        @Override protected void processFirstPhaseResult(ShardRouting shard, DfsSearchResult result) {
            dfsResults.add(result);
        }

        @Override protected void moveToSecondPhase() {
            final AggregatedDfs dfs = searchPhaseController.aggregateDfs(dfsResults);
            final AtomicInteger counter = new AtomicInteger(dfsResults.size());

            int localOperations = 0;
            for (final DfsSearchResult dfsResult : dfsResults) {
                DiscoveryNode node = nodes.get(dfsResult.shardTarget().nodeId());
                if (node.id().equals(nodes.localNodeId())) {
                    localOperations++;
                } else {
                    QuerySearchRequest querySearchRequest = new QuerySearchRequest(dfsResult.id(), dfs);
                    executeSecondPhase(dfsResult, counter, node, querySearchRequest);
                }
            }
            if (localOperations > 0) {
                if (request.operationThreading() == SearchOperationThreading.SINGLE_THREAD) {
                    threadPool.executor(ThreadPool.Names.SEARCH).execute(new Runnable() {
                        @Override public void run() {
                            for (final DfsSearchResult dfsResult : dfsResults) {
                                DiscoveryNode node = nodes.get(dfsResult.shardTarget().nodeId());
                                if (node.id().equals(nodes.localNodeId())) {
                                    QuerySearchRequest querySearchRequest = new QuerySearchRequest(dfsResult.id(), dfs);
                                    executeSecondPhase(dfsResult, counter, node, querySearchRequest);
                                }
                            }
                        }
                    });
                } else {
                    boolean localAsync = request.operationThreading() == SearchOperationThreading.THREAD_PER_SHARD;
                    for (final DfsSearchResult dfsResult : dfsResults) {
                        final DiscoveryNode node = nodes.get(dfsResult.shardTarget().nodeId());
                        if (node.id().equals(nodes.localNodeId())) {
                            final QuerySearchRequest querySearchRequest = new QuerySearchRequest(dfsResult.id(), dfs);
                            if (localAsync) {
                                threadPool.executor(ThreadPool.Names.SEARCH).execute(new Runnable() {
                                    @Override public void run() {
                                        executeSecondPhase(dfsResult, counter, node, querySearchRequest);
                                    }
                                });
                            } else {
                                executeSecondPhase(dfsResult, counter, node, querySearchRequest);
                            }
                        }
                    }
                }
            }
        }

        private void executeSecondPhase(final DfsSearchResult dfsResult, final AtomicInteger counter, DiscoveryNode node, final QuerySearchRequest querySearchRequest) {
            searchService.sendExecuteFetch(node, querySearchRequest, new SearchServiceListener<QueryFetchSearchResult>() {
                @Override public void onResult(QueryFetchSearchResult result) {
                    result.shardTarget(dfsResult.shardTarget());
                    queryFetchResults.put(result.shardTarget(), result);
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                @Override public void onFailure(Throwable t) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("[{}] Failed to execute query phase", t, querySearchRequest.id());
                    }
                    AsyncAction.this.shardFailures.add(new ShardSearchFailure(t));
                    successulOps.decrementAndGet();
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }
            });
        }

        private void finishHim() {
            try {
                innerFinishHim();
            } catch (Exception e) {
                ReduceSearchPhaseException failure = new ReduceSearchPhaseException("query_fetch", "", e, buildShardFailures());
                if (logger.isDebugEnabled()) {
                    logger.debug("failed to reduce search", failure);
                }
                listener.onFailure(failure);
            } finally {
                searchCache.releaseDfsResults(dfsResults);
                searchCache.releaseQueryFetchResults(queryFetchResults);
            }
        }

        private void innerFinishHim() throws Exception {
            sortedShardList = searchPhaseController.sortDocs(queryFetchResults.values());
            final InternalSearchResponse internalResponse = searchPhaseController.merge(sortedShardList, queryFetchResults, queryFetchResults);
            String scrollId = null;
            if (request.scroll() != null) {
                scrollId = buildScrollId(request.searchType(), dfsResults, null);
            }
            listener.onResponse(new SearchResponse(internalResponse, scrollId, expectedSuccessfulOps, successulOps.get(), buildTookInMillis(), buildShardFailures()));
        }
    }
}