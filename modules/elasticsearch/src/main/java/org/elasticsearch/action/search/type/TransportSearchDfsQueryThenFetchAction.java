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

import com.google.inject.Inject;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.*;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.action.SearchServiceListener;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchSearchRequest;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.internal.InternalSearchRequest;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.QuerySearchResultProvider;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.util.settings.Settings;
import org.elasticsearch.util.trove.ExtTIntArrayList;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author kimchy (Shay Banon)
 */
public class TransportSearchDfsQueryThenFetchAction extends TransportSearchTypeAction {

    @Inject public TransportSearchDfsQueryThenFetchAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, IndicesService indicesService,
                                                          TransportSearchCache transportSearchCache, SearchServiceTransportAction searchService, SearchPhaseController searchPhaseController) {
        super(settings, threadPool, clusterService, indicesService, transportSearchCache, searchService, searchPhaseController);
    }

    @Override protected void doExecute(SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        new AsyncAction(searchRequest, listener).start();
    }

    private class AsyncAction extends BaseAsyncAction<DfsSearchResult> {

        private final Collection<DfsSearchResult> dfsResults = searchCache.obtainDfsResults();

        private final Map<SearchShardTarget, QuerySearchResultProvider> queryResults = searchCache.obtainQueryResults();

        private final Map<SearchShardTarget, FetchSearchResult> fetchResults = searchCache.obtainFetchResults();


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
            for (DfsSearchResult dfsResult : dfsResults) {
                DiscoveryNode node = nodes.get(dfsResult.shardTarget().nodeId());
                if (node.id().equals(nodes.localNodeId())) {
                    localOperations++;
                } else {
                    QuerySearchRequest querySearchRequest = new QuerySearchRequest(dfsResult.id(), dfs);
                    executeQuery(counter, querySearchRequest, node);
                }
            }

            if (localOperations > 0) {
                if (request.operationThreading() == SearchOperationThreading.SINGLE_THREAD) {
                    threadPool.execute(new Runnable() {
                        @Override public void run() {
                            for (DfsSearchResult dfsResult : dfsResults) {
                                DiscoveryNode node = nodes.get(dfsResult.shardTarget().nodeId());
                                if (node.id().equals(nodes.localNodeId())) {
                                    QuerySearchRequest querySearchRequest = new QuerySearchRequest(dfsResult.id(), dfs);
                                    executeQuery(counter, querySearchRequest, node);
                                }
                            }
                        }
                    });
                } else {
                    boolean localAsync = request.operationThreading() == SearchOperationThreading.THREAD_PER_SHARD;
                    for (DfsSearchResult dfsResult : dfsResults) {
                        final DiscoveryNode node = nodes.get(dfsResult.shardTarget().nodeId());
                        if (node.id().equals(nodes.localNodeId())) {
                            final QuerySearchRequest querySearchRequest = new QuerySearchRequest(dfsResult.id(), dfs);
                            if (localAsync) {
                                threadPool.execute(new Runnable() {
                                    @Override public void run() {
                                        executeQuery(counter, querySearchRequest, node);
                                    }
                                });
                            } else {
                                executeQuery(counter, querySearchRequest, node);
                            }
                        }
                    }
                }
            }
        }

        private void executeQuery(final AtomicInteger counter, QuerySearchRequest querySearchRequest, DiscoveryNode node) {
            searchService.sendExecuteQuery(node, querySearchRequest, new SearchServiceListener<QuerySearchResult>() {
                @Override public void onResult(QuerySearchResult result) {
                    queryResults.put(result.shardTarget(), result);
                    if (counter.decrementAndGet() == 0) {
                        executeFetchPhase();
                    }
                }

                @Override public void onFailure(Throwable t) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Failed to execute query phase", t);
                    }
                    AsyncAction.this.shardFailures.add(new ShardSearchFailure(t));
                    successulOps.decrementAndGet();
                    if (counter.decrementAndGet() == 0) {
                        executeFetchPhase();
                    }
                }
            });
        }

        private void executeFetchPhase() {
            try {
                innerExecuteFetchPhase();
            } catch (Exception e) {
                invokeListener(new ReduceSearchPhaseException("query", "", e, buildShardFailures()));
            }
        }

        private void innerExecuteFetchPhase() {
            sortedShardList = searchPhaseController.sortDocs(queryResults.values());
            final Map<SearchShardTarget, ExtTIntArrayList> docIdsToLoad = searchPhaseController.docIdsToLoad(sortedShardList);

            if (docIdsToLoad.isEmpty()) {
                releaseIrrelevantSearchContexts(queryResults, docIdsToLoad);
                finishHim();
            }

            final AtomicInteger counter = new AtomicInteger(docIdsToLoad.size());
            int localOperations = 0;
            for (Map.Entry<SearchShardTarget, ExtTIntArrayList> entry : docIdsToLoad.entrySet()) {
                DiscoveryNode node = nodes.get(entry.getKey().nodeId());
                if (node.id().equals(nodes.localNodeId())) {
                    localOperations++;
                } else {
                    FetchSearchRequest fetchSearchRequest = new FetchSearchRequest(queryResults.get(entry.getKey()).id(), entry.getValue());
                    executeFetch(counter, fetchSearchRequest, node);
                }
            }

            if (localOperations > 0) {
                if (request.operationThreading() == SearchOperationThreading.SINGLE_THREAD) {
                    threadPool.execute(new Runnable() {
                        @Override public void run() {
                            for (Map.Entry<SearchShardTarget, ExtTIntArrayList> entry : docIdsToLoad.entrySet()) {
                                DiscoveryNode node = nodes.get(entry.getKey().nodeId());
                                if (node.id().equals(nodes.localNodeId())) {
                                    FetchSearchRequest fetchSearchRequest = new FetchSearchRequest(queryResults.get(entry.getKey()).id(), entry.getValue());
                                    executeFetch(counter, fetchSearchRequest, node);
                                }
                            }
                        }
                    });
                } else {
                    boolean localAsync = request.operationThreading() == SearchOperationThreading.THREAD_PER_SHARD;
                    for (Map.Entry<SearchShardTarget, ExtTIntArrayList> entry : docIdsToLoad.entrySet()) {
                        final DiscoveryNode node = nodes.get(entry.getKey().nodeId());
                        if (node.id().equals(nodes.localNodeId())) {
                            final FetchSearchRequest fetchSearchRequest = new FetchSearchRequest(queryResults.get(entry.getKey()).id(), entry.getValue());
                            if (localAsync) {
                                threadPool.execute(new Runnable() {
                                    @Override public void run() {
                                        executeFetch(counter, fetchSearchRequest, node);
                                    }
                                });
                            } else {
                                executeFetch(counter, fetchSearchRequest, node);
                            }
                        }
                    }
                }
            }

            releaseIrrelevantSearchContexts(queryResults, docIdsToLoad);
        }

        private void executeFetch(final AtomicInteger counter, FetchSearchRequest fetchSearchRequest, DiscoveryNode node) {
            searchService.sendExecuteFetch(node, fetchSearchRequest, new SearchServiceListener<FetchSearchResult>() {
                @Override public void onResult(FetchSearchResult result) {
                    fetchResults.put(result.shardTarget(), result);
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                @Override public void onFailure(Throwable t) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Failed to execute fetch phase", t);
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
                invokeListener(new ReduceSearchPhaseException("fetch", "", e, buildShardFailures()));
            }
        }

        private void innerFinishHim() {
            final InternalSearchResponse internalResponse = searchPhaseController.merge(sortedShardList, queryResults, fetchResults);
            String scrollId = null;
            if (request.scroll() != null) {
                scrollId = TransportSearchHelper.buildScrollId(request.searchType(), dfsResults);
            }
            searchCache.releaseDfsResults(dfsResults);
            searchCache.releaseQueryResults(queryResults);
            searchCache.releaseFetchResults(fetchResults);
            invokeListener(new SearchResponse(internalResponse, scrollId, expectedSuccessfulOps, successulOps.get(), buildShardFailures()));
        }
    }
}
