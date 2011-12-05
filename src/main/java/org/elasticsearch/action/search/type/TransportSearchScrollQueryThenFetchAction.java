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
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.trove.ExtTIntArrayList;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.action.SearchServiceListener;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.controller.ShardDoc;
import org.elasticsearch.search.fetch.FetchSearchRequest;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.QuerySearchResultProvider;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.action.search.type.TransportSearchHelper.*;

/**
 * @author kimchy (shay.banon)
 */
public class TransportSearchScrollQueryThenFetchAction extends AbstractComponent {

    private final ThreadPool threadPool;

    private final ClusterService clusterService;

    private final SearchServiceTransportAction searchService;

    private final SearchPhaseController searchPhaseController;

    private final TransportSearchCache searchCache;

    @Inject public TransportSearchScrollQueryThenFetchAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                                             TransportSearchCache searchCache,
                                                             SearchServiceTransportAction searchService, SearchPhaseController searchPhaseController) {
        super(settings);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.searchCache = searchCache;
        this.searchService = searchService;
        this.searchPhaseController = searchPhaseController;
    }

    public void execute(SearchScrollRequest request, ParsedScrollId scrollId, ActionListener<SearchResponse> listener) {
        new AsyncAction(request, scrollId, listener).start();
    }

    private class AsyncAction {

        private final SearchScrollRequest request;

        private final ActionListener<SearchResponse> listener;

        private final ParsedScrollId scrollId;

        private final DiscoveryNodes nodes;

        protected final Collection<ShardSearchFailure> shardFailures = searchCache.obtainShardFailures();

        private final Map<SearchShardTarget, QuerySearchResultProvider> queryResults = searchCache.obtainQueryResults();

        private final Map<SearchShardTarget, FetchSearchResult> fetchResults = searchCache.obtainFetchResults();

        private volatile ShardDoc[] sortedShardList;

        private final AtomicInteger successfulOps;

        private final long startTime = System.currentTimeMillis();

        private AsyncAction(SearchScrollRequest request, ParsedScrollId scrollId, ActionListener<SearchResponse> listener) {
            this.request = request;
            this.listener = listener;
            this.scrollId = scrollId;
            this.nodes = clusterService.state().nodes();
            this.successfulOps = new AtomicInteger(scrollId.context().length);
        }

        public void start() {
            if (scrollId.context().length == 0) {
                listener.onFailure(new SearchPhaseExecutionException("query", "no nodes to search on", null));
                return;
            }
            final AtomicInteger counter = new AtomicInteger(scrollId.context().length);

            int localOperations = 0;
            for (Tuple<String, Long> target : scrollId.context()) {
                DiscoveryNode node = nodes.get(target.v1());
                if (node != null) {
                    if (nodes.localNodeId().equals(node.id())) {
                        localOperations++;
                    } else {
                        executeQueryPhase(counter, node, target.v2());
                    }
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Node [" + target.v1() + "] not available for scroll request [" + scrollId.source() + "]");
                    }
                    successfulOps.decrementAndGet();
                    if (counter.decrementAndGet() == 0) {
                        executeFetchPhase();
                    }
                }
            }

            if (localOperations > 0) {
                if (request.operationThreading() == SearchOperationThreading.SINGLE_THREAD) {
                    threadPool.executor(ThreadPool.Names.SEARCH).execute(new Runnable() {
                        @Override public void run() {
                            for (Tuple<String, Long> target : scrollId.context()) {
                                DiscoveryNode node = nodes.get(target.v1());
                                if (node != null && nodes.localNodeId().equals(node.id())) {
                                    executeQueryPhase(counter, node, target.v2());
                                }
                            }
                        }
                    });
                } else {
                    boolean localAsync = request.operationThreading() == SearchOperationThreading.THREAD_PER_SHARD;
                    for (final Tuple<String, Long> target : scrollId.context()) {
                        final DiscoveryNode node = nodes.get(target.v1());
                        if (node != null && nodes.localNodeId().equals(node.id())) {
                            if (localAsync) {
                                threadPool.executor(ThreadPool.Names.SEARCH).execute(new Runnable() {
                                    @Override public void run() {
                                        executeQueryPhase(counter, node, target.v2());
                                    }
                                });
                            } else {
                                executeQueryPhase(counter, node, target.v2());
                            }
                        }
                    }
                }
            }
        }

        private void executeQueryPhase(final AtomicInteger counter, DiscoveryNode node, final long searchId) {
            searchService.sendExecuteQuery(node, internalScrollSearchRequest(searchId, request), new SearchServiceListener<QuerySearchResult>() {
                @Override public void onResult(QuerySearchResult result) {
                    queryResults.put(result.shardTarget(), result);
                    if (counter.decrementAndGet() == 0) {
                        executeFetchPhase();
                    }
                }

                @Override public void onFailure(Throwable t) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("[{}] Failed to execute query phase", t, searchId);
                    }
                    shardFailures.add(new ShardSearchFailure(t));
                    successfulOps.decrementAndGet();
                    if (counter.decrementAndGet() == 0) {
                        executeFetchPhase();
                    }
                }
            });
        }

        private void executeFetchPhase() {
            sortedShardList = searchPhaseController.sortDocs(queryResults.values());
            Map<SearchShardTarget, ExtTIntArrayList> docIdsToLoad = searchPhaseController.docIdsToLoad(sortedShardList);

            if (docIdsToLoad.isEmpty()) {
                finishHim();
            }

            final AtomicInteger counter = new AtomicInteger(docIdsToLoad.size());

            for (final Map.Entry<SearchShardTarget, ExtTIntArrayList> entry : docIdsToLoad.entrySet()) {
                SearchShardTarget shardTarget = entry.getKey();
                ExtTIntArrayList docIds = entry.getValue();
                FetchSearchRequest fetchSearchRequest = new FetchSearchRequest(queryResults.get(shardTarget).id(), docIds);
                DiscoveryNode node = nodes.get(shardTarget.nodeId());
                searchService.sendExecuteFetch(node, fetchSearchRequest, new SearchServiceListener<FetchSearchResult>() {
                    @Override public void onResult(FetchSearchResult result) {
                        result.shardTarget(entry.getKey());
                        fetchResults.put(result.shardTarget(), result);
                        if (counter.decrementAndGet() == 0) {
                            finishHim();
                        }
                    }

                    @Override public void onFailure(Throwable t) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Failed to execute fetch phase", t);
                        }
                        successfulOps.decrementAndGet();
                        if (counter.decrementAndGet() == 0) {
                            finishHim();
                        }
                    }
                });
            }
        }

        private void finishHim() {
            try {
                innerFinishHim();
            } catch (Exception e) {
                listener.onFailure(new ReduceSearchPhaseException("fetch", "", e, buildShardFailures(shardFailures, searchCache)));
            }
        }

        private void innerFinishHim() {
            InternalSearchResponse internalResponse = searchPhaseController.merge(sortedShardList, queryResults, fetchResults);
            String scrollId = null;
            if (request.scroll() != null) {
                scrollId = request.scrollId();
            }
            listener.onResponse(new SearchResponse(internalResponse, scrollId, this.scrollId.context().length, successfulOps.get(),
                    System.currentTimeMillis() - startTime, buildShardFailures(shardFailures, searchCache)));
            searchCache.releaseQueryResults(queryResults);
            searchCache.releaseFetchResults(fetchResults);
        }
    }
}
