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

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.*;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.action.SearchServiceListener;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.controller.ShardDoc;
import org.elasticsearch.search.controller.ShardScoreDoc;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.action.search.type.TransportSearchHelper.*;

/**
 * @author kimchy (shay.banon)
 */
public class TransportSearchScrollScanAction extends AbstractComponent {

    private final ThreadPool threadPool;

    private final ClusterService clusterService;

    private final SearchServiceTransportAction searchService;

    private final SearchPhaseController searchPhaseController;

    private final TransportSearchCache searchCache;

    @Inject public TransportSearchScrollScanAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
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

        private final Map<SearchShardTarget, QueryFetchSearchResult> queryFetchResults = searchCache.obtainQueryFetchResults();

        private final AtomicInteger successfulOps;

        private final AtomicInteger counter;

        private final long startTime = System.currentTimeMillis();

        private AsyncAction(SearchScrollRequest request, ParsedScrollId scrollId, ActionListener<SearchResponse> listener) {
            this.request = request;
            this.listener = listener;
            this.scrollId = scrollId;
            this.nodes = clusterService.state().nodes();
            this.successfulOps = new AtomicInteger(scrollId.context().length);
            this.counter = new AtomicInteger(scrollId.context().length);
        }

        public void start() {
            if (scrollId.context().length == 0) {
                final InternalSearchResponse internalResponse = new InternalSearchResponse(new InternalSearchHits(InternalSearchHits.EMPTY, Long.parseLong(this.scrollId.attributes().get("total_hits")), 0.0f), null, false);
                searchCache.releaseQueryFetchResults(queryFetchResults);
                listener.onResponse(new SearchResponse(internalResponse, request.scrollId(), 0, 0, 0l, TransportSearchHelper.buildShardFailures(shardFailures, searchCache)));
                return;
            }

            int localOperations = 0;
            for (Tuple<String, Long> target : scrollId.context()) {
                DiscoveryNode node = nodes.get(target.v1());
                if (node != null) {
                    if (nodes.localNodeId().equals(node.id())) {
                        localOperations++;
                    } else {
                        executePhase(node, target.v2());
                    }
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Node [" + target.v1() + "] not available for scroll request [" + scrollId.source() + "]");
                    }
                    successfulOps.decrementAndGet();
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
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
                                    executePhase(node, target.v2());
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
                                        executePhase(node, target.v2());
                                    }
                                });
                            } else {
                                executePhase(node, target.v2());
                            }
                        }
                    }
                }
            }

            for (Tuple<String, Long> target : scrollId.context()) {
                DiscoveryNode node = nodes.get(target.v1());
                if (node == null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Node [" + target.v1() + "] not available for scroll request [" + scrollId.source() + "]");
                    }
                    successfulOps.decrementAndGet();
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                } else {
                }
            }
        }

        private void executePhase(DiscoveryNode node, final long searchId) {
            searchService.sendExecuteScan(node, internalScrollSearchRequest(searchId, request), new SearchServiceListener<QueryFetchSearchResult>() {
                @Override public void onResult(QueryFetchSearchResult result) {
                    queryFetchResults.put(result.shardTarget(), result);
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                @Override public void onFailure(Throwable t) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("[{}] Failed to execute query phase", t, searchId);
                    }
                    shardFailures.add(new ShardSearchFailure(t));
                    successfulOps.decrementAndGet();
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
                ReduceSearchPhaseException failure = new ReduceSearchPhaseException("fetch", "", e, buildShardFailures(shardFailures, searchCache));
                if (logger.isDebugEnabled()) {
                    logger.debug("failed to reduce search", failure);
                }
                listener.onFailure(failure);
            } finally {
                searchCache.releaseQueryFetchResults(queryFetchResults);
            }
        }

        private void innerFinishHim() throws IOException {
            int numberOfHits = 0;
            for (QueryFetchSearchResult shardResult : queryFetchResults.values()) {
                numberOfHits += shardResult.queryResult().topDocs().scoreDocs.length;
            }
            ShardDoc[] docs = new ShardDoc[numberOfHits];
            int counter = 0;
            for (QueryFetchSearchResult shardResult : queryFetchResults.values()) {
                ScoreDoc[] scoreDocs = shardResult.queryResult().topDocs().scoreDocs;
                for (ScoreDoc scoreDoc : scoreDocs) {
                    docs[counter++] = new ShardScoreDoc(shardResult.shardTarget(), scoreDoc.doc, 0.0f);
                }
            }
            final InternalSearchResponse internalResponse = searchPhaseController.merge(docs, queryFetchResults, queryFetchResults);
            ((InternalSearchHits) internalResponse.hits()).totalHits = Long.parseLong(this.scrollId.attributes().get("total_hits"));


            for (QueryFetchSearchResult shardResult : queryFetchResults.values()) {
                if (shardResult.queryResult().topDocs().scoreDocs.length < shardResult.queryResult().size()) {
                    // we found more than we want for this round, remove this from our scrolling
                    queryFetchResults.remove(shardResult.shardTarget());
                }
            }

            String scrollId = null;
            if (request.scroll() != null) {
                // we rebuild the scroll id since we remove shards that we finished scrolling on
                scrollId = TransportSearchHelper.buildScrollId(this.scrollId.type(), queryFetchResults.values(), this.scrollId.attributes()); // continue moving the total_hits
            }
            listener.onResponse(new SearchResponse(internalResponse, scrollId, this.scrollId.context().length, successfulOps.get(),
                    System.currentTimeMillis() - startTime, buildShardFailures(shardFailures, searchCache)));
        }
    }
}