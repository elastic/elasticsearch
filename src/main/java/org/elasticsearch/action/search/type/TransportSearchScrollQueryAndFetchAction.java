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
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.action.SearchServiceListener;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.action.search.type.TransportSearchHelper.internalScrollSearchRequest;

/**
 *
 */
public class TransportSearchScrollQueryAndFetchAction extends AbstractComponent {

    private final ThreadPool threadPool;

    private final ClusterService clusterService;

    private final SearchServiceTransportAction searchService;

    private final SearchPhaseController searchPhaseController;

    @Inject
    public TransportSearchScrollQueryAndFetchAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                                    SearchServiceTransportAction searchService, SearchPhaseController searchPhaseController) {
        super(settings);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
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

        private volatile AtomicArray<ShardSearchFailure> shardFailures;
        private final AtomicArray<QueryFetchSearchResult> queryFetchResults;

        private final AtomicInteger successfulOps;
        private final AtomicInteger counter;

        private final long startTime = System.currentTimeMillis();

        private AsyncAction(SearchScrollRequest request, ParsedScrollId scrollId, ActionListener<SearchResponse> listener) {
            this.request = request;
            this.listener = listener;
            this.scrollId = scrollId;
            this.nodes = clusterService.state().nodes();
            this.successfulOps = new AtomicInteger(scrollId.getContext().length);
            this.counter = new AtomicInteger(scrollId.getContext().length);

            this.queryFetchResults = new AtomicArray<QueryFetchSearchResult>(scrollId.getContext().length);
        }

        protected final ShardSearchFailure[] buildShardFailures() {
            if (shardFailures == null) {
                return ShardSearchFailure.EMPTY_ARRAY;
            }
            List<AtomicArray.Entry<ShardSearchFailure>> entries = shardFailures.asList();
            ShardSearchFailure[] failures = new ShardSearchFailure[entries.size()];
            for (int i = 0; i < failures.length; i++) {
                failures[i] = entries.get(i).value;
            }
            return failures;
        }

        // we do our best to return the shard failures, but its ok if its not fully concurrently safe
        // we simply try and return as much as possible
        protected final void addShardFailure(final int shardIndex, ShardSearchFailure failure) {
            if (shardFailures == null) {
                shardFailures = new AtomicArray<ShardSearchFailure>(scrollId.getContext().length);
            }
            shardFailures.set(shardIndex, failure);
        }

        public void start() {
            if (scrollId.getContext().length == 0) {
                listener.onFailure(new SearchPhaseExecutionException("query", "no nodes to search on", null));
                return;
            }

            int localOperations = 0;
            Tuple<String, Long>[] context = scrollId.getContext();
            for (int i = 0; i < context.length; i++) {
                Tuple<String, Long> target = context[i];
                DiscoveryNode node = nodes.get(target.v1());
                if (node != null) {
                    if (nodes.localNodeId().equals(node.id())) {
                        localOperations++;
                    } else {
                        executePhase(i, node, target.v2());
                    }
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Node [" + target.v1() + "] not available for scroll request [" + scrollId.getSource() + "]");
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
                        @Override
                        public void run() {
                            Tuple<String, Long>[] context1 = scrollId.getContext();
                            for (int i = 0; i < context1.length; i++) {
                                Tuple<String, Long> target = context1[i];
                                DiscoveryNode node = nodes.get(target.v1());
                                if (node != null && nodes.localNodeId().equals(node.id())) {
                                    executePhase(i, node, target.v2());
                                }
                            }
                        }
                    });
                } else {
                    boolean localAsync = request.operationThreading() == SearchOperationThreading.THREAD_PER_SHARD;
                    Tuple<String, Long>[] context1 = scrollId.getContext();
                    for (int i = 0; i < context1.length; i++) {
                        final Tuple<String, Long> target = context1[i];
                        final int shardIndex = i;
                        final DiscoveryNode node = nodes.get(target.v1());
                        if (node != null && nodes.localNodeId().equals(node.id())) {
                            try {
                                if (localAsync) {
                                    threadPool.executor(ThreadPool.Names.SEARCH).execute(new Runnable() {
                                        @Override
                                        public void run() {
                                            executePhase(shardIndex, node, target.v2());
                                        }
                                    });
                                } else {
                                    executePhase(shardIndex, node, target.v2());
                                }
                            } catch (Throwable t) {
                                onPhaseFailure(t, target.v2(), shardIndex);
                            }
                        }
                    }
                }
            }

            for (Tuple<String, Long> target : scrollId.getContext()) {
                DiscoveryNode node = nodes.get(target.v1());
                if (node == null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Node [" + target.v1() + "] not available for scroll request [" + scrollId.getSource() + "]");
                    }
                    successfulOps.decrementAndGet();
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                } else {
                }
            }
        }

        void executePhase(final int shardIndex, DiscoveryNode node, final long searchId) {
            searchService.sendExecuteFetch(node, internalScrollSearchRequest(searchId, request), new SearchServiceListener<QueryFetchSearchResult>() {
                @Override
                public void onResult(QueryFetchSearchResult result) {
                    queryFetchResults.set(shardIndex, result);
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    onPhaseFailure(t, searchId, shardIndex);
                }
            });
        }

        private void onPhaseFailure(Throwable t, long searchId, int shardIndex) {
            if (logger.isDebugEnabled()) {
                logger.debug("[{}] Failed to execute query phase", t, searchId);
            }
            addShardFailure(shardIndex, new ShardSearchFailure(t));
            successfulOps.decrementAndGet();
            if (counter.decrementAndGet() == 0) {
                finishHim();
            }
        }

        private void finishHim() {
            try {
                innerFinishHim();
            } catch (Throwable e) {
                listener.onFailure(new ReduceSearchPhaseException("fetch", "", e, buildShardFailures()));
            }
        }

        private void innerFinishHim() {
            ScoreDoc[] sortedShardList = searchPhaseController.sortDocs(queryFetchResults);
            final InternalSearchResponse internalResponse = searchPhaseController.merge(sortedShardList, queryFetchResults, queryFetchResults);
            String scrollId = null;
            if (request.scroll() != null) {
                scrollId = request.scrollId();
            }
            listener.onResponse(new SearchResponse(internalResponse, scrollId, this.scrollId.getContext().length, successfulOps.get(),
                    System.currentTimeMillis() - startTime, buildShardFailures()));
        }
    }
}
