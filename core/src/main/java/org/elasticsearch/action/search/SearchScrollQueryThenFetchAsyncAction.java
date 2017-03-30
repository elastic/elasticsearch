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

import com.carrotsearch.hppc.IntArrayList;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchRequest;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.ScrollQuerySearchResult;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.action.search.TransportSearchHelper.internalScrollSearchRequest;

final class SearchScrollQueryThenFetchAsyncAction extends AbstractAsyncAction {

    private final Logger logger;
    private final SearchTask task;
    private final SearchTransportService searchTransportService;
    private final SearchPhaseController searchPhaseController;
    private final SearchScrollRequest request;
    private final ActionListener<SearchResponse> listener;
    private final ParsedScrollId scrollId;
    private final DiscoveryNodes nodes;
    private volatile AtomicArray<ShardSearchFailure> shardFailures;
    final AtomicArray<QuerySearchResult> queryResults;
    final AtomicArray<FetchSearchResult> fetchResults;
    private volatile ScoreDoc[] sortedShardDocs;
    private final AtomicInteger successfulOps;

    SearchScrollQueryThenFetchAsyncAction(Logger logger, ClusterService clusterService, SearchTransportService searchTransportService,
                                          SearchPhaseController searchPhaseController, SearchScrollRequest request, SearchTask task,
                                          ParsedScrollId scrollId, ActionListener<SearchResponse> listener) {
        this.logger = logger;
        this.searchTransportService = searchTransportService;
        this.searchPhaseController = searchPhaseController;
        this.request = request;
        this.task = task;
        this.listener = listener;
        this.scrollId = scrollId;
        this.nodes = clusterService.state().nodes();
        this.successfulOps = new AtomicInteger(scrollId.getContext().length);
        this.queryResults = new AtomicArray<>(scrollId.getContext().length);
        this.fetchResults = new AtomicArray<>(scrollId.getContext().length);
    }

    private ShardSearchFailure[] buildShardFailures() {
        if (shardFailures == null) {
            return ShardSearchFailure.EMPTY_ARRAY;
        }
        List<ShardSearchFailure> failures = shardFailures.asList();
        return failures.toArray(new ShardSearchFailure[failures.size()]);
    }

    // we do our best to return the shard failures, but its ok if its not fully concurrently safe
    // we simply try and return as much as possible
    private void addShardFailure(final int shardIndex, ShardSearchFailure failure) {
        if (shardFailures == null) {
            shardFailures = new AtomicArray<>(scrollId.getContext().length);
        }
        shardFailures.set(shardIndex, failure);
    }

    public void start() {
        if (scrollId.getContext().length == 0) {
            listener.onFailure(new SearchPhaseExecutionException("query", "no nodes to search on", ShardSearchFailure.EMPTY_ARRAY));
            return;
        }
        final CountDown counter = new CountDown(scrollId.getContext().length);
        ScrollIdForNode[] context = scrollId.getContext();
        for (int i = 0; i < context.length; i++) {
            ScrollIdForNode target = context[i];
            DiscoveryNode node = nodes.get(target.getNode());
            if (node != null) {
                executeQueryPhase(i, counter, node, target.getScrollId());
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Node [{}] not available for scroll request [{}]", target.getNode(), scrollId.getSource());
                }
                successfulOps.decrementAndGet();
                if (counter.countDown()) {
                    try {
                        executeFetchPhase();
                    } catch (Exception e) {
                        listener.onFailure(new SearchPhaseExecutionException("query", "Fetch failed", e, ShardSearchFailure.EMPTY_ARRAY));
                        return;
                    }
                }
            }
        }
    }

    private void executeQueryPhase(final int shardIndex, final CountDown counter, DiscoveryNode node, final long searchId) {
        InternalScrollSearchRequest internalRequest = internalScrollSearchRequest(searchId, request);
        searchTransportService.sendExecuteScrollQuery(node, internalRequest, task,
            new SearchActionListener<ScrollQuerySearchResult>(null, shardIndex) {

            @Override
            protected void setSearchShardTarget(ScrollQuerySearchResult response) {
                // don't do this - it's part of the response...
                assert response.getSearchShardTarget() != null : "search shard target must not be null";
            }

                @Override
            protected void innerOnResponse(ScrollQuerySearchResult result) {
                queryResults.setOnce(result.getShardIndex(), result.queryResult());
                if (counter.countDown()) {
                    try {
                        executeFetchPhase();
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }
            }

            @Override
            public void onFailure(Exception t) {
                onQueryPhaseFailure(shardIndex, counter, searchId, t);
            }
        });
    }

    void onQueryPhaseFailure(final int shardIndex, final CountDown counter, final long searchId, Exception failure) {
        if (logger.isDebugEnabled()) {
            logger.debug((Supplier<?>) () -> new ParameterizedMessage("[{}] Failed to execute query phase", searchId), failure);
        }
        addShardFailure(shardIndex, new ShardSearchFailure(failure));
        successfulOps.decrementAndGet();
        if (counter.countDown()) {
            if (successfulOps.get() == 0) {
                listener.onFailure(new SearchPhaseExecutionException("query", "all shards failed", failure, buildShardFailures()));
            } else {
                try {
                    executeFetchPhase();
                } catch (Exception e) {
                    e.addSuppressed(failure);
                    listener.onFailure(new SearchPhaseExecutionException("query", "Fetch failed", e, ShardSearchFailure.EMPTY_ARRAY));
                }
            }
        }
    }

    private void executeFetchPhase() throws Exception {
        sortedShardDocs = searchPhaseController.sortDocs(true, queryResults.asList(), queryResults.length());
        if (sortedShardDocs.length == 0) {
            finishHim(searchPhaseController.reducedQueryPhase(queryResults.asList()));
            return;
        }

        final IntArrayList[] docIdsToLoad = searchPhaseController.fillDocIdsToLoad(queryResults.length(), sortedShardDocs);
        SearchPhaseController.ReducedQueryPhase reducedQueryPhase = searchPhaseController.reducedQueryPhase(queryResults.asList());
        final ScoreDoc[] lastEmittedDocPerShard = searchPhaseController.getLastEmittedDocPerShard(reducedQueryPhase, sortedShardDocs,
            queryResults.length());
        final CountDown counter = new CountDown(docIdsToLoad.length);
        for (int i = 0; i < docIdsToLoad.length; i++) {
            final int index = i;
            final IntArrayList docIds = docIdsToLoad[index];
            if (docIds != null) {
                final QuerySearchResult querySearchResult = queryResults.get(index);
                ScoreDoc lastEmittedDoc = lastEmittedDocPerShard[index];
                ShardFetchRequest shardFetchRequest = new ShardFetchRequest(querySearchResult.getRequestId(), docIds, lastEmittedDoc);
                DiscoveryNode node = nodes.get(querySearchResult.getSearchShardTarget().getNodeId());
                searchTransportService.sendExecuteFetchScroll(node, shardFetchRequest, task,
                    new SearchActionListener<FetchSearchResult>(querySearchResult.getSearchShardTarget(), index) {
                    @Override
                    protected void innerOnResponse(FetchSearchResult response) {
                        fetchResults.setOnce(response.getShardIndex(), response);
                        if (counter.countDown()) {
                            finishHim(reducedQueryPhase);
                        }
                    }

                    @Override
                    public void onFailure(Exception t) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Failed to execute fetch phase", t);
                        }
                        successfulOps.decrementAndGet();
                        if (counter.countDown()) {
                            finishHim(reducedQueryPhase);
                        }
                    }
                });
            } else {
                // the counter is set to the total size of docIdsToLoad which can have null values so we have to count them down too
                if (counter.countDown()) {
                    finishHim(reducedQueryPhase);
                }
            }
        }
    }

    private void finishHim(SearchPhaseController.ReducedQueryPhase queryPhase) {
        try {
            final InternalSearchResponse internalResponse = searchPhaseController.merge(true, sortedShardDocs, queryPhase, fetchResults);
            String scrollId = null;
            if (request.scroll() != null) {
                scrollId = request.scrollId();
            }
            listener.onResponse(new SearchResponse(internalResponse, scrollId, this.scrollId.getContext().length, successfulOps.get(),
                buildTookInMillis(), buildShardFailures()));
        } catch (Exception e) {
            listener.onFailure(new ReduceSearchPhaseException("fetch", "inner finish failed", e, buildShardFailures()));
        }
    }
}
