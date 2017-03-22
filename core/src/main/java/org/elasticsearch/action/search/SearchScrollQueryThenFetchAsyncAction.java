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
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchRequest;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.ScrollQuerySearchResult;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.action.search.TransportSearchHelper.internalScrollSearchRequest;

class SearchScrollQueryThenFetchAsyncAction extends AbstractAsyncAction {

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
            shardFailures = new AtomicArray<>(scrollId.getContext().length);
        }
        shardFailures.set(shardIndex, failure);
    }

    public void start() {
        if (scrollId.getContext().length == 0) {
            listener.onFailure(new SearchPhaseExecutionException("query", "no nodes to search on", ShardSearchFailure.EMPTY_ARRAY));
            return;
        }
        final AtomicInteger counter = new AtomicInteger(scrollId.getContext().length);

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
                if (counter.decrementAndGet() == 0) {
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

    private void executeQueryPhase(final int shardIndex, final AtomicInteger counter, DiscoveryNode node, final long searchId) {
        InternalScrollSearchRequest internalRequest = internalScrollSearchRequest(searchId, request);
        searchTransportService.sendExecuteQuery(node, internalRequest, task, new ActionListener<ScrollQuerySearchResult>() {
            @Override
            public void onResponse(ScrollQuerySearchResult result) {
                queryResults.set(shardIndex, result.queryResult());
                if (counter.decrementAndGet() == 0) {
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

    void onQueryPhaseFailure(final int shardIndex, final AtomicInteger counter, final long searchId, Exception failure) {
        if (logger.isDebugEnabled()) {
            logger.debug((Supplier<?>) () -> new ParameterizedMessage("[{}] Failed to execute query phase", searchId), failure);
        }
        addShardFailure(shardIndex, new ShardSearchFailure(failure));
        successfulOps.decrementAndGet();
        if (counter.decrementAndGet() == 0) {
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
        sortedShardDocs = searchPhaseController.sortDocs(true, queryResults);
        if (sortedShardDocs.length == 0) {
            finishHim(searchPhaseController.reducedQueryPhase(queryResults.asList()));
            return;
        }

        final IntArrayList[] docIdsToLoad = searchPhaseController.fillDocIdsToLoad(queryResults.length(), sortedShardDocs);
        SearchPhaseController.ReducedQueryPhase reducedQueryPhase = searchPhaseController.reducedQueryPhase(queryResults.asList());
        final ScoreDoc[] lastEmittedDocPerShard = searchPhaseController.getLastEmittedDocPerShard(reducedQueryPhase, sortedShardDocs,
            queryResults.length());
        final AtomicInteger counter = new AtomicInteger(docIdsToLoad.length);
        for (int i = 0; i < docIdsToLoad.length; i++) {
            final int index = i;
            final IntArrayList docIds = docIdsToLoad[index];
            if (docIds != null) {
                final QuerySearchResult querySearchResult = queryResults.get(index);
                ScoreDoc lastEmittedDoc = lastEmittedDocPerShard[index];
                ShardFetchRequest shardFetchRequest = new ShardFetchRequest(querySearchResult.id(), docIds, lastEmittedDoc);
                DiscoveryNode node = nodes.get(querySearchResult.shardTarget().getNodeId());
                searchTransportService.sendExecuteFetchScroll(node, shardFetchRequest, task, new ActionListener<FetchSearchResult>() {
                    @Override
                    public void onResponse(FetchSearchResult result) {
                        result.shardTarget(querySearchResult.shardTarget());
                        fetchResults.set(index, result);
                        if (counter.decrementAndGet() == 0) {
                            finishHim(reducedQueryPhase);
                        }
                    }

                    @Override
                    public void onFailure(Exception t) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Failed to execute fetch phase", t);
                        }
                        successfulOps.decrementAndGet();
                        if (counter.decrementAndGet() == 0) {
                            finishHim(reducedQueryPhase);
                        }
                    }
                });
            } else {
                // the counter is set to the total size of docIdsToLoad which can have null values so we have to count them down too
                if (counter.decrementAndGet() == 0) {
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
