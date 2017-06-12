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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.InternalSearchResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.action.search.TransportSearchHelper.internalScrollSearchRequest;

/**
 * Abstract base class for scroll execution modes. This class encapsulates the basic logic to
 * fan out to nodes and execute the query part of the scroll request. Subclasses can for instance
 * run separate fetch phases etc.
 */
abstract class SearchScrollAsyncAction<T extends SearchPhaseResult> implements Runnable {
    /*
     * Some random TODO:
     * Today we still have a dedicated executing mode for scrolls while we could simplify this by implementing
     * scroll like functionality (mainly syntactic sugar) as an ordinary search with search_after. We could even go further and
     * make the scroll entirely stateless and encode the state per shard in the scroll ID.
     *
     * Today we also hold a context per shard but maybe
     * we want the context per coordinating node such that we route the scroll to the same coordinator all the time and hold the context
     * here? This would have the advantage that if we loose that node the entire scroll is deal not just one shard.
     *
     * Additionally there is the possibility to associate the scroll with a seq. id. such that we can talk to any replica as long as
     * the shards engine hasn't advanced that seq. id yet. Such a resume is possible and best effort, it could be even a safety net since
     * if you rely on indices being read-only things can change in-between without notification or it's hard to detect if there where any
     * changes while scrolling. These are all options to improve the current situation which we can look into down the road
     */
    protected final Logger logger;
    protected final ActionListener<SearchResponse> listener;
    protected final ParsedScrollId scrollId;
    protected final DiscoveryNodes nodes;
    protected final SearchPhaseController searchPhaseController;
    protected final SearchScrollRequest request;
    private final long startTime;
    private final List<ShardSearchFailure> shardFailures = new ArrayList<>();
    private final AtomicInteger successfulOps;

    protected SearchScrollAsyncAction(ParsedScrollId scrollId, Logger logger, DiscoveryNodes nodes,
                                      ActionListener<SearchResponse> listener, SearchPhaseController searchPhaseController,
                                      SearchScrollRequest request) {
        this.startTime = System.currentTimeMillis();
        this.scrollId = scrollId;
        this.successfulOps = new AtomicInteger(scrollId.getContext().length);
        this.logger = logger;
        this.listener = listener;
        this.nodes = nodes;
        this.searchPhaseController = searchPhaseController;
        this.request = request;
    }

    /**
     * Builds how long it took to execute the search.
     */
    private long buildTookInMillis() {
        // protect ourselves against time going backwards
        // negative values don't make sense and we want to be able to serialize that thing as a vLong
        return Math.max(1, System.currentTimeMillis() - startTime);
    }

    public final void run() {
        final ScrollIdForNode[] context = scrollId.getContext();
        if (context.length == 0) {
            listener.onFailure(new SearchPhaseExecutionException("query", "no nodes to search on", ShardSearchFailure.EMPTY_ARRAY));
            return;
        }
        final CountDown counter = new CountDown(scrollId.getContext().length);
        for (int i = 0; i < context.length; i++) {
            ScrollIdForNode target = context[i];
            DiscoveryNode node = nodes.get(target.getNode());
            final int shardIndex = i;
            if (node != null) { // it might happen that a node is going down in-between scrolls...
                InternalScrollSearchRequest internalRequest = internalScrollSearchRequest(target.getScrollId(), request);
                // we can't create a SearchShardTarget here since we don't know the index and shard ID we are talking to
                // we only know the node and the search context ID. Yet, the response will contain the SearchShardTarget
                // from the target node instead...that's why we pass null here
                SearchActionListener<T> searchActionListener = new SearchActionListener<T>(null, shardIndex) {

                    @Override
                    protected void setSearchShardTarget(T response) {
                        // don't do this - it's part of the response...
                        assert response.getSearchShardTarget() != null : "search shard target must not be null";
                    }

                    @Override
                    protected void innerOnResponse(T result) {
                        assert shardIndex == result.getShardIndex() : "shard index mismatch: " + shardIndex + " but got: "
                            + result.getShardIndex();
                        onFirstPhaseResult(shardIndex, result);
                        if (counter.countDown()) {
                            SearchPhase phase = moveToNextPhase();
                            try {
                                phase.run();
                            } catch (Exception e) {
                                // we need to fail the entire request here - the entire phase just blew up
                                // don't call onShardFailure or onFailure here since otherwise we'd countDown the counter
                                // again which would result in an exception
                                listener.onFailure(new SearchPhaseExecutionException(phase.getName(), "Phase failed", e,
                                    ShardSearchFailure.EMPTY_ARRAY));
                            }
                        }
                    }

                    @Override
                    public void onFailure(Exception t) {
                        onShardFailure("query", shardIndex, counter, target.getScrollId(), t, null,
                            SearchScrollAsyncAction.this::moveToNextPhase);
                    }
                };
                executeInitialPhase(node, internalRequest, searchActionListener);
            } else { // the node is not available we treat this as a shard failure here
                onShardFailure("query", shardIndex, counter, target.getScrollId(),
                    new IllegalStateException("node [" + target.getNode() + "] is not available"), null,
                    SearchScrollAsyncAction.this::moveToNextPhase);
            }
        }
    }

    synchronized ShardSearchFailure[] buildShardFailures() { // pkg private for testing
        if (shardFailures.isEmpty()) {
            return ShardSearchFailure.EMPTY_ARRAY;
        }
        return shardFailures.toArray(new ShardSearchFailure[shardFailures.size()]);
    }

    // we do our best to return the shard failures, but its ok if its not fully concurrently safe
    // we simply try and return as much as possible
    private synchronized void addShardFailure(ShardSearchFailure failure) {
        shardFailures.add(failure);
    }

    protected abstract void executeInitialPhase(DiscoveryNode node, InternalScrollSearchRequest internalRequest,
                                                SearchActionListener<T> searchActionListener);

    protected abstract SearchPhase moveToNextPhase();

    protected abstract void onFirstPhaseResult(int shardId, T result);

    protected SearchPhase sendResponsePhase(SearchPhaseController.ReducedQueryPhase queryPhase,
                                            final AtomicArray<? extends SearchPhaseResult> fetchResults) {
        return new SearchPhase("fetch") {
            @Override
            public void run() throws IOException {
                sendResponse(queryPhase, fetchResults);
            }
        };
    }

    protected final void sendResponse(SearchPhaseController.ReducedQueryPhase queryPhase,
                                      final AtomicArray<? extends SearchPhaseResult> fetchResults) {
        try {
            final InternalSearchResponse internalResponse = searchPhaseController.merge(true, queryPhase, fetchResults.asList(),
                fetchResults::get);
            // the scroll ID never changes we always return the same ID. This ID contains all the shards and their context ids
            // such that we can talk to them abgain in the next roundtrip.
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

    protected void onShardFailure(String phaseName, final int shardIndex, final CountDown counter, final long searchId, Exception failure,
                                @Nullable SearchShardTarget searchShardTarget,
                                Supplier<SearchPhase> nextPhaseSupplier) {
        if (logger.isDebugEnabled()) {
            logger.debug((Supplier<?>) () -> new ParameterizedMessage("[{}] Failed to execute {} phase", searchId, phaseName), failure);
        }
        addShardFailure(new ShardSearchFailure(failure, searchShardTarget));
        int successfulOperations = successfulOps.decrementAndGet();
        assert successfulOperations >= 0 : "successfulOperations must be >= 0 but was: " + successfulOperations;
        if (counter.countDown()) {
            if (successfulOps.get() == 0) {
                listener.onFailure(new SearchPhaseExecutionException(phaseName, "all shards failed", failure, buildShardFailures()));
            } else {
                SearchPhase phase = nextPhaseSupplier.get();
                try {
                    phase.run();
                } catch (Exception e) {
                    e.addSuppressed(failure);
                    listener.onFailure(new SearchPhaseExecutionException(phase.getName(), "Phase failed", e,
                        ShardSearchFailure.EMPTY_ARRAY));
                }
            }
        }
    }
}
