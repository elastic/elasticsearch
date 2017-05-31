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
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.InternalSearchResponse;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.action.search.TransportSearchHelper.internalScrollSearchRequest;

/**
 * Abstract base class for scroll execution modes. This class encapsulates the basic logic to
 * fan out to nodes and execute the query part of the scroll request. Subclasses can for instance
 * run separate fetch phases etc.
 */
abstract class SearchScrollAsyncAction<T extends SearchPhaseResult> implements Runnable {
    protected final Logger logger;
    protected final ActionListener<SearchResponse> listener;
    protected final ParsedScrollId scrollId;
    protected final DiscoveryNodes nodes;
    protected final AtomicInteger successfulOps;
    protected final SearchPhaseController searchPhaseController;
    protected final SearchScrollRequest request;
    private final long startTime;
    private volatile AtomicArray<ShardSearchFailure> shardFailures; // we initialize this on-demand

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
                final int shardIndex = i;
                InternalScrollSearchRequest internalRequest = internalScrollSearchRequest(target.getScrollId(), request);
                SearchActionListener<T> searchActionListener = new SearchActionListener<T>(null, shardIndex) {

                    @Override
                    protected void setSearchShardTarget(T response) {
                        // don't do this - it's part of the response...
                        assert response.getSearchShardTarget() != null : "search shard target must not be null";
                    }

                    @Override
                    protected void innerOnResponse(T result) {
                        assert shardIndex == result.getShardIndex() : "shard index mismatch: "
                            + shardIndex + " but got: " + result.getShardIndex();
                        onFirstPhaseResult(shardIndex, result);
                        if (counter.countDown()) {
                            try {
                                moveToNextPhase();
                            } catch (Exception e) {
                                onFailure(e);
                            }
                        }
                    }

                    @Override
                    public void onFailure(Exception t) {
                        onInitialPhaseFailure(shardIndex, counter, target.getScrollId(), t);
                    }
                };
                executeInitialPhase(node, internalRequest, searchActionListener);
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Node [{}] not available for scroll request [{}]", target.getNode(), scrollId.getSource());
                }
                successfulOps.decrementAndGet();
                if (counter.countDown()) {
                    try {
                        moveToNextPhase();
                    } catch (RuntimeException e) {
                        listener.onFailure(new SearchPhaseExecutionException("query", "Fetch failed", e, ShardSearchFailure.EMPTY_ARRAY));
                        return;
                    }
                }
            }
        }
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
    protected void addShardFailure(final int shardIndex, ShardSearchFailure failure) {
        if (shardFailures == null) {
            shardFailures = new AtomicArray<>(scrollId.getContext().length);
        }
        shardFailures.set(shardIndex, failure);
    }

    protected abstract void executeInitialPhase(DiscoveryNode node, InternalScrollSearchRequest internalRequest,
                                                SearchActionListener<T> searchActionListener);

    protected abstract void moveToNextPhase();

    protected final void sendResponse(SearchPhaseController.ReducedQueryPhase queryPhase,
                                      final AtomicArray<? extends SearchPhaseResult> fetchResults) {
        try {
            final InternalSearchResponse internalResponse = searchPhaseController.merge(true, queryPhase, fetchResults.asList(),
                fetchResults::get);
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

    private final void onInitialPhaseFailure(final int shardIndex, final CountDown counter, final long searchId, Exception failure) {
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
                    moveToNextPhase();
                } catch (Exception e) {
                    e.addSuppressed(failure);
                    listener.onFailure(new SearchPhaseExecutionException("query", "Fetch failed", e, ShardSearchFailure.EMPTY_ARRAY));
                }
            }
        }
    }

    protected abstract void onFirstPhaseResult(int shardId, T result);
}
