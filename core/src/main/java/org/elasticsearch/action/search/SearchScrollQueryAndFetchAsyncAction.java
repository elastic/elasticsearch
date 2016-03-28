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

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.action.SearchTransportService;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.fetch.ScrollQueryFetchSearchResult;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.InternalSearchResponse;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.action.search.TransportSearchHelper.internalScrollSearchRequest;

class SearchScrollQueryAndFetchAsyncAction extends AbstractAsyncAction {

    private final ESLogger logger;
    private final SearchPhaseController searchPhaseController;
    private final SearchTransportService searchTransportService;
    private final SearchScrollRequest request;
    private final ActionListener<SearchResponse> listener;
    private final ParsedScrollId scrollId;
    private final DiscoveryNodes nodes;
    private volatile AtomicArray<ShardSearchFailure> shardFailures;
    private final AtomicArray<QueryFetchSearchResult> queryFetchResults;
    private final AtomicInteger successfulOps;
    private final AtomicInteger counter;

    SearchScrollQueryAndFetchAsyncAction(ESLogger logger, ClusterService clusterService,
                                         SearchTransportService searchTransportService, SearchPhaseController searchPhaseController,
                                         SearchScrollRequest request, ParsedScrollId scrollId, ActionListener<SearchResponse> listener) {
        this.logger = logger;
        this.searchPhaseController = searchPhaseController;
        this.searchTransportService = searchTransportService;
        this.request = request;
        this.listener = listener;
        this.scrollId = scrollId;
        this.nodes = clusterService.state().nodes();
        this.successfulOps = new AtomicInteger(scrollId.getContext().length);
        this.counter = new AtomicInteger(scrollId.getContext().length);

        this.queryFetchResults = new AtomicArray<>(scrollId.getContext().length);
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

        ScrollIdForNode[] context = scrollId.getContext();
        for (int i = 0; i < context.length; i++) {
            ScrollIdForNode target = context[i];
            DiscoveryNode node = nodes.get(target.getNode());
            if (node != null) {
                executePhase(i, node, target.getScrollId());
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Node [" + target.getNode() + "] not available for scroll request [" + scrollId.getSource() + "]");
                }
                successfulOps.decrementAndGet();
                if (counter.decrementAndGet() == 0) {
                    finishHim();
                }
            }
        }

        for (ScrollIdForNode target : scrollId.getContext()) {
            DiscoveryNode node = nodes.get(target.getNode());
            if (node == null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Node [" + target.getNode() + "] not available for scroll request [" + scrollId.getSource() + "]");
                }
                successfulOps.decrementAndGet();
                if (counter.decrementAndGet() == 0) {
                    finishHim();
                }
            }
        }
    }

    void executePhase(final int shardIndex, DiscoveryNode node, final long searchId) {
        InternalScrollSearchRequest internalRequest = internalScrollSearchRequest(searchId, request);
        searchTransportService.sendExecuteFetch(node, internalRequest, new ActionListener<ScrollQueryFetchSearchResult>() {
            @Override
            public void onResponse(ScrollQueryFetchSearchResult result) {
                queryFetchResults.set(shardIndex, result.result());
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
            if (successfulOps.get() == 0) {
                listener.onFailure(new SearchPhaseExecutionException("query_fetch", "all shards failed", t, buildShardFailures()));
            } else {
                finishHim();
            }
        }
    }

    private void finishHim() {
        try {
            innerFinishHim();
        } catch (Throwable e) {
            listener.onFailure(new ReduceSearchPhaseException("fetch", "", e, buildShardFailures()));
        }
    }

    private void innerFinishHim() throws Exception {
        ScoreDoc[] sortedShardList = searchPhaseController.sortDocs(true, queryFetchResults);
        final InternalSearchResponse internalResponse = searchPhaseController.merge(sortedShardList, queryFetchResults,
            queryFetchResults);
        String scrollId = null;
        if (request.scroll() != null) {
            scrollId = request.scrollId();
        }
        listener.onResponse(new SearchResponse(internalResponse, scrollId, this.scrollId.getContext().length, successfulOps.get(),
            buildTookInMillis(), buildShardFailures()));
    }
}
