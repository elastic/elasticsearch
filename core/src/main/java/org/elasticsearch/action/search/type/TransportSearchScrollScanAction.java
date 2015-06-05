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

package org.elasticsearch.action.search.type;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.ReduceSearchPhaseException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.fetch.ScrollQueryFetchSearchResult;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.action.search.type.TransportSearchHelper.internalScrollSearchRequest;

/**
 *
 */
public class TransportSearchScrollScanAction extends AbstractComponent {

    private final ClusterService clusterService;
    private final SearchServiceTransportAction searchService;
    private final SearchPhaseController searchPhaseController;

    @Inject
    public TransportSearchScrollScanAction(Settings settings, ClusterService clusterService,
                                           SearchServiceTransportAction searchService, SearchPhaseController searchPhaseController) {
        super(settings);
        this.clusterService = clusterService;
        this.searchService = searchService;
        this.searchPhaseController = searchPhaseController;
    }

    public void execute(SearchScrollRequest request, ParsedScrollId scrollId, ActionListener<SearchResponse> listener) {
        new AsyncAction(request, scrollId, listener).start();
    }

    private class AsyncAction extends AbstractAsyncAction {

        private final SearchScrollRequest request;

        private final ActionListener<SearchResponse> listener;

        private final ParsedScrollId scrollId;

        private final DiscoveryNodes nodes;

        private volatile AtomicArray<ShardSearchFailure> shardFailures;
        private final AtomicArray<QueryFetchSearchResult> queryFetchResults;

        private final AtomicInteger successfulOps;
        private final AtomicInteger counter;

        private AsyncAction(SearchScrollRequest request, ParsedScrollId scrollId, ActionListener<SearchResponse> listener) {
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
                final InternalSearchResponse internalResponse = new InternalSearchResponse(new InternalSearchHits(InternalSearchHits.EMPTY, Long.parseLong(this.scrollId.getAttributes().get("total_hits")), 0.0f), null, null, false, null);
                listener.onResponse(new SearchResponse(internalResponse, request.scrollId(), 0, 0, 0l, buildShardFailures()));
                return;
            }

            Tuple<String, Long>[] context = scrollId.getContext();
            for (int i = 0; i < context.length; i++) {
                Tuple<String, Long> target = context[i];
                DiscoveryNode node = nodes.get(target.v1());
                if (node != null) {
                    executePhase(i, node, target.v2());
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
            searchService.sendExecuteScan(node, internalScrollSearchRequest(searchId, request), new ActionListener<ScrollQueryFetchSearchResult>() {
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

        void onPhaseFailure(Throwable t, long searchId, int shardIndex) {
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
                ReduceSearchPhaseException failure = new ReduceSearchPhaseException("fetch", "", e, buildShardFailures());
                if (logger.isDebugEnabled()) {
                    logger.debug("failed to reduce search", failure);
                }
                listener.onFailure(failure);
            }
        }

        private void innerFinishHim() throws IOException {
            int numberOfHits = 0;
            for (AtomicArray.Entry<QueryFetchSearchResult> entry : queryFetchResults.asList()) {
                numberOfHits += entry.value.queryResult().topDocs().scoreDocs.length;
            }
            ScoreDoc[] docs = new ScoreDoc[numberOfHits];
            int counter = 0;
            for (AtomicArray.Entry<QueryFetchSearchResult> entry : queryFetchResults.asList()) {
                ScoreDoc[] scoreDocs = entry.value.queryResult().topDocs().scoreDocs;
                for (ScoreDoc scoreDoc : scoreDocs) {
                    scoreDoc.shardIndex = entry.index;
                    docs[counter++] = scoreDoc;
                }
            }
            final InternalSearchResponse internalResponse = searchPhaseController.merge(docs, queryFetchResults, queryFetchResults);
            ((InternalSearchHits) internalResponse.hits()).totalHits = Long.parseLong(this.scrollId.getAttributes().get("total_hits"));


            for (AtomicArray.Entry<QueryFetchSearchResult> entry : queryFetchResults.asList()) {
                if (entry.value.queryResult().topDocs().scoreDocs.length < entry.value.queryResult().size()) {
                    // we found more than we want for this round, remove this from our scrolling
                    queryFetchResults.set(entry.index, null);
                }
            }

            String scrollId = null;
            if (request.scroll() != null) {
                // we rebuild the scroll id since we remove shards that we finished scrolling on
                scrollId = TransportSearchHelper.buildScrollId(this.scrollId.getType(), queryFetchResults, this.scrollId.getAttributes()); // continue moving the total_hits
            }
            listener.onResponse(new SearchResponse(internalResponse, scrollId, this.scrollId.getContext().length, successfulOps.get(),
                    buildTookInMillis(), buildShardFailures()));
        }
    }
}
