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
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.Node;
import org.elasticsearch.cluster.node.Nodes;
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
import org.elasticsearch.util.Tuple;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.settings.Settings;
import org.elasticsearch.util.trove.ExtTIntArrayList;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author kimchy (Shay Banon)
 */
public class TransportSearchScrollQueryThenFetchAction extends AbstractComponent {

    private final ClusterService clusterService;

    private final SearchServiceTransportAction searchService;

    private final SearchPhaseController searchPhaseController;

    private final TransportSearchCache transportSearchCache;

    @Inject public TransportSearchScrollQueryThenFetchAction(Settings settings, ClusterService clusterService,
                                                             TransportSearchCache transportSearchCache,
                                                             SearchServiceTransportAction searchService, SearchPhaseController searchPhaseController) {
        super(settings);
        this.clusterService = clusterService;
        this.transportSearchCache = transportSearchCache;
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

        private final Nodes nodes;

        private final Map<SearchShardTarget, QuerySearchResultProvider> queryResults = transportSearchCache.obtainQueryResults();

        private final Map<SearchShardTarget, FetchSearchResult> fetchResults = transportSearchCache.obtainFetchResults();

        private volatile ShardDoc[] sortedShardList;

        private final AtomicInteger successfulOps;

        private AsyncAction(SearchScrollRequest request, ParsedScrollId scrollId, ActionListener<SearchResponse> listener) {
            this.request = request;
            this.listener = listener;
            this.scrollId = scrollId;
            this.nodes = clusterService.state().nodes();
            this.successfulOps = new AtomicInteger(scrollId.values().length);
        }

        public void start() {
            final AtomicInteger counter = new AtomicInteger(scrollId.values().length);
            for (Tuple<String, Long> target : scrollId.values()) {
                Node node = nodes.get(target.v1());
                if (node == null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Node [" + target.v1() + "] not available for scroll request [" + scrollId.source() + "]");
                    }
                    successfulOps.decrementAndGet();
                    if (counter.decrementAndGet() == 0) {
                        executeFetchPhase();
                    }
                } else {
                    searchService.sendExecuteQuery(node, TransportSearchHelper.internalScrollSearchRequest(target.v2(), request), new SearchServiceListener<QuerySearchResult>() {
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
                            successfulOps.decrementAndGet();
                            if (counter.decrementAndGet() == 0) {
                                executeFetchPhase();
                            }
                        }
                    });
                }
            }
        }

        private void executeFetchPhase() {
            sortedShardList = searchPhaseController.sortDocs(queryResults.values());
            Map<SearchShardTarget, ExtTIntArrayList> docIdsToLoad = searchPhaseController.docIdsToLoad(sortedShardList);

            if (docIdsToLoad.isEmpty()) {
                finishHim();
            }

            final AtomicInteger counter = new AtomicInteger(docIdsToLoad.size());

            for (Map.Entry<SearchShardTarget, ExtTIntArrayList> entry : docIdsToLoad.entrySet()) {
                SearchShardTarget shardTarget = entry.getKey();
                ExtTIntArrayList docIds = entry.getValue();
                FetchSearchRequest fetchSearchRequest = new FetchSearchRequest(queryResults.get(shardTarget).id(), docIds);
                Node node = nodes.get(shardTarget.nodeId());
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
                        successfulOps.decrementAndGet();
                        if (counter.decrementAndGet() == 0) {
                            finishHim();
                        }
                    }
                });
            }
        }

        private void finishHim() {
            InternalSearchResponse internalResponse = searchPhaseController.merge(sortedShardList, queryResults, fetchResults);
            String scrollId = null;
            if (request.scroll() != null) {
                scrollId = TransportSearchHelper.buildScrollId(this.scrollId.type(), fetchResults.values());
            }
            transportSearchCache.releaseQueryResults(queryResults);
            transportSearchCache.releaseFetchResults(fetchResults);
            listener.onResponse(new SearchResponse(internalResponse, scrollId, this.scrollId.values().length, successfulOps.get()));
        }

    }
}
