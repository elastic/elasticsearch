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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Function;

class SearchQueryAndFetchAsyncAction extends AbstractSearchAsyncAction<QueryFetchSearchResult> {

    private final SearchPhaseController searchPhaseController;

    SearchQueryAndFetchAsyncAction(Logger logger, SearchTransportService searchTransportService,
                                   Function<String, DiscoveryNode> nodeIdToDiscoveryNode,
                                   Map<String, AliasFilter> aliasFilter,
                                   SearchPhaseController searchPhaseController, Executor executor,
                                   SearchRequest request, ActionListener<SearchResponse> listener,
                                   GroupShardsIterator shardsIts, long startTime, long clusterStateVersion,
                                   SearchTask task) {
        super(logger, searchTransportService, nodeIdToDiscoveryNode, aliasFilter, executor,
                request, listener, shardsIts, startTime, clusterStateVersion, task);
        this.searchPhaseController = searchPhaseController;

    }

    @Override
    protected String firstPhaseName() {
        return "query_fetch";
    }

    @Override
    protected void sendExecuteFirstPhase(DiscoveryNode node, ShardSearchTransportRequest request,
                                         ActionListener<QueryFetchSearchResult> listener) {
        searchTransportService.sendExecuteFetch(node, request, task, listener);
    }

    @Override
    protected void moveToSecondPhase() throws Exception {
        getExecutor().execute(new ActionRunnable<SearchResponse>(listener) {
            @Override
            public void doRun() throws IOException {
                final boolean isScrollRequest = request.scroll() != null;
                sortedShardDocs = searchPhaseController.sortDocs(isScrollRequest, firstResults);
                final InternalSearchResponse internalResponse = searchPhaseController.merge(isScrollRequest, sortedShardDocs, firstResults,
                    firstResults);
                String scrollId = isScrollRequest ? TransportSearchHelper.buildScrollId(request.searchType(), firstResults) : null;
                listener.onResponse(new SearchResponse(internalResponse, scrollId, expectedSuccessfulOps, successfulOps.get(),
                    buildTookInMillis(), buildShardFailures()));
            }

            @Override
            public void onFailure(Exception e) {
                ReduceSearchPhaseException failure = new ReduceSearchPhaseException("merge", "", e, buildShardFailures());
                if (logger.isDebugEnabled()) {
                    logger.debug("failed to reduce search", failure);
                }
                super.onFailure(failure);
            }
        });
    }
}
