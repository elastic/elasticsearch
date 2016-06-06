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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.search.action.SearchTransportService;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;

class SearchQueryAndFetchAsyncAction extends AbstractSearchAsyncAction<QueryFetchSearchResult> {

    SearchQueryAndFetchAsyncAction(ESLogger logger, SearchTransportService searchTransportService,
                                           ClusterService clusterService, IndexNameExpressionResolver indexNameExpressionResolver,
                                           SearchPhaseController searchPhaseController, ThreadPool threadPool,
                                           SearchRequest request, ActionListener<SearchResponse> listener) {
        super(logger, searchTransportService, clusterService, indexNameExpressionResolver, searchPhaseController, threadPool,
                request, listener);
    }

    @Override
    protected String firstPhaseName() {
        return "query_fetch";
    }

    @Override
    protected void sendExecuteFirstPhase(DiscoveryNode node, ShardSearchTransportRequest request,
                                         ActionListener<QueryFetchSearchResult> listener) {
        searchTransportService.sendExecuteFetch(node, request, listener);
    }

    @Override
    protected void moveToSecondPhase() throws Exception {
        threadPool.executor(ThreadPool.Names.SEARCH).execute(new ActionRunnable<SearchResponse>(listener) {
            @Override
            public void doRun() throws IOException {
                boolean useScroll = request.scroll() != null;
                sortedShardList = searchPhaseController.sortDocs(useScroll, firstResults);
                final InternalSearchResponse internalResponse = searchPhaseController.merge(sortedShardList, firstResults,
                    firstResults);
                String scrollId = null;
                if (request.scroll() != null) {
                    scrollId = TransportSearchHelper.buildScrollId(request.searchType(), firstResults);
                }
                listener.onResponse(new SearchResponse(internalResponse, scrollId, expectedSuccessfulOps, successfulOps.get(),
                    buildTookInMillis(), buildShardFailures()));
            }

            @Override
            public void onFailure(Throwable t) {
                ReduceSearchPhaseException failure = new ReduceSearchPhaseException("merge", "", t, buildShardFailures());
                if (logger.isDebugEnabled()) {
                    logger.debug("failed to reduce search", failure);
                }
                super.onFailure(failure);
            }
        });
    }
}
