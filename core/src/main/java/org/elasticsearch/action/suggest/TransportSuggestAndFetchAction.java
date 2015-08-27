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

package org.elasticsearch.action.suggest;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.search.ReduceSearchPhaseException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.type.TransportSearchTypeAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Map;

/**
 * Single phase transport action for suggest and fetch
 * Executes suggest and fetch using {@link SearchService#executeSuggestFetchPhase(org.elasticsearch.search.internal.ShardSearchRequest)}
 * on relevant shards
 */
public class TransportSuggestAndFetchAction extends TransportSearchTypeAction {

    @Inject
    public TransportSuggestAndFetchAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                          SearchServiceTransportAction searchService, SearchPhaseController searchPhaseController,
                                          ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, threadPool, clusterService, searchService, searchPhaseController, actionFilters, indexNameExpressionResolver);
    }

    @Override
    protected void doExecute(SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        new AsyncAction(searchRequest, listener).start();
    }

    private class AsyncAction extends BaseAsyncAction<QueryFetchSearchResult> {

        private AsyncAction(SearchRequest request, ActionListener<SearchResponse> listener) {
            super(request, listener);
        }

        @Override
        protected String firstPhaseName() {
            return "suggest_fetch";
        }

        @Override
        protected void sendExecuteFirstPhase(DiscoveryNode node, ShardSearchTransportRequest request, ActionListener<QueryFetchSearchResult> listener) {
            searchService.sendExecuteSuggestFetch(node, request, listener);
        }

        @Override
        protected void moveToSecondPhase() throws Exception {
            threadPool.executor(ThreadPool.Names.SEARCH).execute(new ActionRunnable<SearchResponse>(listener) {
                @Override
                public void doRun() throws IOException {
                    final Map<String, ScoreDoc[]> sortedShardList = searchPhaseController.sortSuggestDocs(firstResults);
                    final InternalSearchResponse internalResponse = searchPhaseController.mergeSuggestions(sortedShardList, firstResults, firstResults);
                    listener.onResponse(new SearchResponse(internalResponse, null, expectedSuccessfulOps, successfulOps.get(), buildTookInMillis(), buildShardFailures()));
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
}
