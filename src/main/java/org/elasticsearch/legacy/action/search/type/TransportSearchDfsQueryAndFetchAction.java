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

package org.elasticsearch.legacy.action.search.type;

import org.elasticsearch.legacy.action.ActionListener;
import org.elasticsearch.legacy.action.search.ReduceSearchPhaseException;
import org.elasticsearch.legacy.action.search.SearchRequest;
import org.elasticsearch.legacy.action.search.SearchResponse;
import org.elasticsearch.legacy.cluster.ClusterService;
import org.elasticsearch.legacy.cluster.node.DiscoveryNode;
import org.elasticsearch.legacy.common.inject.Inject;
import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.common.util.concurrent.AtomicArray;
import org.elasticsearch.legacy.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.legacy.search.action.SearchServiceListener;
import org.elasticsearch.legacy.search.action.SearchServiceTransportAction;
import org.elasticsearch.legacy.search.controller.SearchPhaseController;
import org.elasticsearch.legacy.search.dfs.AggregatedDfs;
import org.elasticsearch.legacy.search.dfs.DfsSearchResult;
import org.elasticsearch.legacy.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.legacy.search.internal.InternalSearchResponse;
import org.elasticsearch.legacy.search.internal.ShardSearchRequest;
import org.elasticsearch.legacy.search.query.QuerySearchRequest;
import org.elasticsearch.legacy.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class TransportSearchDfsQueryAndFetchAction extends TransportSearchTypeAction {

    @Inject
    public TransportSearchDfsQueryAndFetchAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                                 SearchServiceTransportAction searchService, SearchPhaseController searchPhaseController) {
        super(settings, threadPool, clusterService, searchService, searchPhaseController);
    }

    @Override
    protected void doExecute(SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        new AsyncAction(searchRequest, listener).start();
    }

    private class AsyncAction extends BaseAsyncAction<DfsSearchResult> {

        private final AtomicArray<QueryFetchSearchResult> queryFetchResults;

        private AsyncAction(SearchRequest request, ActionListener<SearchResponse> listener) {
            super(request, listener);
            queryFetchResults = new AtomicArray<>(firstResults.length());
        }

        @Override
        protected String firstPhaseName() {
            return "dfs";
        }

        @Override
        protected void sendExecuteFirstPhase(DiscoveryNode node, ShardSearchRequest request, SearchServiceListener<DfsSearchResult> listener) {
            searchService.sendExecuteDfs(node, request, listener);
        }

        @Override
        protected void moveToSecondPhase() {
            final AggregatedDfs dfs = searchPhaseController.aggregateDfs(firstResults);
            final AtomicInteger counter = new AtomicInteger(firstResults.asList().size());

            for (final AtomicArray.Entry<DfsSearchResult> entry : firstResults.asList()) {
                DfsSearchResult dfsResult = entry.value;
                DiscoveryNode node = nodes.get(dfsResult.shardTarget().nodeId());
                QuerySearchRequest querySearchRequest = new QuerySearchRequest(request, dfsResult.id(), dfs);
                executeSecondPhase(entry.index, dfsResult, counter, node, querySearchRequest);
            }
        }

        void executeSecondPhase(final int shardIndex, final DfsSearchResult dfsResult, final AtomicInteger counter, DiscoveryNode node, final QuerySearchRequest querySearchRequest) {
            searchService.sendExecuteFetch(node, querySearchRequest, new SearchServiceListener<QueryFetchSearchResult>() {
                @Override
                public void onResult(QueryFetchSearchResult result) {
                    result.shardTarget(dfsResult.shardTarget());
                    queryFetchResults.set(shardIndex, result);
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    onSecondPhaseFailure(t, querySearchRequest, shardIndex, dfsResult, counter);
                }
            });
        }

        void onSecondPhaseFailure(Throwable t, QuerySearchRequest querySearchRequest, int shardIndex, DfsSearchResult dfsResult, AtomicInteger counter) {
            if (logger.isDebugEnabled()) {
                logger.debug("[{}] Failed to execute query phase", t, querySearchRequest.id());
            }
            this.addShardFailure(shardIndex, dfsResult.shardTarget(), t);
            successfulOps.decrementAndGet();
            if (counter.decrementAndGet() == 0) {
                finishHim();
            }
        }

        private void finishHim() {
            try {
                threadPool.executor(ThreadPool.Names.SEARCH).execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            boolean useScroll = !useSlowScroll && request.scroll() != null;
                            sortedShardList = searchPhaseController.sortDocs(useScroll, queryFetchResults);
                            final InternalSearchResponse internalResponse = searchPhaseController.merge(sortedShardList, queryFetchResults, queryFetchResults);
                            String scrollId = null;
                            if (request.scroll() != null) {
                                scrollId = TransportSearchHelper.buildScrollId(request.searchType(), firstResults, null);
                            }
                            listener.onResponse(new SearchResponse(internalResponse, scrollId, expectedSuccessfulOps, successfulOps.get(), buildTookInMillis(), buildShardFailures()));
                        } catch (Throwable e) {
                            ReduceSearchPhaseException failure = new ReduceSearchPhaseException("query_fetch", "", e, buildShardFailures());
                            if (logger.isDebugEnabled()) {
                                logger.debug("failed to reduce search", failure);
                            }
                            listener.onFailure(failure);
                        }
                    }
                });
            } catch (EsRejectedExecutionException ex) {
                listener.onFailure(ex);
            }

        }
    }
}
