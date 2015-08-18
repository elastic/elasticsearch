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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.search.ReduceSearchPhaseException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class TransportSearchDfsQueryAndFetchAction extends TransportSearchTypeAction {

    @Inject
    public TransportSearchDfsQueryAndFetchAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                                 SearchServiceTransportAction searchService, SearchPhaseController searchPhaseController,
                                                 ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, threadPool, clusterService, searchService, searchPhaseController, actionFilters, indexNameExpressionResolver);
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
        protected void sendExecuteFirstPhase(DiscoveryNode node, ShardSearchTransportRequest request, ActionListener<DfsSearchResult> listener) {
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

        void executeSecondPhase(final int shardIndex, final DfsSearchResult dfsResult, final AtomicInteger counter, final DiscoveryNode node, final QuerySearchRequest querySearchRequest) {
            searchService.sendExecuteFetch(node, querySearchRequest, new ActionListener<QueryFetchSearchResult>() {
                @Override
                public void onResponse(QueryFetchSearchResult result) {
                    result.shardTarget(dfsResult.shardTarget());
                    queryFetchResults.set(shardIndex, result);
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    try {
                        onSecondPhaseFailure(t, querySearchRequest, shardIndex, dfsResult, counter);
                    } finally {
                        // the query might not have been executed at all (for example because thread pool rejected execution)
                        // and the search context that was created in dfs phase might not be released.
                        // release it again to be in the safe side
                        sendReleaseSearchContext(querySearchRequest.id(), node);
                    }
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
            threadPool.executor(ThreadPool.Names.SEARCH).execute(new ActionRunnable<SearchResponse>(listener) {
                @Override
                public void doRun() throws IOException {
                    sortedShardList = searchPhaseController.sortDocs(true, queryFetchResults);
                    final InternalSearchResponse internalResponse = searchPhaseController.merge(sortedShardList, queryFetchResults,
                            queryFetchResults, request);
                    String scrollId = null;
                    if (request.scroll() != null) {
                        scrollId = TransportSearchHelper.buildScrollId(request.searchType(), firstResults, null);
                    }
                    listener.onResponse(new SearchResponse(internalResponse, scrollId, expectedSuccessfulOps, successfulOps.get(), buildTookInMillis(), buildShardFailures()));
                }

                @Override
                public void onFailure(Throwable t) {
                    ReduceSearchPhaseException failure = new ReduceSearchPhaseException("query_fetch", "", t, buildShardFailures());
                    if (logger.isDebugEnabled()) {
                        logger.debug("failed to reduce search", failure);
                    }
                    super.onFailure(t);
                }
            });

        }
    }
}
