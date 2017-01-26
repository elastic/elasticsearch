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
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.QuerySearchResultProvider;
import org.elasticsearch.transport.Transport;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Function;

class SearchDfsQueryThenFetchAsyncAction extends AbstractSearchAsyncAction<DfsSearchResult> {

    protected final SearchPhaseController searchPhaseController;

    SearchDfsQueryThenFetchAsyncAction(Logger logger, SearchTransportService searchTransportService,
                                       Function<String, Transport.Connection> nodeIdToConnection,
                                       Map<String, AliasFilter> aliasFilter, Map<String, Float> concreteIndexBoosts,
                                       SearchPhaseController searchPhaseController, Executor executor, SearchRequest request,
                                       ActionListener<SearchResponse> listener, GroupShardsIterator shardsIts, long startTime,
                                       long clusterStateVersion, SearchTask task) {
        super(logger, searchTransportService, nodeIdToConnection, aliasFilter, concreteIndexBoosts, executor,
                request, listener, shardsIts, startTime, clusterStateVersion, task);
        this.searchPhaseController = searchPhaseController;
    }

    @Override
    protected String initialPhaseName() {
        return "dfs";
    }

    @Override
    protected void sendExecuteFirstPhase(Transport.Connection connection, ShardSearchTransportRequest request,
                                         ActionListener<DfsSearchResult> listener) {
        searchTransportService.sendExecuteDfs(connection, request, task, listener);
    }

    @Override
    protected void executeNextPhase(AtomicArray<DfsSearchResult> initialResults) {
        DfsQueryPhase queryPhase = new DfsQueryPhase(initialResults, searchPhaseController,
            (queryResults) -> new FetchPhase(queryResults, searchPhaseController));
        queryPhase.execute();
    }

    private class DfsQueryPhase {
        private final AtomicArray<QuerySearchResultProvider> queryResult;
        private final SearchPhaseController searchPhaseController;
        private final AtomicArray<DfsSearchResult> firstResults;
        private final Function<AtomicArray<QuerySearchResultProvider>, Runnable> nextPhaseFactory;

        public DfsQueryPhase(AtomicArray<DfsSearchResult> firstResults,
                             SearchPhaseController searchPhaseController,
                             Function<AtomicArray<QuerySearchResultProvider>, Runnable> nextPhaseFactory) {
            this.queryResult = new AtomicArray<>(firstResults.length());
            this.searchPhaseController = searchPhaseController;
            this.firstResults = firstResults;
            this.nextPhaseFactory = nextPhaseFactory;
        }

        public void execute() {
            final AggregatedDfs dfs = searchPhaseController.aggregateDfs(firstResults);
            final CountedCollector<QuerySearchResultProvider> counter = new CountedCollector<>(queryResult, firstResults.asList().size(),
                (successfulOps) -> {
                    if (successfulOps == 0) {
                        listener.onFailure(new SearchPhaseExecutionException("query", "all shards failed", buildShardFailures()));
                    } else {
                        Runnable nextPhase = this.nextPhaseFactory.apply(queryResult);
                        nextPhase.run();
                    }
                });
            for (final AtomicArray.Entry<DfsSearchResult> entry : firstResults.asList()) {
                DfsSearchResult dfsResult = entry.value;
                final int shardIndex = entry.index;
                Transport.Connection connection = nodeIdToConnection.apply(dfsResult.shardTarget().getNodeId());
                QuerySearchRequest querySearchRequest = new QuerySearchRequest(request, dfsResult.id(), dfs);
                searchTransportService.sendExecuteQuery(connection, querySearchRequest, task, new ActionListener<QuerySearchResult>() {
                    @Override
                    public void onResponse(QuerySearchResult result) {
                        counter.onResult(shardIndex, result, dfsResult.shardTarget());
                    }

                    @Override
                    public void onFailure(Exception e) {
                        try {
                            if (logger.isDebugEnabled()) {
                                logger.debug((Supplier<?>) () -> new ParameterizedMessage("[{}] Failed to execute query phase",
                                    querySearchRequest.id()), e);
                            }
                            counter.onFailure(shardIndex, dfsResult.shardTarget(), e);
                        } finally {
                            // the query might not have been executed at all (for example because thread pool rejected
                            // execution) and the search context that was created in dfs phase might not be released.
                            // release it again to be in the safe side
                            sendReleaseSearchContext(querySearchRequest.id(), connection);
                        }
                    }
                });
            }
        }
    }
}
