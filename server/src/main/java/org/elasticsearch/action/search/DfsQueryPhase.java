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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.transport.Transport;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * This search phase fans out to every shards to execute a distributed search with a pre-collected distributed frequencies for all
 * search terms used in the actual search query. This phase is very similar to a the default query-then-fetch search phase but it doesn't
 * retry on another shard if any of the shards are failing. Failures are treated as shard failures and are counted as a non-successful
 * operation.
 * @see CountedCollector#onFailure(int, SearchShardTarget, Exception)
 */
final class DfsQueryPhase extends SearchPhase {
    private final ArraySearchPhaseResults<SearchPhaseResult> queryResult;
    private final SearchPhaseController searchPhaseController;
    private final AtomicArray<DfsSearchResult> dfsSearchResults;
    private final Function<ArraySearchPhaseResults<SearchPhaseResult>, SearchPhase> nextPhaseFactory;
    private final SearchPhaseContext context;
    private final SearchTransportService searchTransportService;
    private final SearchProgressListener progressListener;

    DfsQueryPhase(AtomicArray<DfsSearchResult> dfsSearchResults,
                  SearchPhaseController searchPhaseController,
                  Function<ArraySearchPhaseResults<SearchPhaseResult>, SearchPhase> nextPhaseFactory,
                  SearchPhaseContext context, Consumer<Exception> onPartialMergeFailure) {
        super("dfs_query");
        this.progressListener = context.getTask().getProgressListener();
        this.queryResult = searchPhaseController.newSearchPhaseResults(context, progressListener,
            context.getRequest(), context.getNumShards(), onPartialMergeFailure);
        this.searchPhaseController = searchPhaseController;
        this.dfsSearchResults = dfsSearchResults;
        this.nextPhaseFactory = nextPhaseFactory;
        this.context = context;
        this.searchTransportService = context.getSearchTransport();
    }

    @Override
    public void run() throws IOException {
        // TODO we can potentially also consume the actual per shard results from the initial phase here in the aggregateDfs
        // to free up memory early
        final List<DfsSearchResult> resultList = dfsSearchResults.asList();
        final AggregatedDfs dfs = searchPhaseController.aggregateDfs(resultList);
        final CountedCollector<SearchPhaseResult> counter = new CountedCollector<>(queryResult,
            resultList.size(),
            () -> context.executeNextPhase(this, nextPhaseFactory.apply(queryResult)), context);
        for (final DfsSearchResult dfsResult : resultList) {
            final SearchShardTarget searchShardTarget = dfsResult.getSearchShardTarget();
            Transport.Connection connection = context.getConnection(searchShardTarget.getClusterAlias(), searchShardTarget.getNodeId());
            QuerySearchRequest querySearchRequest = new QuerySearchRequest(searchShardTarget.getOriginalIndices(),
                    dfsResult.getContextId(), dfs);
            final int shardIndex = dfsResult.getShardIndex();
            searchTransportService.sendExecuteQuery(connection, querySearchRequest, context.getTask(),
                new SearchActionListener<QuerySearchResult>(searchShardTarget, shardIndex) {

                    @Override
                    protected void innerOnResponse(QuerySearchResult response) {
                        try {
                            counter.onResult(response);
                        } catch (Exception e) {
                            context.onPhaseFailure(DfsQueryPhase.this, "", e);
                        }
                    }

                    @Override
                    public void onFailure(Exception exception) {
                        try {
                            context.getLogger().debug(() -> new ParameterizedMessage("[{}] Failed to execute query phase",
                                querySearchRequest.contextId()), exception);
                            progressListener.notifyQueryFailure(shardIndex, searchShardTarget, exception);
                            counter.onFailure(shardIndex, searchShardTarget, exception);
                        } finally {
                            // the query might not have been executed at all (for example because thread pool rejected
                            // execution) and the search context that was created in dfs phase might not be released.
                            // release it again to be in the safe side
                            context.sendReleaseSearchContext(
                                querySearchRequest.contextId(), connection, searchShardTarget.getOriginalIndices());
                        }
                    }
                });
        }
    }
}
