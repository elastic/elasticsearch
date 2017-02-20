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
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.search.query.QuerySearchResultProvider;
import org.elasticsearch.transport.Transport;

import java.io.IOException;
import java.util.function.Function;

/**
 * This search phase fans out to every shards to execute a distributed search with a pre-collected distributed frequencies for all
 * search terms used in the actual search query. This phase is very similar to a the default query-then-fetch search phase but it doesn't
 * retry on another shard if any of the shards are failing. Failures are treated as shard failures and are counted as a non-successful
 * operation.
 * @see CountedCollector#onFailure(int, SearchShardTarget, Exception)
 */
final class DfsQueryPhase extends SearchPhase {
    private final InitialSearchPhase.SearchPhaseResults<QuerySearchResultProvider> queryResult;
    private final SearchPhaseController searchPhaseController;
    private final AtomicArray<DfsSearchResult> dfsSearchResults;
    private final Function<InitialSearchPhase.SearchPhaseResults<QuerySearchResultProvider>, SearchPhase> nextPhaseFactory;
    private final SearchPhaseContext context;
    private final SearchTransportService searchTransportService;

    DfsQueryPhase(AtomicArray<DfsSearchResult> dfsSearchResults,
                  SearchPhaseController searchPhaseController,
                  Function<InitialSearchPhase.SearchPhaseResults<QuerySearchResultProvider>, SearchPhase> nextPhaseFactory,
                  SearchPhaseContext context) {
        super("dfs_query");
        this.queryResult = searchPhaseController.newSearchPhaseResults(context.getRequest(), context.getNumShards());
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
        final AggregatedDfs dfs = searchPhaseController.aggregateDfs(dfsSearchResults);
        final CountedCollector<QuerySearchResultProvider> counter = new CountedCollector<>(queryResult::consumeResult,
            dfsSearchResults.asList().size(),
            () -> {
                context.executeNextPhase(this, nextPhaseFactory.apply(queryResult));
            }, context);
        for (final AtomicArray.Entry<DfsSearchResult> entry : dfsSearchResults.asList()) {
            DfsSearchResult dfsResult = entry.value;
            final int shardIndex = entry.index;
            final SearchShardTarget searchShardTarget = dfsResult.shardTarget();
            Transport.Connection connection = context.getConnection(searchShardTarget.getNodeId());
            QuerySearchRequest querySearchRequest = new QuerySearchRequest(context.getRequest(), dfsResult.id(), dfs);
            searchTransportService.sendExecuteQuery(connection, querySearchRequest, context.getTask(),
                ActionListener.wrap(
                    result -> counter.onResult(shardIndex, result, searchShardTarget),
                    exception ->  {
                        try {
                            if (context.getLogger().isDebugEnabled()) {
                                context.getLogger().debug((Supplier<?>) () -> new ParameterizedMessage("[{}] Failed to execute query phase",
                                    querySearchRequest.id()), exception);
                            }
                            counter.onFailure(shardIndex, searchShardTarget, exception);
                        } finally {
                            // the query might not have been executed at all (for example because thread pool rejected
                            // execution) and the search context that was created in dfs phase might not be released.
                            // release it again to be in the safe side
                            context.sendReleaseSearchContext(querySearchRequest.id(), connection);
                        }
                    }));
        }
    }
}
