/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.search;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.transport.Transport;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

/**
 * This search phase fans out to every shards to execute a distributed search with a pre-collected distributed frequencies for all
 * search terms used in the actual search query. This phase is very similar to a the default query-then-fetch search phase but it doesn't
 * retry on another shard if any of the shards are failing. Failures are treated as shard failures and are counted as a non-successful
 * operation.
 * @see CountedCollector#onFailure(int, SearchShardTarget, Exception)
 */
final class DfsQueryPhase extends SearchPhase {
    private final QueryPhaseResultConsumer queryResult;
    private final List<DfsSearchResult> searchResults;
    private final AggregatedDfs dfs;
    private final Function<ArraySearchPhaseResults<SearchPhaseResult>, SearchPhase> nextPhaseFactory;
    private final SearchPhaseContext context;
    private final SearchTransportService searchTransportService;
    private final SearchProgressListener progressListener;

    DfsQueryPhase(
        List<DfsSearchResult> searchResults,
        AggregatedDfs dfs,
        QueryPhaseResultConsumer queryResult,
        Function<ArraySearchPhaseResults<SearchPhaseResult>, SearchPhase> nextPhaseFactory,
        SearchPhaseContext context
    ) {
        super("dfs_query");
        this.progressListener = context.getTask().getProgressListener();
        this.queryResult = queryResult;
        this.searchResults = searchResults;
        this.dfs = dfs;
        this.nextPhaseFactory = nextPhaseFactory;
        this.context = context;
        this.searchTransportService = context.getSearchTransport();

        // register the release of the query consumer to free up the circuit breaker memory
        // at the end of the search
        context.addReleasable(queryResult);
    }

    @Override
    public void run() throws IOException {
        // TODO we can potentially also consume the actual per shard results from the initial phase here in the aggregateDfs
        // to free up memory early
        final CountedCollector<SearchPhaseResult> counter = new CountedCollector<>(
            queryResult,
            searchResults.size(),
            () -> context.executeNextPhase(this, nextPhaseFactory.apply(queryResult)),
            context
        );
        for (final DfsSearchResult dfsResult : searchResults) {
            final SearchShardTarget shardTarget = dfsResult.getSearchShardTarget();
            Transport.Connection connection = context.getConnection(shardTarget.getClusterAlias(), shardTarget.getNodeId());
            QuerySearchRequest querySearchRequest = new QuerySearchRequest(
                context.getOriginalIndices(dfsResult.getShardIndex()),
                dfsResult.getContextId(),
                dfsResult.getShardSearchRequest(),
                dfs
            );
            final int shardIndex = dfsResult.getShardIndex();
            searchTransportService.sendExecuteQuery(
                connection,
                querySearchRequest,
                context.getTask(),
                new SearchActionListener<QuerySearchResult>(shardTarget, shardIndex) {

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
                            context.getLogger()
                                .debug(
                                    () -> new ParameterizedMessage("[{}] Failed to execute query phase", querySearchRequest.contextId()),
                                    exception
                                );
                            progressListener.notifyQueryFailure(shardIndex, shardTarget, exception);
                            counter.onFailure(shardIndex, shardTarget, exception);
                        } finally {
                            if (context.isPartOfPointInTime(querySearchRequest.contextId()) == false) {
                                // the query might not have been executed at all (for example because thread pool rejected
                                // execution) and the search context that was created in dfs phase might not be released.
                                // release it again to be in the safe side
                                context.sendReleaseSearchContext(
                                    querySearchRequest.contextId(),
                                    connection,
                                    context.getOriginalIndices(shardIndex)
                                );
                            }
                        }
                    }
                }
            );
        }
    }
}
