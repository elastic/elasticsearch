/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.dfs.DfsSearchResult;

import java.util.List;

final class SearchDfsQueryThenFetchAsyncAction extends AbstractSearchAsyncAction<DfsSearchResult> {

    private final SearchPhaseController searchPhaseController;

    private final QueryPhaseResultConsumer queryPhaseResultConsumer;

    SearchDfsQueryThenFetchAsyncAction(final SearchPhaseContext context, final Logger logger,
                                       final SearchPhaseController searchPhaseController,
                                       final QueryPhaseResultConsumer queryPhaseResultConsumer,
                                       final SearchPhaseResults<DfsSearchResult> results,
                                       final GroupShardsIterator<SearchShardIterator> shardsIts) {
        super("dfs", context, logger, shardsIts, results, context.getRequest().getMaxConcurrentShardRequests());
        this.queryPhaseResultConsumer = queryPhaseResultConsumer;
        this.searchPhaseController = searchPhaseController;
        SearchProgressListener progressListener = context.getTask().getProgressListener();
        SearchSourceBuilder sourceBuilder = context.getRequest().source();
        progressListener.notifyListShards(SearchProgressListener.buildSearchShards(this.shardsIts),
            SearchProgressListener.buildSearchShards(toSkipShardsIts), context.getClusters(),
            sourceBuilder == null || sourceBuilder.size() != 0);
    }

    @Override
    protected void executePhaseOnShard(final SearchShardIterator shardIt, final SearchShardTarget shard,
                                       final SearchActionListener<DfsSearchResult> listener) {
        getSearchTransport().sendExecuteDfs(getConnection(shard.getClusterAlias(), shard.getNodeId()),
            buildShardSearchRequest(shardIt, listener.requestIndex) , getTask(), listener);
    }

    @Override
    protected SearchPhase getNextPhase(final SearchPhaseResults<DfsSearchResult> results, SearchPhaseContext context) {
        final List<DfsSearchResult> dfsSearchResults = results.getAtomicArray().asList();
        final AggregatedDfs aggregatedDfs = searchPhaseController.aggregateDfs(dfsSearchResults);

        return new DfsQueryPhase(dfsSearchResults, aggregatedDfs, queryPhaseResultConsumer,
            (queryResults) -> new FetchSearchPhase(queryResults, searchPhaseController, aggregatedDfs, context),
            context);
    }
}
