/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.transport.Transport;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

final class SearchDfsQueryThenFetchAsyncAction extends AbstractSearchAsyncAction<DfsSearchResult> {

    private final QueryPhaseResultConsumer queryPhaseResultConsumer;

    SearchDfsQueryThenFetchAsyncAction(
        final Logger logger,
        final SearchTransportService searchTransportService,
        final BiFunction<String, String, Transport.Connection> nodeIdToConnection,
        final Map<String, AliasFilter> aliasFilter,
        final Map<String, Float> concreteIndexBoosts,
        final Executor executor,
        final QueryPhaseResultConsumer queryPhaseResultConsumer,
        final SearchRequest request,
        final ActionListener<SearchResponse> listener,
        final GroupShardsIterator<SearchShardIterator> shardsIts,
        final TransportSearchAction.SearchTimeProvider timeProvider,
        final ClusterState clusterState,
        final SearchTask task,
        SearchResponse.Clusters clusters
    ) {
        super(
            "dfs",
            logger,
            searchTransportService,
            nodeIdToConnection,
            aliasFilter,
            concreteIndexBoosts,
            executor,
            request,
            listener,
            shardsIts,
            timeProvider,
            clusterState,
            task,
            new ArraySearchPhaseResults<>(shardsIts.size()),
            request.getMaxConcurrentShardRequests(),
            clusters
        );
        this.queryPhaseResultConsumer = queryPhaseResultConsumer;
        SearchProgressListener progressListener = task.getProgressListener();
        if (progressListener != SearchProgressListener.NOOP) {
            notifyListShards(progressListener, clusters, request.source());
        }
    }

    @Override
    protected void executePhaseOnShard(
        final SearchShardIterator shardIt,
        final SearchShardTarget shard,
        final SearchActionListener<DfsSearchResult> listener
    ) {
        getSearchTransport().sendExecuteDfs(
            getConnection(shard.getClusterAlias(), shard.getNodeId()),
            buildShardSearchRequest(shardIt, listener.requestIndex),
            getTask(),
            listener
        );
    }

    @Override
    protected SearchPhase getNextPhase(final SearchPhaseResults<DfsSearchResult> results, SearchPhaseContext context) {
        final List<DfsSearchResult> dfsSearchResults = results.getAtomicArray().asList();
        final AggregatedDfs aggregatedDfs = SearchPhaseController.aggregateDfs(dfsSearchResults);

        return new DfsQueryPhase(
            dfsSearchResults,
            aggregatedDfs,
            queryPhaseResultConsumer,
            (queryResults) -> new FetchSearchPhase(queryResults, aggregatedDfs, context),
            context
        );
    }
}
