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
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.dfs.DfsKnnResults;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.transport.Transport;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

final class SearchDfsQueryThenFetchAsyncAction extends AbstractSearchAsyncAction<DfsSearchResult> {

    private final SearchPhaseResults<SearchPhaseResult> queryPhaseResultConsumer;
    private final SearchProgressListener progressListener;

    SearchDfsQueryThenFetchAsyncAction(
        Logger logger,
        NamedWriteableRegistry namedWriteableRegistry,
        SearchTransportService searchTransportService,
        BiFunction<String, String, Transport.Connection> nodeIdToConnection,
        Map<String, AliasFilter> aliasFilter,
        Map<String, Float> concreteIndexBoosts,
        Executor executor,
        SearchPhaseResults<SearchPhaseResult> queryPhaseResultConsumer,
        SearchRequest request,
        ActionListener<SearchResponse> listener,
        GroupShardsIterator<SearchShardIterator> shardsIts,
        TransportSearchAction.SearchTimeProvider timeProvider,
        ClusterState clusterState,
        SearchTask task,
        SearchResponse.Clusters clusters
    ) {
        super(
            "dfs",
            logger,
            namedWriteableRegistry,
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
        addReleasable(queryPhaseResultConsumer);
        this.progressListener = task.getProgressListener();
        // don't build the SearchShard list (can be expensive) if the SearchProgressListener won't use it
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
        final List<DfsKnnResults> mergedKnnResults = SearchPhaseController.mergeKnnResults(getRequest(), dfsSearchResults);
        return new DfsQueryPhase(
            dfsSearchResults,
            aggregatedDfs,
            mergedKnnResults,
            queryPhaseResultConsumer,
            (queryResults) -> new FetchSearchPhase(queryResults, aggregatedDfs, context),
            context
        );
    }

    @Override
    protected void onShardGroupFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
        progressListener.notifyQueryFailure(shardIndex, shardTarget, exc);
    }
}
