/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

public class TransportSearchShardsAction extends HandledTransportAction<SearchShardsRequest, SearchShardsResponse> {
    private final TransportSearchAction transportSearchAction;
    private final SearchService searchService;
    private final RemoteClusterService remoteClusterService;
    private final ClusterService clusterService;
    private final SearchTransportService searchTransportService;
    private final NamedWriteableRegistry namedWriteableRegistry;

    @Inject
    public TransportSearchShardsAction(
        TransportService transportService,
        SearchService searchService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        TransportSearchAction transportSearchAction,
        SearchTransportService searchTransportService,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        super(SearchShardsAction.NAME, transportService, actionFilters, SearchShardsRequest::new);
        this.transportSearchAction = transportSearchAction;
        this.searchService = searchService;
        this.remoteClusterService = transportService.getRemoteClusterService();
        this.clusterService = clusterService;
        this.searchTransportService = searchTransportService;
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    @Override
    protected void doExecute(Task task, SearchShardsRequest request, ActionListener<SearchShardsResponse> listener) {
        final long relativeStartNanos = System.nanoTime();
        SearchRequest original = request.getSearchRequest();
        final TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(
            original.getOrCreateAbsoluteStartMillis(),
            relativeStartNanos,
            System::nanoTime
        );
        ActionListener<SearchRequest> rewriteListener = ActionListener.wrap(rewritten -> {
            final SearchContextId searchContext;
            final Map<String, OriginalIndices> remoteClusterIndices;
            if (rewritten.pointInTimeBuilder() != null) {
                searchContext = rewritten.pointInTimeBuilder().getSearchContextId(namedWriteableRegistry);
                remoteClusterIndices = TransportSearchAction.getIndicesFromSearchContexts(searchContext, rewritten.indicesOptions());
            } else {
                searchContext = null;
                remoteClusterIndices = remoteClusterService.groupIndices(rewritten.indicesOptions(), rewritten.indices());
            }
            OriginalIndices localIndices = remoteClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            if (remoteClusterIndices.isEmpty() == false) {
                throw new IllegalArgumentException("search_shards doesn't support remote indices; request [" + original + "]");
            }
            transportSearchAction.executeLocalSearch(
                task,
                timeProvider,
                rewritten,
                localIndices,
                clusterService.state(),
                searchContext,
                new CanMatchPhaseProvider(listener)
            );
        }, listener::onFailure);
        Rewriteable.rewriteAndFetch(original, searchService.getRewriteContext(timeProvider::absoluteStartMillis), rewriteListener);
    }

    private class CanMatchPhaseProvider implements TransportSearchAction.SearchPhaseProvider {
        private final ActionListener<SearchShardsResponse> listener;

        CanMatchPhaseProvider(ActionListener<SearchShardsResponse> listener) {
            this.listener = listener;
        }

        @Override
        public SearchPhase newSearchPhase(
            SearchTask task,
            SearchRequest searchRequest,
            Executor executor,
            GroupShardsIterator<SearchShardIterator> shardIterators,
            TransportSearchAction.SearchTimeProvider timeProvider,
            BiFunction<String, String, Transport.Connection> connectionLookup,
            ClusterState clusterState,
            Map<String, AliasFilter> aliasFilter,
            Map<String, Float> concreteIndexBoosts,
            boolean preFilter,
            ThreadPool threadPool,
            SearchResponse.Clusters clusters
        ) {
            if (preFilter) {
                return new CanMatchPreFilterSearchPhase(
                    logger,
                    searchTransportService,
                    connectionLookup,
                    aliasFilter,
                    concreteIndexBoosts,
                    threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
                    searchRequest,
                    shardIterators,
                    timeProvider,
                    task,
                    searchService.getCoordinatorRewriteContextProvider(timeProvider::absoluteStartMillis),
                    listener.map(shardIts -> new SearchShardsResponse(toGroups(shardIts), clusterState.nodes(), aliasFilter))
                );
            } else {
                return new SearchPhase("search_shards") {
                    @Override
                    public void run() {
                        listener.onResponse(new SearchShardsResponse(toGroups(shardIterators), clusterState.nodes(), aliasFilter));
                    }
                };
            }
        }
    }

    private static List<SearchShardsGroup> toGroups(GroupShardsIterator<SearchShardIterator> shardIts) {
        List<SearchShardsGroup> groups = new ArrayList<>(shardIts.size());
        for (SearchShardIterator iter : shardIts) {
            boolean skip = iter.skip();
            iter.reset();
            List<String> targetNodes = new ArrayList<>();
            SearchShardTarget target;
            while ((target = iter.nextOrNull()) != null) {
                targetNodes.add(target.getNodeId());
            }
            ShardId shardId = iter.shardId();
            groups.add(new SearchShardsGroup(shardId, targetNodes, true, skip));
        }
        return groups;
    }
}
