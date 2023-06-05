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
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * An internal search shards API performs the can_match phase and returns target shards of indices that might match a query.
 */
public class TransportSearchShardsAction extends HandledTransportAction<SearchShardsRequest, SearchShardsResponse> {
    private final TransportService transportService;
    private final TransportSearchAction transportSearchAction;
    private final SearchService searchService;
    private final RemoteClusterService remoteClusterService;
    private final ClusterService clusterService;
    private final SearchTransportService searchTransportService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final ThreadPool threadPool;

    @Inject
    public TransportSearchShardsAction(
        TransportService transportService,
        SearchService searchService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        TransportSearchAction transportSearchAction,
        SearchTransportService searchTransportService,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(SearchShardsAction.NAME, transportService, actionFilters, SearchShardsRequest::new);
        this.transportService = transportService;
        this.transportSearchAction = transportSearchAction;
        this.searchService = searchService;
        this.remoteClusterService = transportService.getRemoteClusterService();
        this.clusterService = clusterService;
        this.searchTransportService = searchTransportService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.threadPool = transportService.getThreadPool();
    }

    @Override
    protected void doExecute(Task task, SearchShardsRequest searchShardsRequest, ActionListener<SearchShardsResponse> listener) {
        final long relativeStartNanos = System.nanoTime();
        SearchRequest original = new SearchRequest(searchShardsRequest.indices()).indicesOptions(searchShardsRequest.indicesOptions())
            .routing(searchShardsRequest.routing())
            .preference(searchShardsRequest.preference())
            .allowPartialSearchResults(searchShardsRequest.allowPartialSearchResults());
        if (searchShardsRequest.query() != null) {
            original.source(new SearchSourceBuilder().query(searchShardsRequest.query()));
        }
        final TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(
            original.getOrCreateAbsoluteStartMillis(),
            relativeStartNanos,
            System::nanoTime
        );
        ClusterState clusterState = clusterService.state();
        Rewriteable.rewriteAndFetch(
            original,
            searchService.getRewriteContext(timeProvider::absoluteStartMillis),
            ActionListener.wrap(searchRequest -> {
                Map<String, OriginalIndices> groupedIndices = remoteClusterService.groupIndices(
                    searchRequest.indicesOptions(),
                    searchRequest.indices()
                );
                OriginalIndices originalIndices = groupedIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                if (groupedIndices.isEmpty() == false) {
                    throw new UnsupportedOperationException("search_shards API doesn't support remote indices " + searchRequest);
                }
                // TODO: Move a share stuff out of the TransportSearchAction.
                Index[] concreteIndices = transportSearchAction.resolveLocalIndices(originalIndices, clusterState, timeProvider);
                final Set<String> indicesAndAliases = indexNameExpressionResolver.resolveExpressions(clusterState, searchRequest.indices());
                final Map<String, AliasFilter> aliasFilters = transportSearchAction.buildIndexAliasFilters(
                    clusterState,
                    indicesAndAliases,
                    concreteIndices
                );
                String[] concreteIndexNames = Arrays.stream(concreteIndices).map(Index::getName).toArray(String[]::new);
                GroupShardsIterator<SearchShardIterator> shardIts = GroupShardsIterator.sortAndCreate(
                    transportSearchAction.getLocalShardsIterator(
                        clusterState,
                        searchRequest,
                        searchShardsRequest.clusterAlias(),
                        indicesAndAliases,
                        concreteIndexNames
                    )
                );
                if (SearchService.canRewriteToMatchNone(searchRequest.source()) == false) {
                    listener.onResponse(new SearchShardsResponse(toGroups(shardIts), clusterState.nodes().getAllNodes(), aliasFilters));
                } else {
                    var canMatchPhase = new CanMatchPreFilterSearchPhase(logger, searchTransportService, (clusterAlias, node) -> {
                        assert Objects.equals(clusterAlias, searchShardsRequest.clusterAlias());
                        return transportService.getConnection(clusterState.nodes().get(node));
                    },
                        aliasFilters,
                        Map.of(),
                        threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
                        searchRequest,
                        shardIts,
                        timeProvider,
                        (SearchTask) task,
                        false,
                        searchService.getCoordinatorRewriteContextProvider(timeProvider::absoluteStartMillis),
                        listener.map(its -> new SearchShardsResponse(toGroups(its), clusterState.nodes().getAllNodes(), aliasFilters))
                    );
                    canMatchPhase.start();
                }
            }, listener::onFailure)
        );
    }

    private static List<SearchShardsGroup> toGroups(GroupShardsIterator<SearchShardIterator> shardIts) {
        List<SearchShardsGroup> groups = new ArrayList<>(shardIts.size());
        for (SearchShardIterator shardIt : shardIts) {
            boolean skip = shardIt.skip();
            shardIt.reset();
            List<String> targetNodes = new ArrayList<>();
            SearchShardTarget target;
            while ((target = shardIt.nextOrNull()) != null) {
                targetNodes.add(target.getNodeId());
            }
            ShardId shardId = shardIt.shardId();
            groups.add(new SearchShardsGroup(shardId, targetNodes, skip));
        }
        return groups;
    }
}
