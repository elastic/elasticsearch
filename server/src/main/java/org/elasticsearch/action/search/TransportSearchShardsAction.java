/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
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

    public static final String NAME = "indices:admin/search/search_shards";
    public static final ActionType<SearchShardsResponse> TYPE = new ActionType<>(NAME);
    public static final RemoteClusterActionType<SearchShardsResponse> REMOTE_TYPE = new RemoteClusterActionType<>(
        NAME,
        SearchShardsResponse::new
    );
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
        super(
            TYPE.name(),
            transportService,
            actionFilters,
            SearchShardsRequest::new,
            transportService.getThreadPool().executor(ThreadPool.Names.SEARCH_COORDINATION)
        );
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
        searchShards(task, searchShardsRequest, listener);
    }

    /**
     * Notes that this method does not perform authorization for the search shards action.
     * Callers must ensure that the request was properly authorized before calling this method.
     */
    public void searchShards(Task task, SearchShardsRequest searchShardsRequest, ActionListener<SearchShardsResponse> listener) {
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

        final ClusterState clusterState = clusterService.state();
        final ResolvedIndices resolvedIndices = ResolvedIndices.resolveWithIndicesRequest(
            searchShardsRequest,
            clusterState,
            indexNameExpressionResolver,
            remoteClusterService,
            timeProvider.absoluteStartMillis()
        );
        if (resolvedIndices.getRemoteClusterIndices().isEmpty() == false) {
            throw new UnsupportedOperationException("search_shards API doesn't support remote indices " + searchShardsRequest);
        }

        Rewriteable.rewriteAndFetch(
            original,
            searchService.getRewriteContext(timeProvider::absoluteStartMillis, resolvedIndices, null),
            listener.delegateFailureAndWrap((delegate, searchRequest) -> {
                Index[] concreteIndices = resolvedIndices.getConcreteLocalIndices();
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
                    delegate.onResponse(new SearchShardsResponse(toGroups(shardIts), clusterState.nodes().getAllNodes(), aliasFilters));
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
                        delegate.map(its -> new SearchShardsResponse(toGroups(its), clusterState.nodes().getAllNodes(), aliasFilters))
                    );
                    canMatchPhase.start();
                }
            })
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
