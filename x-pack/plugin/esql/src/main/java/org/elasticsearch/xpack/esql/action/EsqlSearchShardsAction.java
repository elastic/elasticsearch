/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.action.search.SearchShardsGroup;
import org.elasticsearch.action.search.SearchShardsRequest;
import org.elasticsearch.action.search.SearchShardsResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.ResolvedExpression;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.SearchShardRouting;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.CoordinatorRewriteContext;
import org.elasticsearch.index.query.CoordinatorRewriteContextProvider;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A fork of the search shards API for ES|QL. This fork allows us to gradually introduce features and optimizations to this
 * internal API without risking breaking the search-shards API, which is used by the search API. For now, this API delegates
 * to the search-shards API, but gradually, we will decouple this API completely from the search-shards API.
 */
public class EsqlSearchShardsAction extends HandledTransportAction<SearchShardsRequest, SearchShardsResponse> {
    public static final String NAME = "indices:data/read/esql/search_shards";
    public static final ActionType<SearchShardsResponse> TYPE = new ActionType<>(NAME);

    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final RemoteClusterService remoteClusterService;
    private final SearchService searchService;

    @Inject
    public EsqlSearchShardsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SearchService searchService
    ) {
        super(NAME, transportService, actionFilters, SearchShardsRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.remoteClusterService = transportService.getRemoteClusterService();
        this.searchService = searchService;
    }

    @Override
    protected void doExecute(Task task, SearchShardsRequest request, ActionListener<SearchShardsResponse> listener) {
        ActionListener.completeWith(listener, () -> searchShards(request));
    }

    SearchShardsResponse searchShards(SearchShardsRequest request) {
        final long nowInMillis = System.currentTimeMillis();
        final ProjectState project = projectResolver.getProjectState(clusterService.state());
        final ResolvedIndices resolvedIndices = ResolvedIndices.resolveWithIndicesRequest(
            request,
            project.metadata(),
            indexNameExpressionResolver,
            remoteClusterService,
            nowInMillis
        );
        if (resolvedIndices.getRemoteClusterIndices().isEmpty() == false) {
            throw new UnsupportedOperationException("search_shards API doesn't support remote indices " + request);
        }
        final Set<ResolvedExpression> indicesAndAliases = indexNameExpressionResolver.resolveExpressionsIgnoringRemotes(
            project.metadata(),
            request.indices()
        );
        final Index[] concreteIndices = resolvedIndices.getConcreteLocalIndices();
        final Map<String, AliasFilter> aliasFilters = Maps.newMapWithExpectedSize(concreteIndices.length);
        final List<String> searchableIndices = new ArrayList<>(concreteIndices.length);
        final boolean hasIndexBlocks = project.blocks().indices(project.projectId()).isEmpty() == false;
        for (Index index : concreteIndices) {
            project.blocks().indexBlockedRaiseException(project.projectId(), ClusterBlockLevel.READ, index.getName());
            aliasFilters.put(index.getUUID(), searchService.buildAliasFilter(project, index.getName(), indicesAndAliases));
            if (hasIndexBlocks == false
                || project.blocks().hasIndexBlock(project.projectId(), index.getName(), IndexMetadata.INDEX_REFRESH_BLOCK) == false) {
                searchableIndices.add(index.getName());
            }
        }
        final List<SearchShardRouting> shardRoutings = clusterService.operationRouting()
            .searchShards(
                project,
                searchableIndices.toArray(String[]::new),
                indexNameExpressionResolver.resolveSearchRouting(project.metadata(), request.routing(), request.indices()),
                request.preference()
            );
        final QueryBuilder query = request.query();
        final CoordinatorRewriteContextProvider rewriteContextProvider = query == null
            ? null
            : searchService.getCoordinatorRewriteContextProvider(() -> nowInMillis);
        final List<SearchShardsGroup> groups = new ArrayList<>(shardRoutings.size());
        for (SearchShardRouting shardRouting : shardRoutings) {
            final ShardId shardId = shardRouting.shardId();
            final AliasFilter aliasFilter = aliasFilters.get(shardId.getIndex().getUUID());
            assert aliasFilter != null : "no alias filter for " + shardId;
            final boolean skipped = rewriteContextProvider != null
                && canMatchOnCoordinator(rewriteContextProvider, request, shardRouting, aliasFilter, nowInMillis) == false;
            final List<String> allocatedNodes = new ArrayList<>(shardRouting.size());
            for (ShardRouting shard : shardRouting) {
                allocatedNodes.add(shard.currentNodeId());
            }
            groups.add(new SearchShardsGroup(shardId, allocatedNodes, skipped, shardRouting.splitShardCountSummary()));
        }
        return new SearchShardsResponse(
            groups,
            0,
            project.cluster().nodes().getAllNodes(),
            aliasFilters,
            request.getResolvedIndexExpressions()
        );
    }

    private static boolean canMatchOnCoordinator(
        CoordinatorRewriteContextProvider rewriteContextProvider,
        SearchShardsRequest request,
        SearchShardRouting shardRouting,
        AliasFilter aliasFilter,
        long nowInMillis
    ) {
        final ShardId shardId = shardRouting.shardId();
        final CoordinatorRewriteContext rewriteContext = rewriteContextProvider.getCoordinatorRewriteContext(shardId.getIndex());
        if (rewriteContext == null) {
            return true;
        }
        final ShardSearchRequest shardRequest = new ShardSearchRequest(
            shardId,
            nowInMillis,
            aliasFilter,
            request.clusterAlias(),
            shardRouting.splitShardCountSummary()
        );
        shardRequest.source(new SearchSourceBuilder().query(request.query()));
        try {
            return SearchService.queryStillMatchesAfterRewrite(shardRequest, rewriteContext);
        } catch (Exception e) {
            // treat as if shard is still a potential match
            return true;
        }
    }
}
