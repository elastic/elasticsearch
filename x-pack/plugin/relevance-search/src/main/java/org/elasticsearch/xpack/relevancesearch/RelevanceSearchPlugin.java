/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.relevancesearch;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.relevancesearch.query.QueryFieldsResolver;
import org.elasticsearch.xpack.relevancesearch.query.RelevanceMatchQueryBuilder;
import org.elasticsearch.xpack.relevancesearch.query.RelevanceMatchQueryRewriter;
import org.elasticsearch.xpack.relevancesearch.settings.curations.CurationsService;
import org.elasticsearch.xpack.relevancesearch.settings.index.IndexCreationService;
import org.elasticsearch.xpack.relevancesearch.settings.relevance.RelevanceSettingsService;
import org.elasticsearch.xpack.relevancesearch.xsearch.XSearchAnalyticsService;
import org.elasticsearch.xpack.relevancesearch.xsearch.XSearchRequestValidationService;
import org.elasticsearch.xpack.relevancesearch.xsearch.action.XSearchAction;
import org.elasticsearch.xpack.relevancesearch.xsearch.action.XSearchTransportAction;
import org.elasticsearch.xpack.relevancesearch.xsearch.action.rest.RestXSearchAction;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class RelevanceSearchPlugin extends Plugin implements ActionPlugin, SearchPlugin {

    private static final Logger logger = LogManager.getLogger(RelevanceSearchPlugin.class);

    public RelevanceSearchPlugin() {
        logger.info("Relevance Search Plugin loaded");
    }

    private final SetOnce<RelevanceMatchQueryRewriter> relevanceMatchQueryRewriter = new SetOnce<>();
    private final SetOnce<IndexCreationService> indexCreationService = new SetOnce<>();

    private final SetOnce<XSearchRequestValidationService> xSearchRequestValidationService = new SetOnce<>();

    private final SetOnce<XSearchAnalyticsService> xSearchAnalyticsService = new SetOnce<>();

    @Override
    public List<RestHandler> getRestHandlers(
        final Settings settings,
        final RestController restController,
        final ClusterSettings clusterSettings,
        final IndexScopedSettings indexScopedSettings,
        final SettingsFilter settingsFilter,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(
            new RestXSearchAction(relevanceMatchQueryRewriter.get(), xSearchRequestValidationService.get(), xSearchAnalyticsService.get())
        );
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(new ActionPlugin.ActionHandler<>(XSearchAction.INSTANCE, XSearchTransportAction.class));
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        return Collections.singletonList(
            new QuerySpec<QueryBuilder>(
                RelevanceMatchQueryBuilder.NAME,
                (in) -> new RelevanceMatchQueryBuilder(relevanceMatchQueryRewriter.get(), in),
                (parser) -> RelevanceMatchQueryBuilder.fromXContent(parser, relevanceMatchQueryRewriter.get())
            )
        );
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        Tracer tracer,
        AllocationDeciders allocationDeciders
    ) {

        indexCreationService.set(new IndexCreationService(client, clusterService));
        xSearchRequestValidationService.set(new XSearchRequestValidationService(indexNameExpressionResolver, clusterService));
        xSearchAnalyticsService.set(new XSearchAnalyticsService(clusterService));
        RelevanceSettingsService relevanceSettingsService = new RelevanceSettingsService(client);
        CurationsService curationsService = new CurationsService(client);
        QueryFieldsResolver queryFieldsResolver = new QueryFieldsResolver();

        relevanceMatchQueryRewriter.set(
            new RelevanceMatchQueryRewriter(clusterService, relevanceSettingsService, curationsService, queryFieldsResolver)
        );

        return List.of(
            relevanceMatchQueryRewriter.get(),
            indexCreationService.get(),
            xSearchRequestValidationService.get(),
            xSearchAnalyticsService.get()
        );
    }
}
