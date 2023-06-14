/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.DriverStatus;
import org.elasticsearch.compute.operator.MvExpandOperator;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkOperator;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceOperator;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.esql.EsqlInfoTransportAction;
import org.elasticsearch.xpack.esql.EsqlUsageTransportAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.RestEsqlQueryAction;
import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolver;
import org.elasticsearch.xpack.esql.execution.PlanExecutor;
import org.elasticsearch.xpack.esql.querydsl.query.SingleValueQuery;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeRegistry;
import org.elasticsearch.xpack.ql.index.IndexResolver;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class EsqlPlugin extends Plugin implements ActionPlugin {
    /**
     * The first version for ESQL. It's actual value is certainly wrong and will need to be
     * updated when we merge.
     */
    public static final TransportVersion TRANSPORT_MINIMUM_VERSION = TransportVersion.V_8_8_0;

    public static final String ESQL_THREAD_POOL_NAME = "esql";

    public static final Setting<Integer> QUERY_RESULT_TRUNCATION_MAX_SIZE = Setting.intSetting(
        "esql.query.result_truncation_max_size",
        10000,
        1,
        1000000,
        Setting.Property.NodeScope
    );

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
        IndexNameExpressionResolver expressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        Tracer tracer,
        AllocationService allocationService
    ) {
        IndexResolver indexResolver = new IndexResolver(
            client,
            clusterService.getClusterName().value(),
            EsqlDataTypeRegistry.INSTANCE,
            Set::of
        );
        return List.of(
            new PlanExecutor(indexResolver, new EnrichPolicyResolver(clusterService, indexResolver, threadPool)),
            new ExchangeService(clusterService.getSettings(), threadPool)
        );
    }

    /**
     * The settings defined by the ESQL plugin.
     *
     * @return the settings
     */
    @Override
    public List<Setting<?>> getSettings() {
        return List.of(QUERY_RESULT_TRUNCATION_MAX_SIZE, ExchangeService.INACTIVE_TIMEOUT_SETTING);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(EsqlQueryAction.INSTANCE, TransportEsqlQueryAction.class),
            new ActionHandler<>(EsqlStatsAction.INSTANCE, TransportEsqlStatsAction.class),
            new ActionHandler<>(XPackUsageFeatureAction.ESQL, EsqlUsageTransportAction.class),
            new ActionHandler<>(XPackInfoFeatureAction.ESQL, EsqlInfoTransportAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(new RestEsqlQueryAction());
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Stream.concat(
            List.of(
                DriverStatus.ENTRY,
                AbstractPageMappingOperator.Status.ENTRY,
                ExchangeSinkOperator.Status.ENTRY,
                ExchangeSourceOperator.Status.ENTRY,
                LuceneSourceOperator.Status.ENTRY,
                MvExpandOperator.Status.ENTRY,
                ValuesSourceReaderOperator.Status.ENTRY,
                SingleValueQuery.ENTRY
            ).stream(),
            Block.getNamedWriteables().stream()
        ).toList();
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        final int allocatedProcessors = EsExecutors.allocatedProcessors(settings);
        return List.of(
            new FixedExecutorBuilder(
                settings,
                ESQL_THREAD_POOL_NAME,
                ThreadPool.searchOrGetThreadPoolSize(allocatedProcessors),
                1000,
                "esql",
                true
            )
        );
    }
}
