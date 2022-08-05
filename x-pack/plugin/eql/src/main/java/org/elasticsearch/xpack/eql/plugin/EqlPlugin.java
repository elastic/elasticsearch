/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.plugin;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.CircuitBreakerPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.eql.EqlInfoTransportAction;
import org.elasticsearch.xpack.eql.EqlUsageTransportAction;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.execution.PlanExecutor;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.index.RemoteClusterResolver;
import org.elasticsearch.xpack.ql.type.DefaultDataTypeRegistry;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class EqlPlugin extends Plugin implements ActionPlugin, CircuitBreakerPlugin {

    public static final String CIRCUIT_BREAKER_NAME = "eql_sequence";
    public static final long CIRCUIT_BREAKER_LIMIT = (long) ((0.50) * JvmInfo.jvmInfo().getMem().getHeapMax().getBytes());
    public static final double CIRCUIT_BREAKER_OVERHEAD = 1.0D;
    private final SetOnce<CircuitBreaker> circuitBreaker = new SetOnce<>();

    public static final Setting<Boolean> EQL_ENABLED_SETTING = Setting.boolSetting(
        "xpack.eql.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.DeprecatedWarning
    );

    public EqlPlugin() {}

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
        Tracer tracer
    ) {
        return createComponents(client, environment.settings(), clusterService);
    }

    private Collection<Object> createComponents(Client client, Settings settings, ClusterService clusterService) {
        RemoteClusterResolver remoteClusterResolver = new RemoteClusterResolver(settings, clusterService.getClusterSettings());
        IndexResolver indexResolver = new IndexResolver(
            client,
            clusterService.getClusterName().value(),
            DefaultDataTypeRegistry.INSTANCE,
            remoteClusterResolver::remoteClusters
        );
        PlanExecutor planExecutor = new PlanExecutor(client, indexResolver, circuitBreaker.get());
        return Collections.singletonList(planExecutor);
    }

    /**
     * The settings defined by EQL plugin.
     *
     * @return the settings
     */
    @Override
    public List<Setting<?>> getSettings() {
        return List.of(EQL_ENABLED_SETTING);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(EqlSearchAction.INSTANCE, TransportEqlSearchAction.class),
            new ActionHandler<>(EqlStatsAction.INSTANCE, TransportEqlStatsAction.class),
            new ActionHandler<>(EqlAsyncGetResultAction.INSTANCE, TransportEqlAsyncGetResultsAction.class),
            new ActionHandler<>(EqlAsyncGetStatusAction.INSTANCE, TransportEqlAsyncGetStatusAction.class),
            new ActionHandler<>(XPackUsageFeatureAction.EQL, EqlUsageTransportAction.class),
            new ActionHandler<>(XPackInfoFeatureAction.EQL, EqlInfoTransportAction.class)
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

        return List.of(
            new RestEqlSearchAction(),
            new RestEqlStatsAction(),
            new RestEqlGetAsyncResultAction(),
            new RestEqlGetAsyncStatusAction(),
            new RestEqlDeleteAsyncResultAction()
        );
    }

    // overridable by tests
    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }

    @Override
    public BreakerSettings getCircuitBreaker(Settings settings) {
        return BreakerSettings.updateFromSettings(
            new BreakerSettings(
                CIRCUIT_BREAKER_NAME,
                CIRCUIT_BREAKER_LIMIT,
                CIRCUIT_BREAKER_OVERHEAD,
                CircuitBreaker.Type.MEMORY,
                CircuitBreaker.Durability.TRANSIENT
            ),
            settings
        );
    }

    @Override
    public void setCircuitBreaker(CircuitBreaker circuitBreaker) {
        assert circuitBreaker.getName().equals(CIRCUIT_BREAKER_NAME);
        this.circuitBreaker.set(circuitBreaker);
    }
}
