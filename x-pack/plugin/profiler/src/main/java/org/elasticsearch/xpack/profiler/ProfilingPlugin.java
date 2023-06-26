/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackSettings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class ProfilingPlugin extends Plugin implements ActionPlugin {
    private static final Logger logger = LogManager.getLogger(ProfilingPlugin.class);
    public static final Setting<Boolean> PROFILING_TEMPLATES_ENABLED = Setting.boolSetting(
        "xpack.profiling.templates.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final String PROFILING_THREAD_POOL_NAME = "profiling";
    private final Settings settings;
    private final boolean enabled;

    private final SetOnce<ProfilingIndexTemplateRegistry> registry = new SetOnce<>();

    private final SetOnce<ProfilingIndexManager> indexManager = new SetOnce<>();

    public ProfilingPlugin(Settings settings) {
        this.settings = settings;
        this.enabled = XPackSettings.PROFILING_ENABLED.get(settings);
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
        AllocationService allocationService
    ) {
        logger.info("Profiling is {}", enabled ? "enabled" : "disabled");
        registry.set(new ProfilingIndexTemplateRegistry(settings, clusterService, threadPool, client, xContentRegistry));
        indexManager.set(new ProfilingIndexManager(threadPool, client, clusterService));
        // set initial value
        updateTemplatesEnabled(PROFILING_TEMPLATES_ENABLED.get(settings));
        clusterService.getClusterSettings().addSettingsUpdateConsumer(PROFILING_TEMPLATES_ENABLED, this::updateTemplatesEnabled);
        if (enabled) {
            registry.get().initialize();
            indexManager.get().initialize();
            return List.of(registry.get(), indexManager.get());
        } else {
            return Collections.emptyList();
        }
    }

    public void updateTemplatesEnabled(boolean newValue) {
        if (newValue == false) {
            logger.info("profiling index templates will not be installed or reinstalled");
        }
        registry.get().setTemplatesEnabled(newValue);
        indexManager.get().setTemplatesEnabled(newValue);
    }

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
        List<RestHandler> handlers = new ArrayList<>();
        handlers.add(new RestGetStatusAction());
        if (enabled) {
            handlers.add(new RestGetProfilingAction());
        }
        return Collections.unmodifiableList(handlers);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            PROFILING_TEMPLATES_ENABLED,
            TransportGetProfilingAction.PROFILING_MAX_STACKTRACE_QUERY_SLICES,
            TransportGetProfilingAction.PROFILING_MAX_DETAIL_QUERY_SLICES,
            TransportGetProfilingAction.PROFILING_QUERY_REALTIME
        );
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return List.of(responseExecutorBuilder());
    }

    /**
     * @return <p>An <code>ExecutorBuilder</code> that creates an executor to offload internal query response processing from the
     * transport thread. The executor will occupy no thread by default to avoid using resources when the plugin is not needed but once used,
     * it will hold onto allocated pool threads for 30 minutes by default to keep response times low.</p>
     */
    public static ExecutorBuilder<?> responseExecutorBuilder() {
        return new ScalingExecutorBuilder(PROFILING_THREAD_POOL_NAME, 0, 1, TimeValue.timeValueMinutes(30L), false);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(GetProfilingAction.INSTANCE, TransportGetProfilingAction.class),
            new ActionHandler<>(GetStatusAction.INSTANCE, TransportGetStatusAction.class)
        );
    }

    @Override
    public void close() {
        registry.get().close();
        indexManager.get().close();
    }
}
