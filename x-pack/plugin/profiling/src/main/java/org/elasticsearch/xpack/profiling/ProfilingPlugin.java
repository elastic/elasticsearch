/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.profiling.action.GetFlamegraphAction;
import org.elasticsearch.xpack.profiling.action.GetStackTracesAction;
import org.elasticsearch.xpack.profiling.action.GetStatusAction;
import org.elasticsearch.xpack.profiling.action.GetTopNFunctionsAction;
import org.elasticsearch.xpack.profiling.action.ProfilingInfoTransportAction;
import org.elasticsearch.xpack.profiling.action.ProfilingLicenseChecker;
import org.elasticsearch.xpack.profiling.action.ProfilingUsageTransportAction;
import org.elasticsearch.xpack.profiling.action.TransportGetFlamegraphAction;
import org.elasticsearch.xpack.profiling.action.TransportGetStackTracesAction;
import org.elasticsearch.xpack.profiling.action.TransportGetStatusAction;
import org.elasticsearch.xpack.profiling.action.TransportGetTopNFunctionsAction;
import org.elasticsearch.xpack.profiling.persistence.IndexStateResolver;
import org.elasticsearch.xpack.profiling.persistence.ProfilingDataStreamManager;
import org.elasticsearch.xpack.profiling.persistence.ProfilingIndexManager;
import org.elasticsearch.xpack.profiling.persistence.ProfilingIndexTemplateRegistry;
import org.elasticsearch.xpack.profiling.rest.RestGetFlamegraphAction;
import org.elasticsearch.xpack.profiling.rest.RestGetStackTracesAction;
import org.elasticsearch.xpack.profiling.rest.RestGetStatusAction;
import org.elasticsearch.xpack.profiling.rest.RestGetTopNFunctionsAction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class ProfilingPlugin extends Plugin implements ActionPlugin {
    private static final Logger logger = LogManager.getLogger(ProfilingPlugin.class);
    public static final Setting<Boolean> PROFILING_TEMPLATES_ENABLED = Setting.boolSetting(
        "xpack.profiling.templates.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // *Internal* setting meant as an escape hatch if we need to skip the check for outdated indices for some reason.
    public static final Setting<Boolean> PROFILING_CHECK_OUTDATED_INDICES = Setting.boolSetting(
        "xpack.profiling.check_outdated_indices",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final String PROFILING_THREAD_POOL_NAME = "profiling";
    private final Settings settings;
    private final boolean enabled;

    private final SetOnce<ProfilingIndexTemplateRegistry> registry = new SetOnce<>();
    private final SetOnce<ProfilingIndexManager> indexManager = new SetOnce<>();
    private final SetOnce<ProfilingDataStreamManager> dataStreamManager = new SetOnce<>();
    private final SetOnce<IndexStateResolver> indexStateResolver = new SetOnce<>();

    public ProfilingPlugin(Settings settings) {
        this.settings = settings;
        this.enabled = XPackSettings.PROFILING_ENABLED.get(settings);
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        Client client = services.client();
        ClusterService clusterService = services.clusterService();
        ThreadPool threadPool = services.threadPool();

        logger.info("Profiling is {}", enabled ? "enabled" : "disabled");
        registry.set(
            new ProfilingIndexTemplateRegistry(
                settings,
                clusterService,
                threadPool,
                client,
                services.xContentRegistry(),
                services.projectResolver()
            )
        );
        indexStateResolver.set(new IndexStateResolver(PROFILING_CHECK_OUTDATED_INDICES.get(settings)));
        clusterService.getClusterSettings().addSettingsUpdateConsumer(PROFILING_CHECK_OUTDATED_INDICES, this::updateCheckOutdatedIndices);

        indexManager.set(new ProfilingIndexManager(threadPool, client, clusterService, indexStateResolver.get()));
        dataStreamManager.set(new ProfilingDataStreamManager(threadPool, client, clusterService, indexStateResolver.get()));
        // set initial value
        updateTemplatesEnabled(PROFILING_TEMPLATES_ENABLED.get(settings));
        clusterService.getClusterSettings().addSettingsUpdateConsumer(PROFILING_TEMPLATES_ENABLED, this::updateTemplatesEnabled);
        if (enabled) {
            registry.get().initialize();
            indexManager.get().initialize();
            dataStreamManager.get().initialize();
        }
        return List.of(createLicenseChecker());
    }

    protected ProfilingLicenseChecker createLicenseChecker() {
        return new ProfilingLicenseChecker(XPackPlugin::getSharedLicenseState);
    }

    public void updateCheckOutdatedIndices(boolean newValue) {
        if (newValue == false) {
            logger.info("profiling will ignore outdated indices");
        }
        indexStateResolver.get().setCheckOutdatedIndices(newValue);
    }

    public void updateTemplatesEnabled(boolean newValue) {
        if (newValue == false) {
            logger.info("profiling index templates will not be installed or reinstalled");
        }
        registry.get().setTemplatesEnabled(newValue);
        indexManager.get().setTemplatesEnabled(newValue);
        dataStreamManager.get().setTemplatesEnabled(newValue);
    }

    @Override
    public List<RestHandler> getRestHandlers(
        final Settings settings,
        NamedWriteableRegistry namedWriteableRegistry,
        final RestController restController,
        final ClusterSettings clusterSettings,
        final IndexScopedSettings indexScopedSettings,
        final SettingsFilter settingsFilter,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        List<RestHandler> handlers = new ArrayList<>();
        handlers.add(new RestGetStatusAction());
        if (enabled) {
            handlers.add(new RestGetStackTracesAction());
            handlers.add(new RestGetFlamegraphAction());
            handlers.add(new RestGetTopNFunctionsAction());
        }
        return Collections.unmodifiableList(handlers);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            PROFILING_TEMPLATES_ENABLED,
            PROFILING_CHECK_OUTDATED_INDICES,
            TransportGetStackTracesAction.PROFILING_MAX_STACKTRACE_QUERY_SLICES,
            TransportGetStackTracesAction.PROFILING_MAX_DETAIL_QUERY_SLICES,
            TransportGetStackTracesAction.PROFILING_QUERY_REALTIME
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
    public List<ActionHandler> getActions() {
        return List.of(
            new ActionHandler(GetStackTracesAction.INSTANCE, TransportGetStackTracesAction.class),
            new ActionHandler(GetFlamegraphAction.INSTANCE, TransportGetFlamegraphAction.class),
            new ActionHandler(GetTopNFunctionsAction.INSTANCE, TransportGetTopNFunctionsAction.class),
            new ActionHandler(GetStatusAction.INSTANCE, TransportGetStatusAction.class),
            new ActionHandler(XPackUsageFeatureAction.UNIVERSAL_PROFILING, ProfilingUsageTransportAction.class),
            new ActionHandler(XPackInfoFeatureAction.UNIVERSAL_PROFILING, ProfilingInfoTransportAction.class)
        );
    }

    @Override
    public void close() {
        registry.get().close();
        indexManager.get().close();
        dataStreamManager.get().close();
    }
}
