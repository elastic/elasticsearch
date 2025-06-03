/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.node;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.repositories.reservedstate.ReservedRepositoryAction;
import org.elasticsearch.action.admin.indices.template.reservedstate.ReservedComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.FailureStoreMetrics;
import org.elasticsearch.action.bulk.IncrementalBulkService;
import org.elasticsearch.action.datastreams.autosharding.DataStreamAutoShardingService;
import org.elasticsearch.action.ingest.ReservedPipelineAction;
import org.elasticsearch.action.search.OnlinePrewarmingService;
import org.elasticsearch.action.search.OnlinePrewarmingServiceProvider;
import org.elasticsearch.action.search.SearchExecutionStatsCollector;
import org.elasticsearch.action.search.SearchPhaseController;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.coordination.CoordinationDiagnosticsService;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.MasterHistoryService;
import org.elasticsearch.cluster.coordination.StableMasterHealthIndicatorService;
import org.elasticsearch.cluster.metadata.DataStreamFailureStoreSettings;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetentionSettings;
import org.elasticsearch.cluster.metadata.IndexMetadataVerifier;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataDataStreamsService;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.metadata.MetadataUpdateSettingsService;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.SystemIndexMetadataUpgradeService;
import org.elasticsearch.cluster.metadata.TemplateUpgradeService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.ProjectResolverFactory;
import org.elasticsearch.cluster.routing.BatchedRerouteService;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdMonitor;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.DynamicContextDataProvider;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.ConsistentSettingsService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.gateway.GatewayMetaState;
import org.elasticsearch.gateway.GatewayModule;
import org.elasticsearch.gateway.MetaStateService;
import org.elasticsearch.gateway.PersistedClusterStateService;
import org.elasticsearch.health.HealthPeriodicLogger;
import org.elasticsearch.health.HealthService;
import org.elasticsearch.health.metadata.HealthMetadataService;
import org.elasticsearch.health.node.DiskHealthIndicatorService;
import org.elasticsearch.health.node.HealthInfoCache;
import org.elasticsearch.health.node.LocalHealthMonitor;
import org.elasticsearch.health.node.ShardsCapacityHealthIndicatorService;
import org.elasticsearch.health.node.selection.HealthNodeTaskExecutor;
import org.elasticsearch.health.node.tracker.DiskHealthTracker;
import org.elasticsearch.health.node.tracker.HealthTracker;
import org.elasticsearch.health.node.tracker.RepositoriesHealthTracker;
import org.elasticsearch.health.stats.HealthApiStats;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettingProviders;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.SlowLogFieldProvider;
import org.elasticsearch.index.SlowLogFields;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.SourceFieldMetrics;
import org.elasticsearch.index.search.stats.ShardSearchPhaseAPMMetrics;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.indices.ExecutorSelector;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.IndicesServiceBuilder;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.indices.SystemIndexMappingUpdateService;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.recovery.PeerRecoverySourceService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.recovery.SnapshotFilesProvider;
import org.elasticsearch.indices.recovery.plan.PeerOnlyRecoveryPlannerService;
import org.elasticsearch.indices.recovery.plan.RecoveryPlannerService;
import org.elasticsearch.indices.recovery.plan.ShardSnapshotsService;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.injection.guice.Injector;
import org.elasticsearch.injection.guice.Key;
import org.elasticsearch.injection.guice.Module;
import org.elasticsearch.injection.guice.ModulesBuilder;
import org.elasticsearch.monitor.MonitorService;
import org.elasticsearch.monitor.fs.FsHealthService;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.metrics.IndicesMetrics;
import org.elasticsearch.monitor.metrics.NodeMetrics;
import org.elasticsearch.node.internal.TerminationHandler;
import org.elasticsearch.node.internal.TerminationHandlerProvider;
import org.elasticsearch.persistent.PersistentTasksClusterService;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.persistent.PersistentTasksExecutorRegistry;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.CircuitBreakerPlugin;
import org.elasticsearch.plugins.ClusterCoordinationPlugin;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.HealthPlugin;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.MetadataUpgrader;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsLoader;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.RecoveryPlannerPlugin;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.plugins.ShutdownAwarePlugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.plugins.TelemetryPlugin;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.plugins.internal.DocumentParsingProviderPlugin;
import org.elasticsearch.plugins.internal.LoggingDataProvider;
import org.elasticsearch.plugins.internal.ReloadAwarePlugin;
import org.elasticsearch.plugins.internal.RestExtension;
import org.elasticsearch.plugins.internal.SettingsExtension;
import org.elasticsearch.readiness.ReadinessService;
import org.elasticsearch.repositories.RepositoriesModule;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.ReservedClusterStateHandlerProvider;
import org.elasticsearch.reservedstate.action.ReservedClusterSettingsAction;
import org.elasticsearch.reservedstate.service.FileSettingsService;
import org.elasticsearch.reservedstate.service.FileSettingsService.FileSettingsHealthIndicatorService;
import org.elasticsearch.reservedstate.service.FileSettingsService.FileSettingsHealthTracker;
import org.elasticsearch.reservedstate.service.FileSettingsServiceProvider;
import org.elasticsearch.rest.action.search.SearchResponseMetrics;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchUtils;
import org.elasticsearch.search.aggregations.support.AggregationUsageService;
import org.elasticsearch.shutdown.PluginShutdownService;
import org.elasticsearch.snapshots.InternalSnapshotsInfoService;
import org.elasticsearch.snapshots.RepositoryIntegrityHealthIndicatorService;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.SnapshotShardsService;
import org.elasticsearch.snapshots.SnapshotsInfoService;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.internal.BuiltInExecutorBuilders;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Collections.newSetFromMap;
import static java.util.function.Predicate.not;
import static org.elasticsearch.core.Types.forciblyCast;

/**
 * Class uses to perform all the operations needed to construct a {@link Node} instance.
 * <p>
 * Constructing a {@link Node} is a complex operation, involving many interdependent services.
 * Separating out this logic into a dedicated class is a lot clearer and more flexible than
 * doing all this logic inside a constructor in {@link Node}.
 */
class NodeConstruction {

    /**
     * Prepare everything needed to create a {@link Node} instance.
     *
     * @param initialEnvironment         the initial environment for this node, which will be added to by plugins
     * @param serviceProvider            provides various service implementations that could be mocked
     * @param forbidPrivateIndexSettings whether or not private index settings are forbidden when creating an index; this is used in the
     *                                   test framework for tests that rely on being able to set private settings
     */
    static NodeConstruction prepareConstruction(
        Environment initialEnvironment,
        PluginsLoader pluginsLoader,
        NodeServiceProvider serviceProvider,
        boolean forbidPrivateIndexSettings
    ) {
        List<Closeable> closeables = new ArrayList<>();
        try {
            NodeConstruction constructor = new NodeConstruction(closeables);

            Settings settings = constructor.createEnvironment(initialEnvironment, serviceProvider, pluginsLoader);
            constructor.loadLoggingDataProviders();
            TelemetryProvider telemetryProvider = constructor.createTelemetryProvider(settings);
            ThreadPool threadPool = constructor.createThreadPool(settings, telemetryProvider.getMeterRegistry());

            final SettingsModule settingsModule;
            try (var ignored = threadPool.getThreadContext().newStoredContext()) {
                // If any deprecated settings are in use then we add warnings to the thread context response headers, but we're not
                // computing a response here so these headers aren't relevant and eventually just get dropped after possibly leaking into
                // places they shouldn't. Best to explicitly drop them now to protect against such leakage.
                settingsModule = constructor.validateSettings(initialEnvironment.settings(), settings, threadPool);
            }

            SearchModule searchModule = constructor.createSearchModule(settingsModule.getSettings(), threadPool, telemetryProvider);
            constructor.createClientAndRegistries(settingsModule.getSettings(), threadPool, searchModule);
            DocumentParsingProvider documentParsingProvider = constructor.getDocumentParsingProvider();

            ScriptService scriptService = constructor.createScriptService(settingsModule, threadPool, serviceProvider);

            constructor.createUpdateHelper(scriptService);

            constructor.construct(
                threadPool,
                settingsModule,
                searchModule,
                scriptService,
                constructor.createAnalysisRegistry(),
                serviceProvider,
                forbidPrivateIndexSettings,
                telemetryProvider,
                documentParsingProvider
            );

            return constructor;
        } catch (IOException e) {
            IOUtils.closeWhileHandlingException(closeables);
            throw new ElasticsearchException("Failed to bind service", e);
        } catch (Throwable t) {
            IOUtils.closeWhileHandlingException(closeables);
            throw t;
        }
    }

    /**
     * See comments on Node#logger for why this is not static
     */
    private final Logger logger = LogManager.getLogger(Node.class);
    private final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(Node.class);

    private final List<Closeable> resourcesToClose;
    private final ModulesBuilder modules = new ModulesBuilder();
    /*
     * References for storing in a Node
     */
    private Injector injector;
    private Environment environment;
    private NodeEnvironment nodeEnvironment;
    private PluginsService pluginsService;
    private NodeClient client;
    private Collection<LifecycleComponent> pluginLifecycleComponents;
    private Node.LocalNodeFactory localNodeFactory;
    private NodeService nodeService;
    private TerminationHandler terminationHandler;
    private NamedWriteableRegistry namedWriteableRegistry;
    private NamedXContentRegistry xContentRegistry;
    private ClusterInfoService clusterInfoService;

    private NodeConstruction(List<Closeable> resourcesToClose) {
        this.resourcesToClose = resourcesToClose;
    }

    Injector injector() {
        return injector;
    }

    Environment environment() {
        return environment;
    }

    NodeEnvironment nodeEnvironment() {
        return nodeEnvironment;
    }

    PluginsService pluginsService() {
        return pluginsService;
    }

    NodeClient client() {
        return client;
    }

    Collection<LifecycleComponent> pluginLifecycleComponents() {
        return pluginLifecycleComponents;
    }

    Node.LocalNodeFactory localNodeFactory() {
        return localNodeFactory;
    }

    NodeService nodeService() {
        return nodeService;
    }

    TerminationHandler terminationHandler() {
        return terminationHandler;
    }

    NamedWriteableRegistry namedWriteableRegistry() {
        return namedWriteableRegistry;
    }

    NamedXContentRegistry namedXContentRegistry() {
        return xContentRegistry;
    }

    private <T> Optional<T> getSinglePlugin(Class<T> pluginClass) {
        return getSinglePlugin(pluginsService.filterPlugins(pluginClass), pluginClass);
    }

    private static <T> Optional<T> getSinglePlugin(Stream<T> plugins, Class<T> pluginClass) {
        var it = plugins.iterator();
        if (it.hasNext() == false) {
            return Optional.empty();
        }
        T plugin = it.next();
        if (it.hasNext()) {
            List<T> allPlugins = new ArrayList<>();
            allPlugins.add(plugin);
            it.forEachRemaining(allPlugins::add);
            throw new IllegalStateException("A single " + pluginClass.getName() + " was expected but got " + allPlugins);
        }
        return Optional.of(plugin);
    }

    private Settings createEnvironment(Environment initialEnvironment, NodeServiceProvider serviceProvider, PluginsLoader pluginsLoader) {
        // Pass the node settings to the DeprecationLogger class so that it can have the deprecation.skip_deprecated_settings setting:
        Settings envSettings = initialEnvironment.settings();
        DeprecationLogger.initialize(envSettings);

        JvmInfo jvmInfo = JvmInfo.jvmInfo();
        if (Environment.PATH_SHARED_DATA_SETTING.exists(envSettings)) {
            // NOTE: this must be done with an explicit check here because the deprecation property on a path setting will
            // cause ES to fail to start since logging is not yet initialized on first read of the setting
            deprecationLogger.warn(
                DeprecationCategory.SETTINGS,
                "shared-data-path",
                "setting [path.shared_data] is deprecated and will be removed in a future release"
            );
        }

        if (initialEnvironment.dataDirs().length > 1) {
            // NOTE: we use initialEnvironment here, but assertEquivalent below ensures the data paths do not change
            deprecationLogger.warn(
                DeprecationCategory.SETTINGS,
                "multiple-data-paths",
                "Configuring multiple [path.data] paths is deprecated. Use RAID or other system level features for utilizing "
                    + "multiple disks. This feature will be removed in a future release."
            );
        }
        if (Environment.dataPathUsesList(envSettings)) {
            // already checked for multiple values above, so if this is a list it is a single valued list
            deprecationLogger.warn(
                DeprecationCategory.SETTINGS,
                "multiple-data-paths-list",
                "Configuring [path.data] with a list is deprecated. Instead specify as a string value."
            );
        }

        if (logger.isDebugEnabled()) {
            logger.debug(
                "using config [{}], data [{}], logs [{}], plugins [{}]",
                initialEnvironment.configDir(),
                Arrays.toString(initialEnvironment.dataDirs()),
                initialEnvironment.logsDir(),
                initialEnvironment.pluginsDir()
            );
        }

        Node.deleteTemporaryApmConfig(
            jvmInfo,
            (e, apmConfig) -> logger.error("failed to delete temporary APM config file [{}], reason: [{}]", apmConfig, e.getMessage())
        );

        pluginsService = serviceProvider.newPluginService(initialEnvironment, pluginsLoader);
        modules.bindToInstance(PluginsService.class, pluginsService);
        Settings settings = Node.mergePluginSettings(pluginsService.pluginMap(), envSettings);

        /*
         * Create the environment based on the finalized view of the settings. This is to ensure that components get the same setting
         * values, no matter they ask for them from.
         */
        environment = new Environment(settings, initialEnvironment.configDir());
        Environment.assertEquivalent(initialEnvironment, environment);
        modules.bindToInstance(Environment.class, environment);

        return settings;
    }

    private void loadLoggingDataProviders() {
        DynamicContextDataProvider.setDataProviders(pluginsService.loadServiceProviders(LoggingDataProvider.class));
    }

    private TelemetryProvider createTelemetryProvider(Settings settings) {
        return getSinglePlugin(TelemetryPlugin.class).map(p -> p.getTelemetryProvider(settings)).orElse(TelemetryProvider.NOOP);
    }

    private ThreadPool createThreadPool(Settings settings, MeterRegistry meterRegistry) throws IOException {
        ThreadPool threadPool = new ThreadPool(
            settings,
            meterRegistry,
            pluginsService.loadSingletonServiceProvider(BuiltInExecutorBuilders.class, DefaultBuiltInExecutorBuilders::new),
            pluginsService.flatMap(p -> p.getExecutorBuilders(settings)).toArray(ExecutorBuilder<?>[]::new)
        );
        resourcesToClose.add(() -> ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));
        modules.bindToInstance(ThreadPool.class, threadPool);

        // adds the context to the DeprecationLogger so that it does not need to be injected everywhere
        HeaderWarning.setThreadContext(threadPool.getThreadContext());
        resourcesToClose.add(() -> HeaderWarning.removeThreadContext(threadPool.getThreadContext()));

        return threadPool;
    }

    private SettingsModule validateSettings(Settings envSettings, Settings settings, ThreadPool threadPool) throws IOException {
        // register the node.data, node.ingest, node.master, node.remote_cluster_client settings here so we can mark them private
        List<Setting<?>> additionalSettings = new ArrayList<>(pluginsService.flatMap(Plugin::getSettings).toList());
        for (final ExecutorBuilder<?> builder : threadPool.builders()) {
            additionalSettings.addAll(builder.getRegisteredSettings());
        }
        addBwcSearchWorkerSettings(additionalSettings);
        SettingsExtension.load().forEach(e -> additionalSettings.addAll(e.getSettings()));

        // this is as early as we can validate settings at this point. we already pass them to ThreadPool
        // so we might be late here already
        SettingsModule settingsModule = new SettingsModule(
            settings,
            additionalSettings,
            pluginsService.flatMap(Plugin::getSettingsFilter).toList()
        );
        modules.add(settingsModule);

        // creating `NodeEnvironment` breaks the ability to rollback to 7.x on an 8.0 upgrade (`upgradeLegacyNodeFolders`) so do this
        // after settings validation.
        nodeEnvironment = new NodeEnvironment(envSettings, environment);
        logger.info(
            "node name [{}], node ID [{}], cluster name [{}], roles {}",
            Node.NODE_NAME_SETTING.get(envSettings),
            nodeEnvironment.nodeId(),
            ClusterName.CLUSTER_NAME_SETTING.get(envSettings).value(),
            DiscoveryNode.getRolesFromSettings(settings)
                .stream()
                .map(DiscoveryNodeRole::roleName)
                .collect(Collectors.toCollection(LinkedHashSet::new))
        );
        resourcesToClose.add(nodeEnvironment);
        modules.bindToInstance(NodeEnvironment.class, nodeEnvironment);

        return settingsModule;
    }

    @UpdateForV10(owner = UpdateForV10.Owner.SEARCH_FOUNDATIONS)
    private static void addBwcSearchWorkerSettings(List<Setting<?>> additionalSettings) {
        // Search workers thread pool has been removed in Elasticsearch 8.16.0. These settings are deprecated and take no effect.
        // They are here only to enable BwC for deployments that still use them
        additionalSettings.add(
            Setting.intSetting("thread_pool.search_worker.queue_size", 0, Setting.Property.NodeScope, Setting.Property.DeprecatedWarning)
        );
        additionalSettings.add(
            Setting.intSetting("thread_pool.search_worker.size", 0, Setting.Property.NodeScope, Setting.Property.DeprecatedWarning)
        );
        // Search throttled thread pool has been removed in Elasticsearch 9.0.0. These settings are deprecated and take no effect.
        // They are here only to enable BwC for deployments that still use them
        additionalSettings.add(
            Setting.intSetting("thread_pool.search_throttled.queue_size", 0, Setting.Property.NodeScope, Setting.Property.DeprecatedWarning)
        );
        additionalSettings.add(
            Setting.intSetting("thread_pool.search_throttled.size", 0, Setting.Property.NodeScope, Setting.Property.DeprecatedWarning)
        );
    }

    private SearchModule createSearchModule(Settings settings, ThreadPool threadPool, TelemetryProvider telemetryProvider) {
        IndexSearcher.setMaxClauseCount(SearchUtils.calculateMaxClauseValue(threadPool));
        return new SearchModule(settings, pluginsService.filterPlugins(SearchPlugin.class).toList(), telemetryProvider);
    }

    /**
     * Create various objects that are stored as member variables. This is so they are accessible as soon as possible.
     */
    private void createClientAndRegistries(Settings settings, ThreadPool threadPool, SearchModule searchModule) {
        client = new NodeClient(settings, threadPool);
        modules.add(b -> {
            b.bind(Client.class).toInstance(client);
            b.bind(NodeClient.class).toInstance(client);
        });

        localNodeFactory = new Node.LocalNodeFactory(settings, nodeEnvironment.nodeId());

        namedWriteableRegistry = new NamedWriteableRegistry(
            Stream.of(
                NetworkModule.getNamedWriteables().stream(),
                IndicesModule.getNamedWriteables().stream(),
                searchModule.getNamedWriteables().stream(),
                pluginsService.flatMap(Plugin::getNamedWriteables),
                ClusterModule.getNamedWriteables().stream()
            ).flatMap(Function.identity()).toList()
        );
        xContentRegistry = new NamedXContentRegistry(
            Stream.of(
                NetworkModule.getNamedXContents().stream(),
                IndicesModule.getNamedXContents().stream(),
                searchModule.getNamedXContents().stream(),
                pluginsService.flatMap(Plugin::getNamedXContent),
                ClusterModule.getNamedXWriteables().stream(),
                HealthNodeTaskExecutor.getNamedXContentParsers().stream()
            ).flatMap(Function.identity()).toList()
        );
        modules.add(b -> {
            b.bind(NamedWriteableRegistry.class).toInstance(namedWriteableRegistry);
            b.bind(NamedXContentRegistry.class).toInstance(xContentRegistry);
        });
    }

    private ScriptService createScriptService(SettingsModule settingsModule, ThreadPool threadPool, NodeServiceProvider serviceProvider) {
        Settings settings = settingsModule.getSettings();
        ScriptModule scriptModule = new ScriptModule(settings, pluginsService.filterPlugins(ScriptPlugin.class).toList());

        ScriptService scriptService = serviceProvider.newScriptService(
            pluginsService,
            settings,
            scriptModule.engines,
            scriptModule.contexts,
            threadPool::absoluteTimeInMillis
        );
        ScriptModule.registerClusterSettingsListeners(scriptService, settingsModule.getClusterSettings());
        modules.add(b -> { b.bind(ScriptService.class).toInstance(scriptService); });

        return scriptService;
    }

    private DataStreamGlobalRetentionSettings createDataStreamServicesAndGlobalRetentionResolver(
        ThreadPool threadPool,
        ClusterService clusterService,
        IndicesService indicesService,
        MetadataCreateIndexService metadataCreateIndexService
    ) {
        DataStreamGlobalRetentionSettings dataStreamGlobalRetentionSettings = DataStreamGlobalRetentionSettings.create(
            clusterService.getClusterSettings()
        );
        modules.bindToInstance(DataStreamGlobalRetentionSettings.class, dataStreamGlobalRetentionSettings);
        modules.bindToInstance(
            DataStreamFailureStoreSettings.class,
            DataStreamFailureStoreSettings.create(clusterService.getClusterSettings())
        );
        modules.bindToInstance(
            MetadataCreateDataStreamService.class,
            new MetadataCreateDataStreamService(threadPool, clusterService, metadataCreateIndexService)
        );
        modules.bindToInstance(
            MetadataDataStreamsService.class,
            new MetadataDataStreamsService(clusterService, indicesService, dataStreamGlobalRetentionSettings)
        );
        return dataStreamGlobalRetentionSettings;
    }

    private UpdateHelper createUpdateHelper(ScriptService scriptService) {
        UpdateHelper updateHelper = new UpdateHelper(scriptService);

        modules.add(b -> b.bind(UpdateHelper.class).toInstance(updateHelper));
        return updateHelper;
    }

    private AnalysisRegistry createAnalysisRegistry() throws IOException {
        AnalysisRegistry registry = new AnalysisModule(
            environment,
            pluginsService.filterPlugins(AnalysisPlugin.class).toList(),
            pluginsService.getStablePluginRegistry()
        ).getAnalysisRegistry();
        modules.bindToInstance(AnalysisRegistry.class, registry);
        return registry;
    }

    private void construct(
        ThreadPool threadPool,
        SettingsModule settingsModule,
        SearchModule searchModule,
        ScriptService scriptService,
        AnalysisRegistry analysisRegistry,
        NodeServiceProvider serviceProvider,
        boolean forbidPrivateIndexSettings,
        TelemetryProvider telemetryProvider,
        DocumentParsingProvider documentParsingProvider
    ) throws IOException {

        Settings settings = settingsModule.getSettings();

        modules.bindToInstance(Tracer.class, telemetryProvider.getTracer());

        TaskManager taskManager = new TaskManager(
            settings,
            threadPool,
            Stream.concat(
                pluginsService.filterPlugins(ActionPlugin.class).flatMap(p -> p.getTaskHeaders().stream()),
                Task.HEADERS_TO_COPY.stream()
            ).collect(Collectors.toSet()),
            telemetryProvider.getTracer()
        );

        // serverless deployments plug-in the multi-project resolver factory
        ProjectResolver projectResolver = pluginsService.loadSingletonServiceProvider(
            ProjectResolverFactory.class,
            () -> ProjectResolverFactory.DEFAULT
        ).create();
        modules.bindToInstance(ProjectResolver.class, projectResolver);
        ClusterService clusterService = createClusterService(settingsModule, threadPool, taskManager);
        clusterService.addStateApplier(scriptService);

        modules.bindToInstance(DocumentParsingProvider.class, documentParsingProvider);

        FeatureService featureService = new FeatureService(pluginsService.loadServiceProviders(FeatureSpecification.class));

        FailureStoreMetrics failureStoreMetrics = new FailureStoreMetrics(telemetryProvider.getMeterRegistry());
        final IngestService ingestService = new IngestService(
            clusterService,
            threadPool,
            environment,
            scriptService,
            analysisRegistry,
            pluginsService.filterPlugins(IngestPlugin.class).toList(),
            client,
            IngestService.createGrokThreadWatchdog(environment, threadPool),
            failureStoreMetrics,
            projectResolver,
            featureService
        );

        SystemIndices systemIndices = createSystemIndices(settings);

        CircuitBreakerService circuitBreakerService = createCircuitBreakerService(
            new CircuitBreakerMetrics(telemetryProvider),
            settingsModule.getSettings(),
            settingsModule.getClusterSettings()
        );
        PageCacheRecycler pageCacheRecycler = serviceProvider.newPageCacheRecycler(pluginsService, settings);
        BigArrays bigArrays = serviceProvider.newBigArrays(pluginsService, pageCacheRecycler, circuitBreakerService);

        final RecoverySettings recoverySettings = new RecoverySettings(settings, settingsModule.getClusterSettings());
        RepositoriesModule repositoriesModule = new RepositoriesModule(
            environment,
            pluginsService.filterPlugins(RepositoryPlugin.class).toList(),
            client,
            threadPool,
            clusterService,
            bigArrays,
            xContentRegistry,
            recoverySettings,
            telemetryProvider
        );
        RepositoriesService repositoriesService = repositoriesModule.getRepositoryService();
        final SetOnce<RerouteService> rerouteServiceReference = new SetOnce<>();
        this.clusterInfoService = serviceProvider.newClusterInfoService(pluginsService, settings, clusterService, threadPool, client);
        final InternalSnapshotsInfoService snapshotsInfoService = new InternalSnapshotsInfoService(
            settings,
            clusterService,
            repositoriesService,
            rerouteServiceReference::get
        );
        final ClusterModule clusterModule = new ClusterModule(
            settings,
            clusterService,
            pluginsService.filterPlugins(ClusterPlugin.class).toList(),
            clusterInfoService,
            snapshotsInfoService,
            threadPool,
            systemIndices,
            projectResolver,
            getWriteLoadForecaster(threadPool, settings, clusterService.getClusterSettings()),
            telemetryProvider
        );
        modules.add(clusterModule);

        RerouteService rerouteService = new BatchedRerouteService(clusterService, clusterModule.getAllocationService()::reroute);
        rerouteServiceReference.set(rerouteService);

        clusterInfoService.addListener(
            new DiskThresholdMonitor(
                settings,
                clusterService::state,
                clusterService.getClusterSettings(),
                client,
                threadPool.relativeTimeInMillisSupplier(),
                rerouteService,
                projectResolver
            )::onNewInfo
        );

        IndicesModule indicesModule = new IndicesModule(pluginsService.filterPlugins(MapperPlugin.class).toList());
        modules.add(indicesModule);

        modules.add(new GatewayModule());

        CompatibilityVersions compatibilityVersions = new CompatibilityVersions(
            TransportVersion.current(),
            systemIndices.getMappingsVersions()
        );
        modules.add(
            loadPersistedClusterStateService(clusterService.getClusterSettings(), threadPool, compatibilityVersions, projectResolver)
        );

        final MetaStateService metaStateService = new MetaStateService(nodeEnvironment, xContentRegistry);

        if (DiscoveryNode.isMasterNode(settings)) {
            clusterService.addListener(new SystemIndexMappingUpdateService(systemIndices, client, projectResolver));
        }

        SourceFieldMetrics sourceFieldMetrics = new SourceFieldMetrics(
            telemetryProvider.getMeterRegistry(),
            threadPool::relativeTimeInMillis
        );
        MapperMetrics mapperMetrics = new MapperMetrics(sourceFieldMetrics);
        final List<SearchOperationListener> searchOperationListeners = List.of(
            new ShardSearchPhaseAPMMetrics(telemetryProvider.getMeterRegistry())
        );

        List<? extends SlowLogFieldProvider> slowLogFieldProviders = pluginsService.loadServiceProviders(SlowLogFieldProvider.class);
        // NOTE: the response of index/search slow log fields below must be calculated dynamically on every call
        // because the responses may change dynamically at runtime
        SlowLogFieldProvider slowLogFieldProvider = new SlowLogFieldProvider() {
            public SlowLogFields create() {
                final List<SlowLogFields> fields = new ArrayList<>();
                for (var provider : slowLogFieldProviders) {
                    fields.add(provider.create());
                }
                return new SlowLogFields() {
                    @Override
                    public Map<String, String> indexFields() {
                        return fields.stream()
                            .flatMap(f -> f.indexFields().entrySet().stream())
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    }

                    @Override
                    public Map<String, String> searchFields() {
                        return fields.stream()
                            .flatMap(f -> f.searchFields().entrySet().stream())
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    }

                    @Override
                    public Map<String, String> queryFields() {
                        return fields.stream()
                            .flatMap(f -> f.queryFields().entrySet().stream())
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    }
                };
            }

            public SlowLogFields create(IndexSettings indexSettings) {
                final List<SlowLogFields> fields = new ArrayList<>();
                for (var provider : slowLogFieldProviders) {
                    fields.add(provider.create(indexSettings));
                }
                return new SlowLogFields() {
                    @Override
                    public Map<String, String> indexFields() {
                        return fields.stream()
                            .flatMap(f -> f.indexFields().entrySet().stream())
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    }

                    @Override
                    public Map<String, String> searchFields() {
                        return fields.stream()
                            .flatMap(f -> f.searchFields().entrySet().stream())
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    }

                    @Override
                    public Map<String, String> queryFields() {
                        return fields.stream()
                            .flatMap(f -> f.queryFields().entrySet().stream())
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    }
                };
            }

        };

        IndicesService indicesService = new IndicesServiceBuilder().settings(settings)
            .pluginsService(pluginsService)
            .nodeEnvironment(nodeEnvironment)
            .xContentRegistry(xContentRegistry)
            .analysisRegistry(analysisRegistry)
            .indexNameExpressionResolver(clusterModule.getIndexNameExpressionResolver())
            .mapperRegistry(indicesModule.getMapperRegistry())
            .namedWriteableRegistry(namedWriteableRegistry)
            .threadPool(threadPool)
            .indexScopedSettings(settingsModule.getIndexScopedSettings())
            .circuitBreakerService(circuitBreakerService)
            .bigArrays(bigArrays)
            .scriptService(scriptService)
            .clusterService(clusterService)
            .projectResolver(projectResolver)
            .client(client)
            .metaStateService(metaStateService)
            .valuesSourceRegistry(searchModule.getValuesSourceRegistry())
            .requestCacheKeyDifferentiator(searchModule.getRequestCacheKeyDifferentiator())
            .mapperMetrics(mapperMetrics)
            .searchOperationListeners(searchOperationListeners)
            .slowLogFieldProvider(slowLogFieldProvider)
            .build();

        final var parameters = new IndexSettingProvider.Parameters(clusterService, indicesService::createIndexMapperServiceForValidation);
        IndexSettingProviders indexSettingProviders = new IndexSettingProviders(
            Sets.union(
                builtinIndexSettingProviders(),
                pluginsService.flatMap(p -> p.getAdditionalIndexSettingProviders(parameters)).collect(Collectors.toSet())
            )
        );

        final ShardLimitValidator shardLimitValidator = new ShardLimitValidator(settings, clusterService);

        final MetadataCreateIndexService metadataCreateIndexService = new MetadataCreateIndexService(
            settings,
            clusterService,
            indicesService,
            clusterModule.getAllocationService(),
            shardLimitValidator,
            environment,
            settingsModule.getIndexScopedSettings(),
            threadPool,
            xContentRegistry,
            systemIndices,
            forbidPrivateIndexSettings,
            indexSettingProviders
        );

        final MetadataUpdateSettingsService metadataUpdateSettingsService = new MetadataUpdateSettingsService(
            clusterService,
            clusterModule.getAllocationService(),
            settingsModule.getIndexScopedSettings(),
            indicesService,
            shardLimitValidator,
            threadPool
        );

        final DataStreamGlobalRetentionSettings dataStreamGlobalRetentionSettings = createDataStreamServicesAndGlobalRetentionResolver(
            threadPool,
            clusterService,
            indicesService,
            metadataCreateIndexService
        );

        final IndexingPressure indexingLimits = new IndexingPressure(settings);

        PluginServiceInstances pluginServices = new PluginServiceInstances(
            client,
            clusterService,
            rerouteService,
            threadPool,
            createResourceWatcherService(settings, threadPool),
            scriptService,
            xContentRegistry,
            environment,
            nodeEnvironment,
            namedWriteableRegistry,
            clusterModule.getIndexNameExpressionResolver(),
            repositoriesService,
            telemetryProvider,
            clusterModule.getAllocationService(),
            indicesService,
            featureService,
            systemIndices,
            dataStreamGlobalRetentionSettings,
            documentParsingProvider,
            taskManager,
            projectResolver,
            slowLogFieldProvider,
            indexingLimits
        );

        Collection<?> pluginComponents = pluginsService.flatMap(plugin -> {
            Collection<?> allItems = plugin.createComponents(pluginServices);
            List<?> componentObjects = allItems.stream().filter(not(x -> x instanceof Class<?>)).toList();
            List<? extends Class<?>> classes = allItems.stream().filter(x -> x instanceof Class<?>).map(x -> (Class<?>) x).toList();

            // Then, injection
            Collection<?> componentsFromInjector;
            if (classes.isEmpty()) {
                componentsFromInjector = Set.of();
            } else {
                logger.debug("Using injector to instantiate classes for {}: {}", plugin.getClass().getSimpleName(), classes);
                var injector = org.elasticsearch.injection.Injector.create();
                injector.addInstances(componentObjects);
                addRecordContents(injector, pluginServices);
                var resultMap = injector.inject(classes);
                // For now, assume we want all components added to the Guice injector
                var distinctObjects = newSetFromMap(new IdentityHashMap<>());
                distinctObjects.addAll(resultMap.values());
                componentsFromInjector = distinctObjects;
            }

            // Return both
            return Stream.of(componentObjects, componentsFromInjector).flatMap(Collection::stream).toList();
        }).toList();

        var terminationHandlers = pluginsService.loadServiceProviders(TerminationHandlerProvider.class)
            .stream()
            .map(TerminationHandlerProvider::handler);
        terminationHandler = getSinglePlugin(terminationHandlers, TerminationHandler.class).orElse(null);

        if (clusterInfoService instanceof InternalClusterInfoService icis) {
            icis.setHeapUsageSupplier(serviceProvider.newHeapUsageSupplier(pluginsService));
        }
        final IncrementalBulkService incrementalBulkService = new IncrementalBulkService(client, indexingLimits);

        final ResponseCollectorService responseCollectorService = new ResponseCollectorService(clusterService);
        modules.bindToInstance(ResponseCollectorService.class, responseCollectorService);

        var reservedStateHandlerProviders = pluginsService.loadServiceProviders(ReservedClusterStateHandlerProvider.class);

        ActionModule actionModule = new ActionModule(
            settings,
            clusterModule.getIndexNameExpressionResolver(),
            namedWriteableRegistry,
            settingsModule.getIndexScopedSettings(),
            settingsModule.getClusterSettings(),
            settingsModule.getSettingsFilter(),
            threadPool,
            pluginsService.filterPlugins(ActionPlugin.class).toList(),
            client,
            circuitBreakerService,
            createUsageService(),
            systemIndices,
            telemetryProvider,
            clusterService,
            rerouteService,
            buildReservedClusterStateHandlers(reservedStateHandlerProviders, settingsModule),
            buildReservedProjectStateHandlers(
                reservedStateHandlerProviders,
                settingsModule,
                clusterService,
                indicesService,
                systemIndices,
                indexSettingProviders,
                metadataCreateIndexService,
                dataStreamGlobalRetentionSettings
            ),
            pluginsService.loadSingletonServiceProvider(RestExtension.class, RestExtension::allowAll),
            incrementalBulkService,
            projectResolver
        );
        modules.add(actionModule);

        final NetworkService networkService = new NetworkService(
            pluginsService.filterPlugins(DiscoveryPlugin.class)
                .map(d -> d.getCustomNameResolver(environment.settings()))
                .filter(Objects::nonNull)
                .toList()
        );
        final NetworkModule networkModule = new NetworkModule(
            settings,
            pluginsService.filterPlugins(NetworkPlugin.class).toList(),
            threadPool,
            bigArrays,
            pageCacheRecycler,
            circuitBreakerService,
            namedWriteableRegistry,
            xContentRegistry,
            networkService,
            actionModule.getRestController(),
            actionModule::copyRequestHeadersToThreadContext,
            clusterService.getClusterSettings(),
            telemetryProvider.getTracer()
        );

        var indexTemplateMetadataUpgraders = pluginsService.map(Plugin::getIndexTemplateMetadataUpgrader).toList();
        List<Map<String, UnaryOperator<Metadata.ProjectCustom>>> customMetadataUpgraders = pluginsService.map(
            Plugin::getProjectCustomMetadataUpgraders
        ).toList();
        modules.bindToInstance(MetadataUpgrader.class, new MetadataUpgrader(indexTemplateMetadataUpgraders, customMetadataUpgraders));

        final IndexMetadataVerifier indexMetadataVerifier = new IndexMetadataVerifier(
            settings,
            clusterService,
            xContentRegistry,
            indicesModule.getMapperRegistry(),
            settingsModule.getIndexScopedSettings(),
            scriptService,
            mapperMetrics
        );
        if (DiscoveryNode.isMasterNode(settings)) {
            clusterService.addListener(new SystemIndexMetadataUpgradeService(systemIndices, clusterService));
            clusterService.addListener(new TemplateUpgradeService(client, clusterService, threadPool, indexTemplateMetadataUpgraders));
        }
        final Transport transport = networkModule.getTransportSupplier().get();
        final TransportService transportService = serviceProvider.newTransportService(
            pluginsService,
            settings,
            transport,
            threadPool,
            networkModule.getTransportInterceptor(),
            localNodeFactory,
            settingsModule.getClusterSettings(),
            taskManager,
            telemetryProvider.getTracer()
        );
        final SearchResponseMetrics searchResponseMetrics = new SearchResponseMetrics(telemetryProvider.getMeterRegistry());
        final SearchTransportService searchTransportService = new SearchTransportService(
            transportService,
            client,
            SearchExecutionStatsCollector.makeWrapper(responseCollectorService)
        );
        final HttpServerTransport httpServerTransport = serviceProvider.newHttpTransport(pluginsService, networkModule);

        SnapshotsService snapshotsService = new SnapshotsService(
            settings,
            clusterService,
            rerouteService,
            clusterModule.getIndexNameExpressionResolver(),
            repositoriesService,
            transportService,
            actionModule.getActionFilters(),
            systemIndices
        );
        SnapshotShardsService snapshotShardsService = new SnapshotShardsService(
            settings,
            clusterService,
            repositoriesService,
            transportService,
            indicesService
        );

        actionModule.getReservedClusterStateService().installClusterStateHandler(new ReservedRepositoryAction(repositoriesService));
        actionModule.getReservedClusterStateService().installProjectStateHandler(new ReservedPipelineAction());

        var fileSettingsHealthIndicatorPublisher = new FileSettingsService.FileSettingsHealthIndicatorPublisherImpl(clusterService, client);
        var fileSettingsHealthTracker = new FileSettingsHealthTracker(settings, fileSettingsHealthIndicatorPublisher);
        FileSettingsService fileSettingsService = pluginsService.loadSingletonServiceProvider(
            FileSettingsServiceProvider.class,
            () -> FileSettingsService::new
        ).construct(clusterService, actionModule.getReservedClusterStateService(), environment, fileSettingsHealthTracker);

        RestoreService restoreService = new RestoreService(
            clusterService,
            repositoriesService,
            clusterModule.getAllocationService(),
            metadataCreateIndexService,
            indexMetadataVerifier,
            shardLimitValidator,
            systemIndices,
            indicesService,
            fileSettingsService,
            threadPool
        );

        DiscoveryModule discoveryModule = createDiscoveryModule(
            settings,
            threadPool,
            transportService,
            networkService,
            clusterService,
            clusterModule.getAllocationService(),
            rerouteService,
            circuitBreakerService,
            compatibilityVersions,
            featureService
        );
        nodeService = new NodeService(
            settings,
            threadPool,
            new MonitorService(settings, nodeEnvironment, threadPool),
            discoveryModule.getCoordinator(),
            transportService,
            indicesService,
            pluginsService,
            circuitBreakerService,
            scriptService,
            httpServerTransport,
            ingestService,
            clusterService,
            settingsModule.getSettingsFilter(),
            responseCollectorService,
            searchTransportService,
            indexingLimits,
            searchModule.getValuesSourceRegistry().getUsageService(),
            repositoriesService,
            compatibilityVersions
        );

        final TimeValue metricsInterval = settings.getAsTime("telemetry.agent.metrics_interval", TimeValue.timeValueSeconds(10));
        final NodeMetrics nodeMetrics = new NodeMetrics(telemetryProvider.getMeterRegistry(), nodeService, metricsInterval);
        final IndicesMetrics indicesMetrics = new IndicesMetrics(telemetryProvider.getMeterRegistry(), indicesService, metricsInterval);

        OnlinePrewarmingService onlinePrewarmingService = pluginsService.loadSingletonServiceProvider(
            OnlinePrewarmingServiceProvider.class,
            () -> OnlinePrewarmingServiceProvider.DEFAULT
        ).create(clusterService.getSettings(), threadPool, clusterService);
        final SearchService searchService = serviceProvider.newSearchService(
            pluginsService,
            clusterService,
            indicesService,
            threadPool,
            scriptService,
            bigArrays,
            searchModule.getFetchPhase(),
            circuitBreakerService,
            systemIndices.getExecutorSelector(),
            telemetryProvider.getTracer(),
            onlinePrewarmingService
        );

        final ShutdownPrepareService shutdownPrepareService = new ShutdownPrepareService(settings, httpServerTransport, terminationHandler);

        modules.add(loadPersistentTasksService(settingsModule, clusterService, threadPool, clusterModule.getIndexNameExpressionResolver()));

        modules.add(
            loadPluginShutdownService(clusterService),
            loadDiagnosticServices(
                settings,
                discoveryModule.getCoordinator(),
                clusterService,
                transportService,
                threadPool,
                telemetryProvider,
                repositoriesService
            )
        );

        RecoveryPlannerService recoveryPlannerService = getRecoveryPlannerService(threadPool, clusterService, repositoriesService);
        modules.add(b -> {
            serviceProvider.processRecoverySettings(pluginsService, settingsModule.getClusterSettings(), recoverySettings);
            SnapshotFilesProvider snapshotFilesProvider = new SnapshotFilesProvider(repositoriesService);
            var peerRecovery = new PeerRecoverySourceService(
                transportService,
                indicesService,
                clusterService,
                recoverySettings,
                recoveryPlannerService
            );
            resourcesToClose.add(peerRecovery);
            b.bind(PeerRecoverySourceService.class).toInstance(peerRecovery);
            b.bind(PeerRecoveryTargetService.class)
                .toInstance(
                    new PeerRecoveryTargetService(
                        client,
                        threadPool,
                        transportService,
                        recoverySettings,
                        clusterService,
                        snapshotFilesProvider
                    )
                );
        });

        modules.add(loadPluginComponents(pluginComponents));

        DataStreamAutoShardingService dataStreamAutoShardingService = new DataStreamAutoShardingService(
            settings,
            clusterService,
            threadPool::absoluteTimeInMillis
        );
        dataStreamAutoShardingService.init();

        modules.add(b -> {
            b.bind(NodeService.class).toInstance(nodeService);
            b.bind(BigArrays.class).toInstance(bigArrays);
            b.bind(PageCacheRecycler.class).toInstance(pageCacheRecycler);
            b.bind(IngestService.class).toInstance(ingestService);
            b.bind(IndexingPressure.class).toInstance(indexingLimits);
            b.bind(IncrementalBulkService.class).toInstance(incrementalBulkService);
            b.bind(AggregationUsageService.class).toInstance(searchModule.getValuesSourceRegistry().getUsageService());
            b.bind(MetaStateService.class).toInstance(metaStateService);
            b.bind(IndicesService.class).toInstance(indicesService);
            b.bind(MetadataCreateIndexService.class).toInstance(metadataCreateIndexService);
            b.bind(MetadataUpdateSettingsService.class).toInstance(metadataUpdateSettingsService);
            b.bind(SearchService.class).toInstance(searchService);
            b.bind(SearchResponseMetrics.class).toInstance(searchResponseMetrics);
            b.bind(SearchTransportService.class).toInstance(searchTransportService);
            b.bind(SearchPhaseController.class).toInstance(new SearchPhaseController(searchService::aggReduceContextBuilder));
            b.bind(Transport.class).toInstance(transport);
            b.bind(TransportService.class).toInstance(transportService);
            b.bind(NodeMetrics.class).toInstance(nodeMetrics);
            b.bind(IndicesMetrics.class).toInstance(indicesMetrics);
            b.bind(NetworkService.class).toInstance(networkService);
            b.bind(IndexMetadataVerifier.class).toInstance(indexMetadataVerifier);
            b.bind(ClusterInfoService.class).toInstance(clusterInfoService);
            b.bind(SnapshotsInfoService.class).toInstance(snapshotsInfoService);
            b.bind(FeatureService.class).toInstance(featureService);
            b.bind(HttpServerTransport.class).toInstance(httpServerTransport);
            b.bind(RepositoriesService.class).toInstance(repositoriesService);
            b.bind(SnapshotsService.class).toInstance(snapshotsService);
            b.bind(SnapshotShardsService.class).toInstance(snapshotShardsService);
            b.bind(RestoreService.class).toInstance(restoreService);
            b.bind(RerouteService.class).toInstance(rerouteService);
            b.bind(ShardLimitValidator.class).toInstance(shardLimitValidator);
            b.bind(IndexSettingProviders.class).toInstance(indexSettingProviders);
            b.bind(FileSettingsService.class).toInstance(fileSettingsService);
            b.bind(CompatibilityVersions.class).toInstance(compatibilityVersions);
            b.bind(DataStreamAutoShardingService.class).toInstance(dataStreamAutoShardingService);
            b.bind(FailureStoreMetrics.class).toInstance(failureStoreMetrics);
            b.bind(ShutdownPrepareService.class).toInstance(shutdownPrepareService);
            b.bind(OnlinePrewarmingService.class).toInstance(onlinePrewarmingService);
        });

        if (ReadinessService.enabled(environment)) {
            modules.bindToInstance(
                ReadinessService.class,
                serviceProvider.newReadinessService(pluginsService, clusterService, environment)
            );
        }

        injector = modules.createInjector();

        postInjection(clusterModule, actionModule, clusterService, transportService, featureService);
    }

    /**
     * For each "component" (getter) <em>c</em> of a {@link Record},
     * calls {@link org.elasticsearch.injection.Injector#addInstance(Object) Injector.addInstance}
     * to register the value with the component's declared type.
     */
    @SuppressForbidden(reason = "Can't call invokeExact because we don't know the exact Record subtype statically")
    private static <T> void addRecordContents(org.elasticsearch.injection.Injector injector, Record r) {
        for (var c : r.getClass().getRecordComponents()) {
            try {
                @SuppressWarnings("unchecked")
                Class<T> type = (Class<T>) c.getType(); // T represents the declared type of the record component, whatever it is
                injector.addInstance(type, type.cast(lookup().unreflect(c.getAccessor()).invoke(r)));
            } catch (Throwable e) {
                throw new IllegalStateException("Unable to read record component " + c, e);
            }
        }
    }

    private ClusterService createClusterService(SettingsModule settingsModule, ThreadPool threadPool, TaskManager taskManager) {
        ClusterService clusterService = new ClusterService(
            settingsModule.getSettings(),
            settingsModule.getClusterSettings(),
            settingsModule.getProjectScopedSettings(),
            threadPool,
            taskManager
        );
        resourcesToClose.add(clusterService);

        Set<Setting<?>> consistentSettings = settingsModule.getConsistentSettings();
        if (consistentSettings.isEmpty() == false) {
            clusterService.addLocalNodeMasterListener(
                new ConsistentSettingsService(settingsModule.getSettings(), clusterService, consistentSettings).newHashPublisher()
            );
        }
        return clusterService;
    }

    private UsageService createUsageService() {
        UsageService usageService = new UsageService();
        modules.bindToInstance(UsageService.class, usageService);
        return usageService;
    }

    private SystemIndices createSystemIndices(Settings settings) {
        List<SystemIndices.Feature> features = pluginsService.filterPlugins(SystemIndexPlugin.class).map(plugin -> {
            SystemIndices.validateFeatureName(plugin.getFeatureName(), plugin.getClass().getCanonicalName());
            return SystemIndices.Feature.fromSystemIndexPlugin(plugin, settings);
        }).toList();

        SystemIndices systemIndices = new SystemIndices(features);
        modules.add(b -> {
            b.bind(SystemIndices.class).toInstance(systemIndices);
            b.bind(ExecutorSelector.class).toInstance(systemIndices.getExecutorSelector());
        });
        return systemIndices;
    }

    private ResourceWatcherService createResourceWatcherService(Settings settings, ThreadPool threadPool) {
        ResourceWatcherService resourceWatcherService = new ResourceWatcherService(settings, threadPool);
        resourcesToClose.add(resourceWatcherService);
        modules.bindToInstance(ResourceWatcherService.class, resourceWatcherService);
        return resourceWatcherService;
    }

    private Module loadPluginShutdownService(ClusterService clusterService) {
        PluginShutdownService pluginShutdownService = new PluginShutdownService(
            pluginsService.filterPlugins(ShutdownAwarePlugin.class).toList()
        );
        clusterService.addListener(pluginShutdownService);

        return b -> b.bind(PluginShutdownService.class).toInstance(pluginShutdownService);
    }

    private Module loadDiagnosticServices(
        Settings settings,
        Coordinator coordinator,
        ClusterService clusterService,
        TransportService transportService,
        ThreadPool threadPool,
        TelemetryProvider telemetryProvider,
        RepositoriesService repositoriesService
    ) {

        MasterHistoryService masterHistoryService = new MasterHistoryService(transportService, threadPool, clusterService);
        CoordinationDiagnosticsService coordinationDiagnosticsService = new CoordinationDiagnosticsService(
            clusterService,
            transportService,
            coordinator,
            masterHistoryService
        );

        var serverHealthIndicatorServices = Stream.of(
            new StableMasterHealthIndicatorService(coordinationDiagnosticsService, clusterService),
            new RepositoryIntegrityHealthIndicatorService(clusterService),
            new DiskHealthIndicatorService(clusterService),
            new ShardsCapacityHealthIndicatorService(clusterService),
            new FileSettingsHealthIndicatorService()
        );
        var pluginHealthIndicatorServices = pluginsService.filterPlugins(HealthPlugin.class)
            .flatMap(plugin -> plugin.getHealthIndicatorServices().stream());

        HealthService healthService = new HealthService(
            Stream.concat(serverHealthIndicatorServices, pluginHealthIndicatorServices).toList(),
            threadPool
        );
        HealthPeriodicLogger healthPeriodicLogger = HealthPeriodicLogger.create(
            settings,
            clusterService,
            client,
            healthService,
            telemetryProvider
        );
        HealthMetadataService healthMetadataService = HealthMetadataService.create(clusterService, settings);

        List<HealthTracker<?>> healthTrackers = List.of(
            new DiskHealthTracker(nodeService, clusterService),
            new RepositoriesHealthTracker(repositoriesService)
        );
        LocalHealthMonitor localHealthMonitor = LocalHealthMonitor.create(settings, clusterService, threadPool, client, healthTrackers);
        HealthInfoCache nodeHealthOverview = HealthInfoCache.create(clusterService);

        return b -> {
            b.bind(HealthService.class).toInstance(healthService);
            b.bind(MasterHistoryService.class).toInstance(masterHistoryService);
            b.bind(CoordinationDiagnosticsService.class).toInstance(coordinationDiagnosticsService);
            b.bind(HealthMetadataService.class).toInstance(healthMetadataService);
            b.bind(LocalHealthMonitor.class).toInstance(localHealthMonitor);
            b.bind(HealthInfoCache.class).toInstance(nodeHealthOverview);
            b.bind(HealthApiStats.class).toInstance(new HealthApiStats());
            b.bind(HealthPeriodicLogger.class).toInstance(healthPeriodicLogger);
        };
    }

    private Module loadPluginComponents(Collection<?> pluginComponents) {
        List<LifecycleComponent> pluginLifecycleComponents = pluginComponents.stream().map(p -> {
            if (p instanceof PluginComponentBinding<?, ?> pcb) {
                return pcb.impl();
            }
            return p;
        }).filter(p -> p instanceof LifecycleComponent).map(p -> (LifecycleComponent) p).toList();
        resourcesToClose.addAll(pluginLifecycleComponents);
        this.pluginLifecycleComponents = pluginLifecycleComponents;

        List<ReloadablePlugin> reloadablePlugins = pluginsService.filterPlugins(ReloadablePlugin.class).toList();
        pluginsService.filterPlugins(ReloadAwarePlugin.class).forEach(p -> p.setReloadCallback(wrapPlugins(reloadablePlugins)));

        return b -> pluginComponents.forEach(p -> {
            if (p instanceof PluginComponentBinding<?, ?> pcb) {
                @SuppressWarnings("unchecked")
                Class<Object> clazz = (Class<Object>) pcb.inter();
                b.bind(clazz).toInstance(pcb.impl());
            } else {
                @SuppressWarnings("unchecked")
                Class<Object> clazz = (Class<Object>) p.getClass();
                b.bind(clazz).toInstance(p);
            }
        });
    }

    private void postInjection(
        ClusterModule clusterModule,
        ActionModule actionModule,
        ClusterService clusterService,
        TransportService transportService,
        FeatureService featureService
    ) {
        // We allocate copies of existing shards by looking for a viable copy of the shard in the cluster and assigning the shard there.
        // The search for viable copies is triggered by an allocation attempt (i.e. a reroute) and is performed asynchronously. When it
        // completes we trigger another reroute to try the allocation again. This means there is a circular dependency: the allocation
        // service needs access to the existing shards allocators (e.g. the GatewayAllocator) which need to be able to trigger a
        // reroute, which needs to call into the allocation service. We close the loop here:
        clusterModule.setExistingShardsAllocators(injector.getInstance(GatewayAllocator.class));

        // Due to Java's type erasure with generics, the injector can't give us exactly what we need, and we have
        // to resort to some evil casting.
        @SuppressWarnings("rawtypes")
        Map<ActionType<?>, TransportAction<?, ?>> actions = forciblyCast(injector.getInstance(new Key<Map<ActionType, TransportAction>>() {
        }));

        client.initialize(
            actions,
            transportService.getTaskManager(),
            () -> clusterService.localNode().getId(),
            transportService.getLocalNodeConnection(),
            transportService.getRemoteClusterService()
        );

        logger.debug("initializing HTTP handlers ...");
        actionModule.initRestHandlers(() -> clusterService.state().nodesIfRecovered(), f -> {
            ClusterState state = clusterService.state();
            return state.clusterRecovered() && featureService.clusterHasFeature(state, f);
        });
        logger.info("initialized");
    }

    private DocumentParsingProvider getDocumentParsingProvider() {
        return getSinglePlugin(DocumentParsingProviderPlugin.class).map(DocumentParsingProviderPlugin::getDocumentParsingProvider)
            .orElse(DocumentParsingProvider.EMPTY_INSTANCE);
    }

    /**
     * Create and initialize a new {@link CircuitBreakerService} based on the settings provided.
     *
     * @see Node#BREAKER_TYPE_KEY
     */
    private CircuitBreakerService createCircuitBreakerService(
        CircuitBreakerMetrics metrics,
        Settings settings,
        ClusterSettings clusterSettings
    ) {
        var pluginBreakers = pluginsService.filterPlugins(CircuitBreakerPlugin.class)
            .map(p -> Tuple.tuple(p, p.getCircuitBreaker(settings)))
            .toList();

        String type = Node.BREAKER_TYPE_KEY.get(settings);
        CircuitBreakerService circuitBreakerService = switch (type) {
            case "hierarchy" -> new HierarchyCircuitBreakerService(
                metrics,
                settings,
                pluginBreakers.stream().map(Tuple::v2).toList(),
                clusterSettings
            );
            case "none" -> new NoneCircuitBreakerService();
            default -> throw new IllegalArgumentException("Unknown circuit breaker type [" + type + "]");
        };
        modules.bindToInstance(CircuitBreakerService.class, circuitBreakerService);

        pluginBreakers.forEach(t -> {
            final CircuitBreaker circuitBreaker = circuitBreakerService.getBreaker(t.v2().getName());
            t.v1().setCircuitBreaker(circuitBreaker);
        });

        return circuitBreakerService;
    }

    /**
     * Wrap a group of reloadable plugins into a single reloadable plugin interface
     *
     * @param reloadablePlugins A list of reloadable plugins
     * @return A single ReloadablePlugin that, upon reload, reloads the plugins it wraps
     */
    private static ReloadablePlugin wrapPlugins(List<ReloadablePlugin> reloadablePlugins) {
        return settings -> {
            for (ReloadablePlugin plugin : reloadablePlugins) {
                try {
                    plugin.reload(settings);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        };
    }

    private RecoveryPlannerService getRecoveryPlannerService(
        ThreadPool threadPool,
        ClusterService clusterService,
        RepositoriesService repositoriesService
    ) {
        var recoveryPlannerServices = pluginsService.filterPlugins(RecoveryPlannerPlugin.class)
            .map(
                plugin -> plugin.createRecoveryPlannerService(
                    new ShardSnapshotsService(client, repositoriesService, threadPool, clusterService)
                )
            )
            .flatMap(Optional::stream);
        return getSinglePlugin(recoveryPlannerServices, RecoveryPlannerService.class).orElseGet(PeerOnlyRecoveryPlannerService::new);
    }

    private WriteLoadForecaster getWriteLoadForecaster(ThreadPool threadPool, Settings settings, ClusterSettings clusterSettings) {
        var writeLoadForecasters = pluginsService.filterPlugins(ClusterPlugin.class)
            .flatMap(clusterPlugin -> clusterPlugin.createWriteLoadForecasters(threadPool, settings, clusterSettings).stream());

        WriteLoadForecaster forecaster = getSinglePlugin(writeLoadForecasters, WriteLoadForecaster.class).orElse(
            WriteLoadForecaster.DEFAULT
        );
        modules.bindToInstance(WriteLoadForecaster.class, forecaster);
        return forecaster;
    }

    private Module loadPersistedClusterStateService(
        ClusterSettings clusterSettings,
        ThreadPool threadPool,
        CompatibilityVersions compatibilityVersions,
        ProjectResolver projectResolver
    ) {
        var persistedClusterStateServiceFactories = pluginsService.filterPlugins(ClusterCoordinationPlugin.class)
            .map(ClusterCoordinationPlugin::getPersistedClusterStateServiceFactory)
            .flatMap(Optional::stream);

        var service = getSinglePlugin(
            persistedClusterStateServiceFactories,
            ClusterCoordinationPlugin.PersistedClusterStateServiceFactory.class
        ).map(f -> f.newPersistedClusterStateService(nodeEnvironment, xContentRegistry, clusterSettings, threadPool, compatibilityVersions))
            .orElseGet(
                () -> new PersistedClusterStateService(
                    nodeEnvironment,
                    xContentRegistry,
                    clusterSettings,
                    threadPool.relativeTimeInMillisSupplier(),
                    projectResolver::supportsMultipleProjects
                )
            );

        return b -> b.bind(PersistedClusterStateService.class).toInstance(service);
    }

    private List<ReservedClusterStateHandler<ClusterState, ?>> buildReservedClusterStateHandlers(
        List<? extends ReservedClusterStateHandlerProvider> handlers,
        SettingsModule settingsModule
    ) {
        List<ReservedClusterStateHandler<ClusterState, ?>> reservedStateHandlers = new ArrayList<>();

        // add all reserved state handlers from server
        reservedStateHandlers.add(new ReservedClusterSettingsAction(settingsModule.getClusterSettings()));

        // add all reserved state handlers from plugins
        handlers.forEach(h -> reservedStateHandlers.addAll(h.clusterHandlers()));

        return reservedStateHandlers;
    }

    private List<ReservedClusterStateHandler<ProjectMetadata, ?>> buildReservedProjectStateHandlers(
        List<? extends ReservedClusterStateHandlerProvider> handlers,
        SettingsModule settingsModule,
        ClusterService clusterService,
        IndicesService indicesService,
        SystemIndices systemIndices,
        IndexSettingProviders indexSettingProviders,
        MetadataCreateIndexService metadataCreateIndexService,
        DataStreamGlobalRetentionSettings globalRetentionSettings
    ) {
        List<ReservedClusterStateHandler<ProjectMetadata, ?>> reservedStateHandlers = new ArrayList<>();

        var templateService = new MetadataIndexTemplateService(
            clusterService,
            metadataCreateIndexService,
            indicesService,
            settingsModule.getIndexScopedSettings(),
            xContentRegistry,
            systemIndices,
            indexSettingProviders,
            globalRetentionSettings
        );
        reservedStateHandlers.add(new ReservedComposableIndexTemplateAction(templateService, settingsModule.getIndexScopedSettings()));

        // add all reserved state handlers from plugins
        handlers.forEach(h -> reservedStateHandlers.addAll(h.projectHandlers()));

        return reservedStateHandlers;
    }

    private DiscoveryModule createDiscoveryModule(
        Settings settings,
        ThreadPool threadPool,
        TransportService transportService,
        NetworkService networkService,
        ClusterService clusterService,
        AllocationService allocationService,
        RerouteService rerouteService,
        CircuitBreakerService circuitBreakerService,
        CompatibilityVersions compatibilityVersions,
        FeatureService featureService
    ) {
        GatewayMetaState gatewayMetaState = new GatewayMetaState();
        FsHealthService fsHealthService = new FsHealthService(settings, clusterService.getClusterSettings(), threadPool, nodeEnvironment);

        DiscoveryModule module = new DiscoveryModule(
            settings,
            transportService,
            client,
            namedWriteableRegistry,
            networkService,
            clusterService.getMasterService(),
            clusterService.getClusterApplierService(),
            clusterService.getClusterSettings(),
            pluginsService.filterPlugins(DiscoveryPlugin.class).toList(),
            pluginsService.filterPlugins(ClusterCoordinationPlugin.class).toList(),
            allocationService,
            environment.configDir(),
            gatewayMetaState,
            rerouteService,
            fsHealthService,
            circuitBreakerService,
            compatibilityVersions,
            featureService
        );

        modules.add(module, b -> {
            b.bind(GatewayMetaState.class).toInstance(gatewayMetaState);
            b.bind(FsHealthService.class).toInstance(fsHealthService);
        });

        return module;
    }

    private Module loadPersistentTasksService(
        SettingsModule settingsModule,
        ClusterService clusterService,
        ThreadPool threadPool,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        PersistentTasksService persistentTasksService = new PersistentTasksService(clusterService, threadPool, client);
        HealthNodeTaskExecutor healthNodeTaskExecutor = HealthNodeTaskExecutor.create(
            clusterService,
            persistentTasksService,
            settingsModule.getSettings(),
            clusterService.getClusterSettings()
        );
        Stream<PersistentTasksExecutor<?>> builtinTaskExecutors = Stream.of(healthNodeTaskExecutor);

        Stream<PersistentTasksExecutor<?>> pluginTaskExecutors = pluginsService.filterPlugins(PersistentTaskPlugin.class)
            .map(p -> p.getPersistentTasksExecutor(clusterService, threadPool, client, settingsModule, indexNameExpressionResolver))
            .flatMap(List::stream);

        PersistentTasksExecutorRegistry registry = new PersistentTasksExecutorRegistry(
            Stream.concat(pluginTaskExecutors, builtinTaskExecutors).toList()
        );
        PersistentTasksClusterService persistentTasksClusterService = new PersistentTasksClusterService(
            settingsModule.getSettings(),
            registry,
            clusterService,
            threadPool
        );
        resourcesToClose.add(persistentTasksClusterService);

        return b -> {
            b.bind(PersistentTasksService.class).toInstance(persistentTasksService);
            b.bind(HealthNodeTaskExecutor.class).toInstance(healthNodeTaskExecutor);
            b.bind(PersistentTasksExecutorRegistry.class).toInstance(registry);
            b.bind(PersistentTasksClusterService.class).toInstance(persistentTasksClusterService);
        };
    }

    private Set<IndexSettingProvider> builtinIndexSettingProviders() {
        return Set.of(new IndexMode.IndexModeSettingsProvider());
    }
}
