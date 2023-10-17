/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.node;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Build;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.repositories.reservedstate.ReservedRepositoryAction;
import org.elasticsearch.action.admin.indices.template.reservedstate.ReservedComposableIndexTemplateAction;
import org.elasticsearch.action.ingest.ReservedPipelineAction;
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
import org.elasticsearch.cluster.coordination.CoordinationDiagnosticsService;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.MasterHistoryService;
import org.elasticsearch.cluster.coordination.Reconfigurator;
import org.elasticsearch.cluster.coordination.StableMasterHealthIndicatorService;
import org.elasticsearch.cluster.desirednodes.DesiredNodesSettingsValidator;
import org.elasticsearch.cluster.metadata.IndexMetadataVerifier;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataDataStreamsService;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.metadata.MetadataUpdateSettingsService;
import org.elasticsearch.cluster.metadata.SystemIndexMetadataUpgradeService;
import org.elasticsearch.cluster.metadata.TemplateUpgradeService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.BatchedRerouteService;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdMonitor;
import org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorService;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.TransportVersionsFixupListener;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
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
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Strings;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
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
import org.elasticsearch.health.stats.HealthApiStats;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettingProviders;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.indices.ExecutorSelector;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.indices.SystemIndexMappingUpdateService;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.breaker.BreakerSettings;
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
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.monitor.MonitorService;
import org.elasticsearch.monitor.fs.FsHealthService;
import org.elasticsearch.monitor.jvm.JvmInfo;
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
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.HealthPlugin;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.InferenceServicePlugin;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.MetadataUpgrader;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.RecoveryPlannerPlugin;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.plugins.ShutdownAwarePlugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.plugins.TelemetryPlugin;
import org.elasticsearch.plugins.internal.DocumentParsingObserver;
import org.elasticsearch.plugins.internal.DocumentParsingObserverPlugin;
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
import org.elasticsearch.rest.RestController;
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
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.upgrades.SystemIndexMigrationExecutor;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.common.util.CollectionUtils.concatLists;
import static org.elasticsearch.core.Types.forciblyCast;

/**
 * Class uses to perform all the operations needed to construct a {@link Node} instance.
 * <p>
 * Constructing a {@link Node} is a complex operation, involving many interdependent services.
 * Separating out this logic into a dedicated class is a lot clearer & more flexible than
 * doing all this logic inside a constructor in {@link Node}.
 */
class NodeConstructor {

    /**
     * Performs node construction operations
     *
     * @param initialEnvironment         the initial environment for this node, which will be added to by plugins
     * @param serviceProvider            provides various service implementations that could be mocked
     * @param forbidPrivateIndexSettings whether or not private index settings are forbidden when creating an index; this is used in the
     *                                   test framework for tests that rely on being able to set private settings
     */
    static NodeConstructor construct(
        Environment initialEnvironment,
        NodeServiceProvider serviceProvider,
        boolean forbidPrivateIndexSettings
    ) {
        List<Closeable> closeables = new ArrayList<>();
        try {
            /*
             * NOTE: This is a work in progress. Order is important here.
             */
            var constructor = new NodeConstructor(closeables);
            constructor.checkEnvironment(initialEnvironment);
            constructor.createEnvironment(initialEnvironment, serviceProvider.pluginsServiceCtor(initialEnvironment));
            constructor.createThreadPools();
            constructor.createTracer();
            constructor.createTaskManager();
            constructor.loadAdditionalSettings();
            constructor.createClient();
            constructor.createScriptService(serviceProvider);
            constructor.createAnalysisModule();
            // this is as early as we can validate settings at this point. we already pass them to ScriptModule as well as ThreadPool
            // so we might be late here already
            constructor.createSettingsModule();
            // creating NodeEnvironment breaks the ability to rollback to 7.x on an 8.0 upgrade (upgradeLegacyNodeFolders) so do this
            // after settings validation.
            constructor.createNodeEnvironment();
            constructor.createLocalNodeFactory();
            constructor.createNetworkService();
            constructor.createClusterService();
            constructor.createInferenceServiceRegistry();
            constructor.createIngestService();
            constructor.createClusterInfoService(serviceProvider);
            constructor.createUsageService();
            constructor.createSearchModule();
            constructor.createNamedWriteableRegistry();
            constructor.createXContentRegistry();
            constructor.createSystemIndices();
            constructor.createFsHealthService();
            constructor.createInternalSnapshotsInfoService();
            constructor.createWriteLoadForecaster();
            constructor.createClusterModule();
            constructor.createIndicesModule();
            constructor.createCircuitBreakerService();
            constructor.createGatewayModule();
            constructor.createCompatibilityVersions();
            constructor.createPageCacheRecycler(serviceProvider);
            constructor.createBigArrays(serviceProvider);
            constructor.createMetaStateService();
            constructor.createPersistedClusterStateService();
            constructor.createRerouteService();
            constructor.createIndicesService();
            constructor.createIndexSettingsProviders();
            constructor.createShardLimitValidator();
            constructor.createMetadataCreateIndexService(forbidPrivateIndexSettings);
            constructor.createMetadataCreateDataStreamService();
            constructor.createMetadataDataStreamsService();
            constructor.createMetadataUpdateSettingsService();
            constructor.loadPluginComponents();
            constructor.createTerminationHandler();
            constructor.createActionModule();
            constructor.createNetworkModule();
            constructor.createMetadataUpgrader();
            constructor.createIndexMetadataVerifier();
            constructor.createTransport();
            constructor.createTransportService(serviceProvider);
            constructor.createGatewayMetaState();
            constructor.createResponseCollectorService();
            constructor.createSearchTransportService();
            constructor.createHttpServerTransport(serviceProvider);
            constructor.createIndexingLimits();
            constructor.createRecoverySettings();
            constructor.createRepositoriesService();
            constructor.createSnapshotsService();
            constructor.createSnapshotShardsService();
            constructor.createFileSettingsService();
            constructor.createRestoreService();
            constructor.createDiskThresholdMonitor();
            constructor.createDiscoveryModule();
            constructor.createNodeService();
            constructor.createSearchService(serviceProvider);
            constructor.createPersistentTasksService();
            constructor.createHealthNodeTaskExecutor();
            constructor.createPersistentTasksExecutorRegistry();
            constructor.createPersistentTasksClusterService();
            constructor.createPluginShutdownService();
            constructor.createRecoveryPlannerService();
            constructor.createDesiredNodesSettingsValidator();
            constructor.createMasterHistoryService();
            constructor.createCoordinationDiagnosticsService();
            constructor.createHealthObjects();
            constructor.createInjector(serviceProvider);
            constructor.setExistingShardsAllocator();
            constructor.setPluginLifecycleComponents();
            constructor.initializeClient();
            constructor.initializeHttpHandlers();
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

    /*
     * References needed during build only
     */
    private Settings environmentSettings;
    private Settings settings;
    private List<Setting<?>> additionalSettings;
    private ThreadPool threadPool;
    private ResourceWatcherService resourceWatcherService;
    private TelemetryProvider telemetryProvider;
    private Tracer tracer;
    private TaskManager taskManager;
    private ScriptService scriptService;
    private final ModulesBuilder modules = new ModulesBuilder();
    private AnalysisModule analysisModule;
    private SettingsModule settingsModule;
    private NetworkService networkService;
    private ClusterService clusterService;
    private InferenceServiceRegistry inferenceServiceRegistry;
    private IngestService ingestService;
    private final SetOnce<RepositoriesService> repositoriesServiceReference = new SetOnce<>();
    private ClusterInfoService clusterInfoService;
    private UsageService usageService;
    private SearchModule searchModule;
    private SystemIndices systemIndices;
    private FsHealthService fsHealthService;
    private final SetOnce<RerouteService> rerouteServiceReference = new SetOnce<>();
    private InternalSnapshotsInfoService snapshotsInfoService;
    private WriteLoadForecaster writeLoadForecaster;
    private ClusterModule clusterModule;
    private IndicesModule indicesModule;
    private CircuitBreakerService circuitBreakerService;
    private CompatibilityVersions compatibilityVersions;
    private PageCacheRecycler pageCacheRecycler;
    private BigArrays bigArrays;
    private MetaStateService metaStateService;
    private PersistedClusterStateService persistedClusterStateService;
    private IndicesService indicesService;
    private IndexSettingProviders indexSettingProviders;
    private ShardLimitValidator shardLimitValidator;
    private MetadataCreateIndexService metadataCreateIndexService;
    private MetadataCreateDataStreamService metadataCreateDataStreamService;
    private MetadataDataStreamsService metadataDataStreamsService;
    private MetadataUpdateSettingsService metadataUpdateSettingsService;
    private List<Object> pluginComponents;
    private ActionModule actionModule;
    private NetworkModule networkModule;
    private MetadataUpgrader metadataUpgrader;
    private IndexMetadataVerifier indexMetadataVerifier;
    private Transport transport;
    private TransportService transportService;
    private GatewayMetaState gatewayMetaState;
    private ResponseCollectorService responseCollectorService;
    private SearchTransportService searchTransportService;
    private HttpServerTransport httpServerTransport;
    private IndexingPressure indexingLimits;
    private RecoverySettings recoverySettings;
    private SnapshotsService snapshotsService;
    private SnapshotShardsService snapshotShardsService;
    private FileSettingsService fileSettingsService;
    private RestoreService restoreService;
    private DiscoveryModule discoveryModule;
    private SearchService searchService;
    private PersistentTasksService persistentTasksService;
    private HealthNodeTaskExecutor healthNodeTaskExecutor;
    private PersistentTasksExecutorRegistry persistentTasksExecutorRegistry;
    private PersistentTasksClusterService persistentTasksClusterService;
    private PluginShutdownService pluginShutdownService;
    private RecoveryPlannerService recoveryPlannerService;
    private DesiredNodesSettingsValidator desiredNodesSettingsValidator;
    private MasterHistoryService masterHistoryService;
    private CoordinationDiagnosticsService coordinationDiagnosticsService;
    private HealthService healthService;
    private HealthPeriodicLogger healthPeriodicLogger;
    private HealthMetadataService healthMetadataService;
    private LocalHealthMonitor localHealthMonitor;
    private HealthInfoCache nodeHealthOverview;
    private HealthApiStats healthApiStats;

    private NodeConstructor(List<Closeable> resourcesToClose) {
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

    private void checkEnvironment(Environment initialEnvironment) {
        // Pass the node settings to the DeprecationLogger class so that it can have the deprecation.skip_deprecated_settings setting:
        DeprecationLogger.initialize(initialEnvironment.settings());
        environmentSettings = Settings.builder()
            .put(initialEnvironment.settings())
            .put(Client.CLIENT_TYPE_SETTING_S.getKey(), "node")
            .build();

        final JvmInfo jvmInfo = JvmInfo.jvmInfo();
        logger.info(
            "version[{}], pid[{}], build[{}/{}/{}], OS[{}/{}/{}], JVM[{}/{}/{}/{}]",
            Build.current().qualifiedVersion(),
            jvmInfo.pid(),
            Build.current().type().displayName(),
            Build.current().hash(),
            Build.current().date(),
            Constants.OS_NAME,
            Constants.OS_VERSION,
            Constants.OS_ARCH,
            Constants.JVM_VENDOR,
            Constants.JVM_NAME,
            Constants.JAVA_VERSION,
            Constants.JVM_VERSION
        );
        logger.info("JVM home [{}], using bundled JDK [{}]", System.getProperty("java.home"), jvmInfo.getUsingBundledJdk());
        logger.info("JVM arguments {}", Arrays.toString(jvmInfo.getInputArguments()));
        if (Build.current().isProductionRelease() == false) {
            logger.warn(
                "version [{}] is a pre-release version of Elasticsearch and is not suitable for production",
                Build.current().qualifiedVersion()
            );
        }
        if (Environment.PATH_SHARED_DATA_SETTING.exists(environmentSettings)) {
            // NOTE: this must be done with an explicit check here because the deprecation property on a path setting will
            // cause ES to fail to start since logging is not yet initialized on first read of the setting
            deprecationLogger.warn(
                DeprecationCategory.SETTINGS,
                "shared-data-path",
                "setting [path.shared_data] is deprecated and will be removed in a future release"
            );
        }

        if (initialEnvironment.dataFiles().length > 1) {
            // NOTE: we use initialEnvironment here, but assertEquivalent below ensures the data paths do not change
            deprecationLogger.warn(
                DeprecationCategory.SETTINGS,
                "multiple-data-paths",
                "Configuring multiple [path.data] paths is deprecated. Use RAID or other system level features for utilizing "
                    + "multiple disks. This feature will be removed in a future release."
            );
        }
        if (Environment.dataPathUsesList(environmentSettings)) {
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
                initialEnvironment.configFile(),
                Arrays.toString(initialEnvironment.dataFiles()),
                initialEnvironment.logsFile(),
                initialEnvironment.pluginsFile()
            );
        }
    }

    private void createEnvironment(Environment initialEnvironment, Function<Settings, PluginsService> pluginServiceCtor) {
        pluginsService = pluginServiceCtor.apply(environmentSettings);
        settings = Node.mergePluginSettings(pluginsService.pluginMap(), environmentSettings);

        /*
         * Create the environment based on the finalized view of the settings. This is to ensure that components get the same setting
         * values, no matter they ask for them from.
         */
        this.environment = new Environment(settings, initialEnvironment.configFile());
        Environment.assertEquivalent(initialEnvironment, this.environment);
    }

    private void createThreadPools() {
        List<ExecutorBuilder<?>> executorBuilders = pluginsService.flatMap(p -> p.getExecutorBuilders(settings)).toList();

        threadPool = new ThreadPool(settings, executorBuilders.toArray(ExecutorBuilder<?>[]::new));
        resourcesToClose.add(() -> ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));
        resourceWatcherService = new ResourceWatcherService(settings, threadPool);
        resourcesToClose.add(resourceWatcherService);
        // adds the context to the DeprecationLogger so that it does not need to be injected everywhere
        HeaderWarning.setThreadContext(threadPool.getThreadContext());
        resourcesToClose.add(() -> HeaderWarning.removeThreadContext(threadPool.getThreadContext()));
    }

    private void createTracer() {
        telemetryProvider = getTelemetryProvider(pluginsService, settings);
        tracer = telemetryProvider.getTracer();
    }

    private void createTaskManager() {
        Set<String> taskHeaders = Stream.concat(
            pluginsService.filterPlugins(ActionPlugin.class).stream().flatMap(p -> p.getTaskHeaders().stream()),
            Task.HEADERS_TO_COPY.stream()
        ).collect(Collectors.toSet());

        taskManager = new TaskManager(settings, threadPool, taskHeaders, tracer);
    }

    private void loadAdditionalSettings() {
        // register the node.data, node.ingest, node.master, node.remote_cluster_client settings here so we can mark them private
        additionalSettings = new ArrayList<>(pluginsService.flatMap(Plugin::getSettings).toList());
        for (final ExecutorBuilder<?> builder : threadPool.builders()) {
            additionalSettings.addAll(builder.getRegisteredSettings());
        }
        SettingsExtension.load().forEach(e -> additionalSettings.addAll(e.getSettings()));
    }

    private static TelemetryProvider getTelemetryProvider(PluginsService pluginsService, Settings settings) {
        final List<TelemetryPlugin> telemetryPlugins = pluginsService.filterPlugins(TelemetryPlugin.class);

        if (telemetryPlugins.size() > 1) {
            throw new IllegalStateException("A single TelemetryPlugin was expected but got: " + telemetryPlugins);
        }

        return telemetryPlugins.isEmpty() ? TelemetryProvider.NOOP : telemetryPlugins.get(0).getTelemetryProvider(settings);
    }

    private void createClient() {
        client = new NodeClient(settings, threadPool);
    }

    private void createScriptService(NodeServiceProvider provider) {
        ScriptModule scriptModule = new ScriptModule(settings, pluginsService.filterPlugins(ScriptPlugin.class));
        scriptService = provider.newScriptService(
            pluginsService,
            settings,
            scriptModule.engines,
            scriptModule.contexts,
            threadPool::absoluteTimeInMillis
        );
    }

    private void createAnalysisModule() throws IOException {
        analysisModule = new AnalysisModule(
            this.environment,
            pluginsService.filterPlugins(AnalysisPlugin.class),
            pluginsService.getStablePluginRegistry()
        );
    }

    private void createSettingsModule() {
        settingsModule = new SettingsModule(settings, additionalSettings, pluginsService.flatMap(Plugin::getSettingsFilter).toList());
        modules.add(settingsModule);
    }

    private void createNodeEnvironment() throws IOException {
        nodeEnvironment = new NodeEnvironment(environmentSettings, environment);
        logger.info(
            "node name [{}], node ID [{}], cluster name [{}], roles {}",
            Node.NODE_NAME_SETTING.get(environmentSettings),
            nodeEnvironment.nodeId(),
            ClusterName.CLUSTER_NAME_SETTING.get(environmentSettings).value(),
            DiscoveryNode.getRolesFromSettings(settings)
                .stream()
                .map(DiscoveryNodeRole::roleName)
                .collect(Collectors.toCollection(LinkedHashSet::new))
        );
        resourcesToClose.add(nodeEnvironment);
    }

    private void createLocalNodeFactory() {
        localNodeFactory = new Node.LocalNodeFactory(settings, nodeEnvironment.nodeId());
    }

    private void createNetworkService() {
        networkService = new NetworkService(getCustomNameResolvers(pluginsService.filterPlugins(DiscoveryPlugin.class)));
    }

    /**
     * Get Custom Name Resolvers list based on a Discovery Plugins list
     *
     * @param discoveryPlugins Discovery plugins list
     */
    private List<NetworkService.CustomNameResolver> getCustomNameResolvers(List<DiscoveryPlugin> discoveryPlugins) {
        List<NetworkService.CustomNameResolver> customNameResolvers = new ArrayList<>();
        for (DiscoveryPlugin discoveryPlugin : discoveryPlugins) {
            NetworkService.CustomNameResolver customNameResolver = discoveryPlugin.getCustomNameResolver(environment.settings());
            if (customNameResolver != null) {
                customNameResolvers.add(customNameResolver);
            }
        }
        return customNameResolvers;
    }

    private void createClusterService() {
        clusterService = new ClusterService(settings, settingsModule.getClusterSettings(), threadPool, taskManager);
        clusterService.addStateApplier(scriptService);
        resourcesToClose.add(clusterService);

        Set<Setting<?>> consistentSettings = settingsModule.getConsistentSettings();
        if (consistentSettings.isEmpty() == false) {
            clusterService.addLocalNodeMasterListener(
                new ConsistentSettingsService(settings, clusterService, consistentSettings).newHashPublisher()
            );
        }

        if (DiscoveryNode.isMasterNode(settings)) {
            clusterService.addListener(new TransportVersionsFixupListener(clusterService, client.admin().cluster(), threadPool));
        }
    }

    private void createInferenceServiceRegistry() {
        var factoryContext = new InferenceServicePlugin.InferenceServiceFactoryContext(client);
        inferenceServiceRegistry = new InferenceServiceRegistry(pluginsService.filterPlugins(InferenceServicePlugin.class), factoryContext);
    }

    private void createIngestService() {
        ingestService = new IngestService(
            clusterService,
            threadPool,
            this.environment,
            scriptService,
            analysisModule.getAnalysisRegistry(),
            pluginsService.filterPlugins(IngestPlugin.class),
            client,
            IngestService.createGrokThreadWatchdog(this.environment, threadPool),
            getDocumentParsingObserverSupplier()
        );
    }

    private Supplier<DocumentParsingObserver> getDocumentParsingObserverSupplier() {
        List<DocumentParsingObserverPlugin> plugins = pluginsService.filterPlugins(DocumentParsingObserverPlugin.class);
        if (plugins.size() == 1) {
            return plugins.get(0).getDocumentParsingObserverSupplier();
        } else if (plugins.isEmpty()) {
            return () -> DocumentParsingObserver.EMPTY_INSTANCE;
        }
        throw new IllegalStateException("too many DocumentParsingObserverPlugin instances");
    }

    private void createClusterInfoService(NodeServiceProvider provider) {
        clusterInfoService = provider.newClusterInfoService(pluginsService, settings, clusterService, threadPool, client);
    }

    private void createUsageService() {
        usageService = new UsageService();
    }

    private void createSearchModule() {
        searchModule = new SearchModule(settings, pluginsService.filterPlugins(SearchPlugin.class));
        IndexSearcher.setMaxClauseCount(SearchUtils.calculateMaxClauseValue(threadPool));
    }

    private void createNamedWriteableRegistry() {
        namedWriteableRegistry = new NamedWriteableRegistry(
            Stream.of(
                NetworkModule.getNamedWriteables().stream(),
                IndicesModule.getNamedWriteables().stream(),
                searchModule.getNamedWriteables().stream(),
                pluginsService.flatMap(Plugin::getNamedWriteables),
                ClusterModule.getNamedWriteables().stream(),
                SystemIndexMigrationExecutor.getNamedWriteables().stream(),
                inferenceServiceRegistry.getNamedWriteables().stream()
            ).flatMap(Function.identity()).toList()
        );
    }

    private void createXContentRegistry() {
        xContentRegistry = new NamedXContentRegistry(
            Stream.of(
                NetworkModule.getNamedXContents().stream(),
                IndicesModule.getNamedXContents().stream(),
                searchModule.getNamedXContents().stream(),
                pluginsService.flatMap(Plugin::getNamedXContent),
                ClusterModule.getNamedXWriteables().stream(),
                SystemIndexMigrationExecutor.getNamedXContentParsers().stream(),
                HealthNodeTaskExecutor.getNamedXContentParsers().stream()
            ).flatMap(Function.identity()).collect(toList())
        );
    }

    private void createSystemIndices() {
        List<SystemIndices.Feature> features = pluginsService.filterPlugins(SystemIndexPlugin.class).stream().map(plugin -> {
            SystemIndices.validateFeatureName(plugin.getFeatureName(), plugin.getClass().getCanonicalName());
            return SystemIndices.Feature.fromSystemIndexPlugin(plugin, settings);
        }).toList();
        systemIndices = new SystemIndices(features);

        if (DiscoveryNode.isMasterNode(settings)) {
            clusterService.addListener(new SystemIndexMappingUpdateService(systemIndices, client));
        }
    }

    private void createFsHealthService() {
        fsHealthService = new FsHealthService(settings, clusterService.getClusterSettings(), threadPool, nodeEnvironment);
    }

    private void createInternalSnapshotsInfoService() {
        snapshotsInfoService = new InternalSnapshotsInfoService(
            settings,
            clusterService,
            repositoriesServiceReference::get,
            rerouteServiceReference::get
        );
    }

    private void createWriteLoadForecaster() {
        List<ClusterPlugin> clusterPlugins = pluginsService.filterPlugins(ClusterPlugin.class);
        List<WriteLoadForecaster> writeLoadForecasters = clusterPlugins.stream()
            .flatMap(
                clusterPlugin -> clusterPlugin.createWriteLoadForecasters(threadPool, settings, clusterService.getClusterSettings())
                    .stream()
            )
            .toList();

        writeLoadForecaster = switch (writeLoadForecasters.size()) {
            case 1 -> writeLoadForecasters.get(0);
            case 0 -> WriteLoadForecaster.DEFAULT;
            default -> throw new IllegalStateException("A single WriteLoadForecaster was expected but got: " + writeLoadForecasters);
        };
    }

    private void createClusterModule() {
        clusterModule = new ClusterModule(
            settings,
            clusterService,
            pluginsService.filterPlugins(ClusterPlugin.class),
            clusterInfoService,
            snapshotsInfoService,
            threadPool,
            systemIndices,
            writeLoadForecaster
        );
        modules.add(clusterModule);
    }

    private void createIndicesModule() {
        indicesModule = new IndicesModule(pluginsService.filterPlugins(MapperPlugin.class));
        modules.add(indicesModule);
    }

    private void createCircuitBreakerService() {
        List<BreakerSettings> pluginCircuitBreakers = pluginsService.filterPlugins(CircuitBreakerPlugin.class)
            .stream()
            .map(plugin -> plugin.getCircuitBreaker(settings))
            .toList();

        circuitBreakerService = createCircuitBreakerService(
            settingsModule.getSettings(),
            pluginCircuitBreakers,
            settingsModule.getClusterSettings()
        );

        pluginsService.filterPlugins(CircuitBreakerPlugin.class).forEach(plugin -> {
            CircuitBreaker breaker = circuitBreakerService.getBreaker(plugin.getCircuitBreaker(settings).getName());
            plugin.setCircuitBreaker(breaker);
        });
        resourcesToClose.add(circuitBreakerService);
    }

    /**
     * Creates a new {@link CircuitBreakerService} based on the settings provided.
     *
     * @see Node#BREAKER_TYPE_KEY
     */
    private static CircuitBreakerService createCircuitBreakerService(
        Settings settings,
        List<BreakerSettings> breakerSettings,
        ClusterSettings clusterSettings
    ) {
        String type = Node.BREAKER_TYPE_KEY.get(settings);
        if (type.equals("hierarchy")) {
            return new HierarchyCircuitBreakerService(settings, breakerSettings, clusterSettings);
        } else if (type.equals("none")) {
            return new NoneCircuitBreakerService();
        } else {
            throw new IllegalArgumentException("Unknown circuit breaker type [" + type + "]");
        }
    }

    private void createGatewayModule() {
        modules.add(new GatewayModule());
    }

    private void createCompatibilityVersions() {
        compatibilityVersions = new CompatibilityVersions(TransportVersion.current(), systemIndices.getMappingsVersions());
    }

    private void createPageCacheRecycler(NodeServiceProvider provider) {
        pageCacheRecycler = provider.newPageCacheRecycler(pluginsService, settings);
    }

    private void createBigArrays(NodeServiceProvider provider) {
        bigArrays = provider.newBigArrays(pluginsService, pageCacheRecycler, circuitBreakerService);
    }

    private void createMetaStateService() {
        metaStateService = new MetaStateService(nodeEnvironment, xContentRegistry);
        ;
    }

    private void createPersistedClusterStateService() {
        List<ClusterCoordinationPlugin.PersistedClusterStateServiceFactory> persistedClusterStateServiceFactories = pluginsService
            .filterPlugins(ClusterCoordinationPlugin.class)
            .stream()
            .map(ClusterCoordinationPlugin::getPersistedClusterStateServiceFactory)
            .flatMap(Optional::stream)
            .toList();

        if (persistedClusterStateServiceFactories.size() > 1) {
            throw new IllegalStateException("multiple persisted-state-service factories found: " + persistedClusterStateServiceFactories);
        }

        if (persistedClusterStateServiceFactories.size() == 1) {
            persistedClusterStateService = persistedClusterStateServiceFactories.get(0)
                .newPersistedClusterStateService(
                    nodeEnvironment,
                    xContentRegistry,
                    clusterService.getClusterSettings(),
                    threadPool,
                    compatibilityVersions
                );
        } else {
            persistedClusterStateService = new PersistedClusterStateService(
                nodeEnvironment,
                xContentRegistry,
                clusterService.getClusterSettings(),
                threadPool::relativeTimeInMillis
            );
        }
    }

    private void createRerouteService() {
        RerouteService rerouteService = new BatchedRerouteService(clusterService, clusterModule.getAllocationService()::reroute);
        rerouteServiceReference.set(rerouteService);
        clusterService.setRerouteService(rerouteService);
    }

    private void createIndicesService() {
        // collect engine factory providers from plugins
        Collection<EnginePlugin> enginePlugins = pluginsService.filterPlugins(EnginePlugin.class);
        Collection<Function<IndexSettings, Optional<EngineFactory>>> engineFactoryProviders = enginePlugins.stream()
            .map(plugin -> (Function<IndexSettings, Optional<EngineFactory>>) plugin::getEngineFactory)
            .toList();

        Map<String, IndexStorePlugin.DirectoryFactory> indexStoreFactories = pluginsService.filterPlugins(IndexStorePlugin.class)
            .stream()
            .map(IndexStorePlugin::getDirectoryFactories)
            .flatMap(m -> m.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        Map<String, IndexStorePlugin.RecoveryStateFactory> recoveryStateFactories = pluginsService.filterPlugins(IndexStorePlugin.class)
            .stream()
            .map(IndexStorePlugin::getRecoveryStateFactories)
            .flatMap(m -> m.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        List<IndexStorePlugin.IndexFoldersDeletionListener> indexFoldersDeletionListeners = pluginsService.filterPlugins(
            IndexStorePlugin.class
        ).stream().map(IndexStorePlugin::getIndexFoldersDeletionListeners).flatMap(List::stream).toList();

        Map<String, IndexStorePlugin.SnapshotCommitSupplier> snapshotCommitSuppliers = pluginsService.filterPlugins(IndexStorePlugin.class)
            .stream()
            .map(IndexStorePlugin::getSnapshotCommitSuppliers)
            .flatMap(m -> m.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        indicesService = new IndicesService(
            settings,
            pluginsService,
            nodeEnvironment,
            xContentRegistry,
            analysisModule.getAnalysisRegistry(),
            clusterModule.getIndexNameExpressionResolver(),
            indicesModule.getMapperRegistry(),
            namedWriteableRegistry,
            threadPool,
            settingsModule.getIndexScopedSettings(),
            circuitBreakerService,
            bigArrays,
            scriptService,
            clusterService,
            client,
            metaStateService,
            engineFactoryProviders,
            indexStoreFactories,
            searchModule.getValuesSourceRegistry(),
            recoveryStateFactories,
            indexFoldersDeletionListeners,
            snapshotCommitSuppliers,
            searchModule.getRequestCacheKeyDifferentiator(),
            getDocumentParsingObserverSupplier()
        );
    }

    private void createIndexSettingsProviders() {
        var parameters = new IndexSettingProvider.Parameters(indicesService::createIndexMapperServiceForValidation);
        indexSettingProviders = new IndexSettingProviders(
            pluginsService.flatMap(p -> p.getAdditionalIndexSettingProviders(parameters)).collect(Collectors.toSet())
        );
    }

    private void createShardLimitValidator() {
        shardLimitValidator = new ShardLimitValidator(settings, clusterService);
        ;
    }

    private void createMetadataCreateIndexService(boolean forbidPrivateIndexSettings) {
        metadataCreateIndexService = new MetadataCreateIndexService(
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
    }

    private void createMetadataCreateDataStreamService() {
        metadataCreateDataStreamService = new MetadataCreateDataStreamService(threadPool, clusterService, metadataCreateIndexService);
    }

    private void createMetadataDataStreamsService() {
        metadataDataStreamsService = new MetadataDataStreamsService(clusterService, indicesService);
    }

    private void createMetadataUpdateSettingsService() {
        metadataUpdateSettingsService = new MetadataUpdateSettingsService(
            clusterService,
            clusterModule.getAllocationService(),
            settingsModule.getIndexScopedSettings(),
            indicesService,
            shardLimitValidator,
            threadPool
        );
    }

    private void loadPluginComponents() {
        pluginComponents = pluginsService.flatMap(
            p -> p.createComponents(
                client,
                clusterService,
                threadPool,
                resourceWatcherService,
                scriptService,
                xContentRegistry,
                environment,
                nodeEnvironment,
                namedWriteableRegistry,
                clusterModule.getIndexNameExpressionResolver(),
                repositoriesServiceReference::get,
                telemetryProvider,
                clusterModule.getAllocationService(),
                indicesService
            )
        ).toList();

        List<ReloadablePlugin> reloadablePlugins = pluginsService.filterPlugins(ReloadablePlugin.class);
        pluginsService.filterPlugins(ReloadAwarePlugin.class).forEach(p -> p.setReloadCallback(wrapPlugins(reloadablePlugins)));
    }

    /**
     * Wrap a group of reloadable plugins into a single reloadable plugin interface
     * @param reloadablePlugins A list of reloadable plugins
     * @return A single ReloadablePlugin that, upon reload, reloads the plugins it wraps
     */
    private static ReloadablePlugin wrapPlugins(List<ReloadablePlugin> reloadablePlugins) {
        return settings -> {
            for (ReloadablePlugin plugin : reloadablePlugins) {
                try {
                    plugin.reload(settings);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    private void createTerminationHandler() {
        List<TerminationHandler> terminationHandlers = pluginsService.loadServiceProviders(TerminationHandlerProvider.class)
            .stream()
            .map(TerminationHandlerProvider::handler)
            .toList();
        if (terminationHandlers.size() == 1) {
            terminationHandler = terminationHandlers.get(0);
        } else if (terminationHandlers.size() > 1) {
            throw new IllegalStateException(
                Strings.format(
                    "expected at most one termination handler, but found %s: [%s]",
                    terminationHandlers.size(),
                    terminationHandlers.stream().map(it -> it.getClass().getCanonicalName())
                )
            );
        }
    }

    private void createActionModule() {
        List<ReservedClusterStateHandler<?>> reservedStateHandlers = new ArrayList<>();

        // add all reserved state handlers from server
        reservedStateHandlers.add(new ReservedClusterSettingsAction(settingsModule.getClusterSettings()));

        var templateService = new MetadataIndexTemplateService(
            clusterService,
            metadataCreateIndexService,
            indicesService,
            settingsModule.getIndexScopedSettings(),
            xContentRegistry,
            systemIndices,
            indexSettingProviders
        );

        reservedStateHandlers.add(new ReservedComposableIndexTemplateAction(templateService, settingsModule.getIndexScopedSettings()));

        // add all reserved state handlers from plugins
        List<? extends ReservedClusterStateHandlerProvider> pluginHandlers = pluginsService.loadServiceProviders(
            ReservedClusterStateHandlerProvider.class
        );
        pluginHandlers.forEach(h -> reservedStateHandlers.addAll(h.handlers()));

        actionModule = new ActionModule(
            settings,
            clusterModule.getIndexNameExpressionResolver(),
            settingsModule.getIndexScopedSettings(),
            settingsModule.getClusterSettings(),
            settingsModule.getSettingsFilter(),
            threadPool,
            pluginsService.filterPlugins(ActionPlugin.class),
            client,
            circuitBreakerService,
            usageService,
            systemIndices,
            tracer,
            clusterService,
            reservedStateHandlers,
            pluginsService.loadSingletonServiceProvider(RestExtension.class, RestExtension::allowAll)
        );
        modules.add(actionModule);
    }

    private void createNetworkModule() {
        RestController restController = actionModule.getRestController();
        networkModule = new NetworkModule(
            settings,
            pluginsService.filterPlugins(NetworkPlugin.class),
            threadPool,
            bigArrays,
            pageCacheRecycler,
            circuitBreakerService,
            namedWriteableRegistry,
            xContentRegistry,
            networkService,
            restController,
            actionModule::copyRequestHeadersToThreadContext,
            clusterService.getClusterSettings(),
            tracer
        );
    }

    private void createMetadataUpgrader() {
        Collection<UnaryOperator<Map<String, IndexTemplateMetadata>>> indexTemplateMetadataUpgraders = pluginsService.map(
            Plugin::getIndexTemplateMetadataUpgrader
        ).toList();
        metadataUpgrader = new MetadataUpgrader(indexTemplateMetadataUpgraders);

        // this is added as a cluster listener within the constructor
        new TemplateUpgradeService(client, clusterService, threadPool, indexTemplateMetadataUpgraders);
    }

    private void createIndexMetadataVerifier() {
        indexMetadataVerifier = new IndexMetadataVerifier(
            settings,
            clusterService,
            xContentRegistry,
            indicesModule.getMapperRegistry(),
            settingsModule.getIndexScopedSettings(),
            scriptService
        );
        if (DiscoveryNode.isMasterNode(settings)) {
            clusterService.addListener(new SystemIndexMetadataUpgradeService(systemIndices, clusterService));
        }
    }

    private void createTransport() {
        transport = networkModule.getTransportSupplier().get();
    }

    private void createTransportService(NodeServiceProvider provider) {
        transportService = provider.newTransportService(
            pluginsService,
            settings,
            transport,
            threadPool,
            networkModule.getTransportInterceptor(),
            localNodeFactory,
            settingsModule.getClusterSettings(),
            taskManager,
            tracer
        );
    }

    private void createGatewayMetaState() {
        gatewayMetaState = new GatewayMetaState();
    }

    private void createResponseCollectorService() {
        responseCollectorService = new ResponseCollectorService(clusterService);
    }

    private void createSearchTransportService() {
        searchTransportService = new SearchTransportService(
            transportService,
            client,
            SearchExecutionStatsCollector.makeWrapper(responseCollectorService)
        );
    }

    private void createHttpServerTransport(NodeServiceProvider provider) {
        httpServerTransport = provider.newHttpTransport(pluginsService, networkModule);
    }

    private void createIndexingLimits() {
        indexingLimits = new IndexingPressure(settings);
    }

    private void createRecoverySettings() {
        recoverySettings = new RecoverySettings(settings, settingsModule.getClusterSettings());
    }

    private void createRepositoriesService() {
        RepositoriesModule repositoriesModule = new RepositoriesModule(
            this.environment,
            pluginsService.filterPlugins(RepositoryPlugin.class),
            transportService,
            clusterService,
            bigArrays,
            xContentRegistry,
            recoverySettings,
            telemetryProvider
        );
        repositoriesServiceReference.set(repositoriesModule.getRepositoryService());

        actionModule.getReservedClusterStateService()
            .installStateHandler(new ReservedRepositoryAction(repositoriesModule.getRepositoryService()));
    }

    private void createSnapshotsService() {
        snapshotsService = new SnapshotsService(
            settings,
            clusterService,
            clusterModule.getIndexNameExpressionResolver(),
            repositoriesServiceReference.get(),
            transportService,
            actionModule.getActionFilters(),
            systemIndices
        );
    }

    private void createSnapshotShardsService() {
        snapshotShardsService = new SnapshotShardsService(
            settings,
            clusterService,
            repositoriesServiceReference.get(),
            transportService,
            indicesService
        );
    }

    private void createFileSettingsService() {
        fileSettingsService = new FileSettingsService(clusterService, actionModule.getReservedClusterStateService(), environment);

        actionModule.getReservedClusterStateService().installStateHandler(new ReservedPipelineAction());
    }

    private void createRestoreService() {
        restoreService = new RestoreService(
            clusterService,
            repositoriesServiceReference.get(),
            clusterModule.getAllocationService(),
            metadataCreateIndexService,
            indexMetadataVerifier,
            shardLimitValidator,
            systemIndices,
            indicesService,
            fileSettingsService,
            threadPool
        );
    }

    private void createDiskThresholdMonitor() {
        DiskThresholdMonitor diskThresholdMonitor = new DiskThresholdMonitor(
            settings,
            clusterService::state,
            clusterService.getClusterSettings(),
            client,
            threadPool::relativeTimeInMillis,
            rerouteServiceReference.get()
        );
        clusterInfoService.addListener(diskThresholdMonitor::onNewInfo);
    }

    private void createDiscoveryModule() {
        discoveryModule = new DiscoveryModule(
            settings,
            transportService,
            client,
            namedWriteableRegistry,
            networkService,
            clusterService.getMasterService(),
            clusterService.getClusterApplierService(),
            clusterService.getClusterSettings(),
            pluginsService.filterPlugins(DiscoveryPlugin.class),
            pluginsService.filterPlugins(ClusterCoordinationPlugin.class),
            clusterModule.getAllocationService(),
            environment.configFile(),
            gatewayMetaState,
            rerouteServiceReference.get(),
            fsHealthService,
            circuitBreakerService,
            compatibilityVersions
        );
    }

    private void createNodeService() throws IOException {
        MonitorService monitorService = new MonitorService(settings, nodeEnvironment, threadPool);
        nodeService = new NodeService(
            settings,
            threadPool,
            monitorService,
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
            repositoriesServiceReference.get()
        );
    }

    private void createSearchService(NodeServiceProvider provider) {
        searchService = provider.newSearchService(
            pluginsService,
            clusterService,
            indicesService,
            threadPool,
            scriptService,
            bigArrays,
            searchModule.getFetchPhase(),
            responseCollectorService,
            circuitBreakerService,
            systemIndices.getExecutorSelector(),
            tracer
        );
    }

    private void createPersistentTasksService() {
        persistentTasksService = new PersistentTasksService(clusterService, threadPool, client);
    }

    private void createHealthNodeTaskExecutor() {
        healthNodeTaskExecutor = HealthNodeTaskExecutor.create(
            clusterService,
            persistentTasksService,
            settings,
            clusterService.getClusterSettings()
        );
    }

    private void createPersistentTasksExecutorRegistry() {
        SystemIndexMigrationExecutor systemIndexMigrationExecutor = new SystemIndexMigrationExecutor(
            client,
            clusterService,
            systemIndices,
            metadataUpdateSettingsService,
            metadataCreateIndexService,
            settingsModule.getIndexScopedSettings()
        );

        List<PersistentTasksExecutor<?>> builtinTaskExecutors = List.of(systemIndexMigrationExecutor, healthNodeTaskExecutor);
        List<PersistentTasksExecutor<?>> pluginTaskExecutors = pluginsService.filterPlugins(PersistentTaskPlugin.class)
            .stream()
            .map(
                p -> p.getPersistentTasksExecutor(
                    clusterService,
                    threadPool,
                    client,
                    settingsModule,
                    clusterModule.getIndexNameExpressionResolver()
                )
            )
            .flatMap(List::stream)
            .collect(toList());
        persistentTasksExecutorRegistry = new PersistentTasksExecutorRegistry(concatLists(pluginTaskExecutors, builtinTaskExecutors));
    }

    private void createPersistentTasksClusterService() {
        persistentTasksClusterService = new PersistentTasksClusterService(
            settings,
            persistentTasksExecutorRegistry,
            clusterService,
            threadPool
        );
        resourcesToClose.add(persistentTasksClusterService);
    }

    private void createPluginShutdownService() {
        List<ShutdownAwarePlugin> shutdownAwarePlugins = pluginsService.filterPlugins(ShutdownAwarePlugin.class);
        pluginShutdownService = new PluginShutdownService(shutdownAwarePlugins);
        clusterService.addListener(pluginShutdownService);
    }

    private void createRecoveryPlannerService() {
        List<RecoveryPlannerService> recoveryPlannerServices = pluginsService.filterPlugins(RecoveryPlannerPlugin.class)
            .stream()
            .map(
                plugin -> plugin.createRecoveryPlannerService(
                    new ShardSnapshotsService(client, repositoriesServiceReference.get(), threadPool, clusterService)
                )
            )
            .filter(Optional::isPresent)
            .map(Optional::get)
            .toList();

        recoveryPlannerService = switch (recoveryPlannerServices.size()) {
            case 1 -> recoveryPlannerServices.get(0);
            case 0 -> new PeerOnlyRecoveryPlannerService();
            default -> throw new IllegalStateException(
                "Expected a single RecoveryPlannerService but got: " + recoveryPlannerServices.size()
            );
        };
    }

    private void createDesiredNodesSettingsValidator() {
        desiredNodesSettingsValidator = new DesiredNodesSettingsValidator();
    }

    private void createMasterHistoryService() {
        masterHistoryService = new MasterHistoryService(transportService, threadPool, clusterService);
        ;
    }

    private void createCoordinationDiagnosticsService() {
        coordinationDiagnosticsService = new CoordinationDiagnosticsService(
            clusterService,
            transportService,
            discoveryModule.getCoordinator(),
            masterHistoryService
        );
    }

    private void createHealthObjects() {
        var serverHealthIndicatorServices = List.of(
            new StableMasterHealthIndicatorService(coordinationDiagnosticsService, clusterService),
            new RepositoryIntegrityHealthIndicatorService(clusterService),
            new ShardsAvailabilityHealthIndicatorService(clusterService, clusterModule.getAllocationService(), systemIndices),
            new DiskHealthIndicatorService(clusterService),
            new ShardsCapacityHealthIndicatorService(clusterService)
        );
        var pluginHealthIndicatorServices = pluginsService.filterPlugins(HealthPlugin.class)
            .stream()
            .flatMap(plugin -> plugin.getHealthIndicatorServices().stream())
            .toList();
        healthService = new HealthService(concatLists(serverHealthIndicatorServices, pluginHealthIndicatorServices), threadPool);

        healthPeriodicLogger = new HealthPeriodicLogger(settings, clusterService, client, healthService);
        healthPeriodicLogger.init();
        healthMetadataService = HealthMetadataService.create(clusterService, settings);
        localHealthMonitor = LocalHealthMonitor.create(settings, clusterService, nodeService, threadPool, client);
        nodeHealthOverview = HealthInfoCache.create(clusterService);
        healthApiStats = new HealthApiStats();
    }

    private void createInjector(NodeServiceProvider provider) {
        modules.add(b -> {
            b.bind(NodeService.class).toInstance(nodeService);
            b.bind(NamedXContentRegistry.class).toInstance(xContentRegistry);
            b.bind(PluginsService.class).toInstance(pluginsService);
            b.bind(Client.class).toInstance(client);
            b.bind(NodeClient.class).toInstance(client);
            b.bind(Environment.class).toInstance(this.environment);
            b.bind(ThreadPool.class).toInstance(threadPool);
            b.bind(NodeEnvironment.class).toInstance(nodeEnvironment);
            b.bind(ResourceWatcherService.class).toInstance(resourceWatcherService);
            b.bind(CircuitBreakerService.class).toInstance(circuitBreakerService);
            b.bind(BigArrays.class).toInstance(bigArrays);
            b.bind(PageCacheRecycler.class).toInstance(pageCacheRecycler);
            b.bind(ScriptService.class).toInstance(scriptService);
            b.bind(AnalysisRegistry.class).toInstance(analysisModule.getAnalysisRegistry());
            b.bind(IngestService.class).toInstance(ingestService);
            b.bind(IndexingPressure.class).toInstance(indexingLimits);
            b.bind(UsageService.class).toInstance(usageService);
            b.bind(AggregationUsageService.class).toInstance(searchModule.getValuesSourceRegistry().getUsageService());
            b.bind(NamedWriteableRegistry.class).toInstance(namedWriteableRegistry);
            b.bind(MetadataUpgrader.class).toInstance(metadataUpgrader);
            b.bind(MetaStateService.class).toInstance(metaStateService);
            b.bind(PersistedClusterStateService.class).toInstance(persistedClusterStateService);
            b.bind(IndicesService.class).toInstance(indicesService);
            b.bind(MetadataCreateIndexService.class).toInstance(metadataCreateIndexService);
            b.bind(MetadataCreateDataStreamService.class).toInstance(metadataCreateDataStreamService);
            b.bind(MetadataDataStreamsService.class).toInstance(metadataDataStreamsService);
            b.bind(MetadataUpdateSettingsService.class).toInstance(metadataUpdateSettingsService);
            b.bind(SearchService.class).toInstance(searchService);
            b.bind(SearchTransportService.class).toInstance(searchTransportService);
            b.bind(SearchPhaseController.class).toInstance(new SearchPhaseController(searchService::aggReduceContextBuilder));
            b.bind(Transport.class).toInstance(transport);
            b.bind(TransportService.class).toInstance(transportService);
            b.bind(NetworkService.class).toInstance(networkService);
            b.bind(UpdateHelper.class).toInstance(new UpdateHelper(scriptService));
            b.bind(IndexMetadataVerifier.class).toInstance(indexMetadataVerifier);
            b.bind(ClusterInfoService.class).toInstance(clusterInfoService);
            b.bind(SnapshotsInfoService.class).toInstance(snapshotsInfoService);
            b.bind(GatewayMetaState.class).toInstance(gatewayMetaState);
            b.bind(Coordinator.class).toInstance(discoveryModule.getCoordinator());
            b.bind(Reconfigurator.class).toInstance(discoveryModule.getReconfigurator());
            {
                provider.processRecoverySettings(pluginsService, settingsModule.getClusterSettings(), recoverySettings);
                SnapshotFilesProvider snapshotFilesProvider = new SnapshotFilesProvider(repositoriesServiceReference.get());
                b.bind(PeerRecoverySourceService.class)
                    .toInstance(
                        new PeerRecoverySourceService(
                            transportService,
                            indicesService,
                            clusterService,
                            recoverySettings,
                            recoveryPlannerService
                        )
                    );
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
            }
            b.bind(HttpServerTransport.class).toInstance(httpServerTransport);
            pluginComponents.forEach(p -> {
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
            b.bind(PersistentTasksService.class).toInstance(persistentTasksService);
            b.bind(PersistentTasksClusterService.class).toInstance(persistentTasksClusterService);
            b.bind(PersistentTasksExecutorRegistry.class).toInstance(persistentTasksExecutorRegistry);
            b.bind(RepositoriesService.class).toInstance(repositoriesServiceReference.get());
            b.bind(SnapshotsService.class).toInstance(snapshotsService);
            b.bind(SnapshotShardsService.class).toInstance(snapshotShardsService);
            b.bind(RestoreService.class).toInstance(restoreService);
            b.bind(RerouteService.class).toInstance(rerouteServiceReference.get());
            b.bind(ShardLimitValidator.class).toInstance(shardLimitValidator);
            b.bind(FsHealthService.class).toInstance(fsHealthService);
            b.bind(SystemIndices.class).toInstance(systemIndices);
            b.bind(PluginShutdownService.class).toInstance(pluginShutdownService);
            b.bind(ExecutorSelector.class).toInstance(systemIndices.getExecutorSelector());
            b.bind(IndexSettingProviders.class).toInstance(indexSettingProviders);
            b.bind(DesiredNodesSettingsValidator.class).toInstance(desiredNodesSettingsValidator);
            b.bind(HealthService.class).toInstance(healthService);
            b.bind(MasterHistoryService.class).toInstance(masterHistoryService);
            b.bind(CoordinationDiagnosticsService.class).toInstance(coordinationDiagnosticsService);
            b.bind(HealthNodeTaskExecutor.class).toInstance(healthNodeTaskExecutor);
            b.bind(HealthMetadataService.class).toInstance(healthMetadataService);
            b.bind(LocalHealthMonitor.class).toInstance(localHealthMonitor);
            b.bind(HealthInfoCache.class).toInstance(nodeHealthOverview);
            b.bind(HealthApiStats.class).toInstance(healthApiStats);
            b.bind(Tracer.class).toInstance(tracer);
            b.bind(FileSettingsService.class).toInstance(fileSettingsService);
            b.bind(WriteLoadForecaster.class).toInstance(writeLoadForecaster);
            b.bind(HealthPeriodicLogger.class).toInstance(healthPeriodicLogger);
            b.bind(CompatibilityVersions.class).toInstance(compatibilityVersions);
            b.bind(InferenceServiceRegistry.class).toInstance(inferenceServiceRegistry);
        });

        if (ReadinessService.enabled(environment)) {
            modules.add(
                b -> b.bind(ReadinessService.class).toInstance(provider.newReadinessService(pluginsService, clusterService, environment))
            );
        }

        injector = modules.createInjector();

        resourcesToClose.add(injector.getInstance(PeerRecoverySourceService.class));
    }

    private void setExistingShardsAllocator() {
        // We allocate copies of existing shards by looking for a viable copy of the shard in the cluster and assigning the shard there.
        // The search for viable copies is triggered by an allocation attempt (i.e. a reroute) and is performed asynchronously. When it
        // completes we trigger another reroute to try the allocation again. This means there is a circular dependency: the allocation
        // service needs access to the existing shards allocators (e.g. the GatewayAllocator) which need to be able to trigger a
        // reroute, which needs to call into the allocation service. We close the loop here:
        clusterModule.setExistingShardsAllocators(injector.getInstance(GatewayAllocator.class));
    }

    private void setPluginLifecycleComponents() {
        List<LifecycleComponent> pluginLifecycleComponents = pluginComponents.stream().map(p -> {
            if (p instanceof PluginComponentBinding<?, ?> pcb) {
                return pcb.impl();
            }
            return p;
        }).filter(p -> p instanceof LifecycleComponent).map(p -> (LifecycleComponent) p).toList();
        resourcesToClose.addAll(pluginLifecycleComponents);
        this.pluginLifecycleComponents = pluginLifecycleComponents;
    }

    private void initializeClient() {
        // Due to Java's type erasure with generics, the injector can't give us exactly what we need, and we have
        // to resort to some evil casting.
        @SuppressWarnings("rawtypes")
        Map<ActionType<? extends ActionResponse>, TransportAction<? extends ActionRequest, ? extends ActionResponse>> actions =
            forciblyCast(injector.getInstance(new Key<Map<ActionType, TransportAction>>() {
            }));

        client.initialize(
            actions,
            transportService.getTaskManager(),
            () -> clusterService.localNode().getId(),
            transportService.getLocalNodeConnection(),
            transportService.getRemoteClusterService(),
            namedWriteableRegistry
        );
    }

    private void initializeHttpHandlers() {
        logger.debug("initializing HTTP handlers ...");
        actionModule.initRestHandlers(() -> clusterService.state().nodesIfRecovered());
        logger.info("initialized");
    }
}
