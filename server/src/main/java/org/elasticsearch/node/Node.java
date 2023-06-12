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
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.Version;
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
import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.coordination.CoordinationDiagnosticsService;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.MasterHistoryService;
import org.elasticsearch.cluster.coordination.StableMasterHealthIndicatorService;
import org.elasticsearch.cluster.desirednodes.DesiredNodesSettingsValidator;
import org.elasticsearch.cluster.metadata.IndexMetadataVerifier;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
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
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.logging.NodeAndClusterIdStateListener;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.ConsistentSettingsService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.SettingUpgrader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.NodeMetadata;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.gateway.GatewayMetaState;
import org.elasticsearch.gateway.GatewayModule;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.gateway.MetaStateService;
import org.elasticsearch.gateway.PersistedClusterStateService;
import org.elasticsearch.health.HealthIndicatorService;
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
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.recovery.PeerRecoverySourceService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.recovery.SnapshotFilesProvider;
import org.elasticsearch.indices.recovery.plan.PeerOnlyRecoveryPlannerService;
import org.elasticsearch.indices.recovery.plan.RecoveryPlannerService;
import org.elasticsearch.indices.recovery.plan.ShardSnapshotsService;
import org.elasticsearch.indices.store.IndicesStore;
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
import org.elasticsearch.plugins.TracerPlugin;
import org.elasticsearch.plugins.internal.ReloadAwarePlugin;
import org.elasticsearch.readiness.ReadinessService;
import org.elasticsearch.repositories.RepositoriesModule;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.ReservedClusterStateHandlerProvider;
import org.elasticsearch.reservedstate.action.ReservedClusterSettingsAction;
import org.elasticsearch.reservedstate.service.FileSettingsService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchUtils;
import org.elasticsearch.search.aggregations.support.AggregationUsageService;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.shutdown.PluginShutdownService;
import org.elasticsearch.snapshots.InternalSnapshotsInfoService;
import org.elasticsearch.snapshots.RepositoryIntegrityHealthIndicatorService;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.SnapshotShardsService;
import org.elasticsearch.snapshots.SnapshotsInfoService;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancellationService;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.tasks.TaskResultsService;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.transport.RemoteClusterPortSettings;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.upgrades.SystemIndexMigrationExecutor;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.net.ssl.SNIHostName;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.common.util.CollectionUtils.concatLists;
import static org.elasticsearch.core.Types.forciblyCast;

/**
 * A node represent a node within a cluster ({@code cluster.name}). The {@link #client()} can be used
 * in order to use a {@link Client} to perform actions/operations against the cluster.
 */
public class Node implements Closeable {
    public static final Setting<Boolean> WRITE_PORTS_FILE_SETTING = Setting.boolSetting("node.portsfile", false, Property.NodeScope);

    public static final Setting<String> NODE_NAME_SETTING = Setting.simpleString("node.name", Property.NodeScope);
    public static final Setting<String> NODE_EXTERNAL_ID_SETTING = Setting.simpleString(
        "node.external_id",
        NODE_NAME_SETTING,
        Property.NodeScope
    );
    public static final Setting.AffixSetting<String> NODE_ATTRIBUTES = Setting.prefixKeySetting(
        "node.attr.",
        (key) -> new Setting<>(key, "", (value) -> {
            if (value.length() > 0
                && (Character.isWhitespace(value.charAt(0)) || Character.isWhitespace(value.charAt(value.length() - 1)))) {
                throw new IllegalArgumentException(key + " cannot have leading or trailing whitespace [" + value + "]");
            }
            if (value.length() > 0 && "node.attr.server_name".equals(key)) {
                try {
                    new SNIHostName(value);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("invalid node.attr.server_name [" + value + "]", e);
                }
            }
            return value;
        }, Property.NodeScope)
    );
    public static final Setting<String> BREAKER_TYPE_KEY = new Setting<>("indices.breaker.type", "hierarchy", (s) -> {
        return switch (s) {
            case "hierarchy", "none" -> s;
            default -> throw new IllegalArgumentException("indices.breaker.type must be one of [hierarchy, none] but was: " + s);
        };
    }, Setting.Property.NodeScope);

    public static final Setting<TimeValue> INITIAL_STATE_TIMEOUT_SETTING = Setting.positiveTimeSetting(
        "discovery.initial_state_timeout",
        TimeValue.timeValueSeconds(30),
        Property.NodeScope
    );

    private static final String CLIENT_TYPE = "node";

    private final Lifecycle lifecycle = new Lifecycle();

    /**
     * This logger instance is an instance field as opposed to a static field. This ensures that the field is not
     * initialized until an instance of Node is constructed, which is sure to happen after the logging infrastructure
     * has been initialized to include the hostname. If this field were static, then it would be initialized when the
     * class initializer runs. Alas, this happens too early, before logging is initialized as this class is referred to
     * in InternalSettingsPreparer#finalizeSettings, which runs when creating the Environment, before logging is
     * initialized.
     */
    private final Logger logger = LogManager.getLogger(Node.class);
    private final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(Node.class);
    private final Injector injector;
    private final Environment environment;
    private final NodeEnvironment nodeEnvironment;
    private final PluginsService pluginsService;
    private final NodeClient client;
    private final Collection<LifecycleComponent> pluginLifecycleComponents;
    private final LocalNodeFactory localNodeFactory;
    private final NodeService nodeService;
    private final SetOnce<TerminationHandler> terminationHandler = new SetOnce<>();
    // for testing
    final NamedWriteableRegistry namedWriteableRegistry;
    final NamedXContentRegistry namedXContentRegistry;

    /**
     * Constructs a node
     *
     * @param environment         the initial environment for this node, which will be added to by plugins
     */
    public Node(Environment environment) {
        this(environment, PluginsService.getPluginsServiceCtor(environment), true);
    }

    /**
     * Constructs a node
     *
     * @param initialEnvironment         the initial environment for this node, which will be added to by plugins
     * @param pluginServiceCtor          a function that takes a {@link Settings} object and returns a {@link PluginsService}
     * @param forbidPrivateIndexSettings whether or not private index settings are forbidden when creating an index; this is used in the
     *                                   test framework for tests that rely on being able to set private settings
     */
    protected Node(
        final Environment initialEnvironment,
        final Function<Settings, PluginsService> pluginServiceCtor,
        boolean forbidPrivateIndexSettings
    ) {
        final List<Closeable> resourcesToClose = new ArrayList<>(); // register everything we need to release in the case of an error
        boolean success = false;
        try {
            // Pass the node settings to the DeprecationLogger class so that it can have the deprecation.skip_deprecated_settings setting:
            DeprecationLogger.initialize(initialEnvironment.settings());
            Settings tmpSettings = Settings.builder()
                .put(initialEnvironment.settings())
                .put(Client.CLIENT_TYPE_SETTING_S.getKey(), CLIENT_TYPE)
                .build();

            final JvmInfo jvmInfo = JvmInfo.jvmInfo();
            logger.info(
                "version[{}], pid[{}], build[{}/{}/{}], OS[{}/{}/{}], JVM[{}/{}/{}/{}]",
                Build.CURRENT.qualifiedVersion(),
                jvmInfo.pid(),
                Build.CURRENT.type().displayName(),
                Build.CURRENT.hash(),
                Build.CURRENT.date(),
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
            if (Build.CURRENT.isProductionRelease() == false) {
                logger.warn(
                    "version [{}] is a pre-release version of Elasticsearch and is not suitable for production",
                    Build.CURRENT.qualifiedVersion()
                );
            }
            if (Environment.PATH_SHARED_DATA_SETTING.exists(tmpSettings)) {
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
            if (Environment.dataPathUsesList(tmpSettings)) {
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

            deleteTemporaryApmConfig(
                jvmInfo,
                (e, apmConfig) -> logger.error("failed to delete temporary APM config file [{}], reason: [{}]", apmConfig, e.getMessage())
            );

            this.pluginsService = pluginServiceCtor.apply(tmpSettings);
            final Settings settings = mergePluginSettings(pluginsService.pluginMap(), tmpSettings);

            /*
             * Create the environment based on the finalized view of the settings. This is to ensure that components get the same setting
             * values, no matter they ask for them from.
             */
            this.environment = new Environment(settings, initialEnvironment.configFile());
            Environment.assertEquivalent(initialEnvironment, this.environment);

            final List<ExecutorBuilder<?>> executorBuilders = pluginsService.flatMap(p -> p.getExecutorBuilders(settings)).toList();

            final ThreadPool threadPool = new ThreadPool(settings, executorBuilders.toArray(new ExecutorBuilder<?>[0]));
            resourcesToClose.add(() -> ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));
            final ResourceWatcherService resourceWatcherService = new ResourceWatcherService(settings, threadPool);
            resourcesToClose.add(resourceWatcherService);
            // adds the context to the DeprecationLogger so that it does not need to be injected everywhere
            HeaderWarning.setThreadContext(threadPool.getThreadContext());
            resourcesToClose.add(() -> HeaderWarning.removeThreadContext(threadPool.getThreadContext()));

            final Set<String> taskHeaders = Stream.concat(
                pluginsService.filterPlugins(ActionPlugin.class).stream().flatMap(p -> p.getTaskHeaders().stream()),
                Task.HEADERS_TO_COPY.stream()
            ).collect(Collectors.toSet());

            final Tracer tracer = getTracer(pluginsService, settings);

            final TaskManager taskManager = new TaskManager(settings, threadPool, taskHeaders, tracer);

            // register the node.data, node.ingest, node.master, node.remote_cluster_client settings here so we can mark them private
            final List<Setting<?>> additionalSettings = new ArrayList<>(pluginsService.flatMap(Plugin::getSettings).toList());
            for (final ExecutorBuilder<?> builder : threadPool.builders()) {
                additionalSettings.addAll(builder.getRegisteredSettings());
            }
            client = new NodeClient(settings, threadPool);

            final ScriptModule scriptModule = new ScriptModule(settings, pluginsService.filterPlugins(ScriptPlugin.class));
            final ScriptService scriptService = newScriptService(
                settings,
                scriptModule.engines,
                scriptModule.contexts,
                threadPool::absoluteTimeInMillis
            );
            AnalysisModule analysisModule = new AnalysisModule(
                this.environment,
                pluginsService.filterPlugins(AnalysisPlugin.class),
                pluginsService.getStablePluginRegistry()
            );
            // this is as early as we can validate settings at this point. we already pass them to ScriptModule as well as ThreadPool
            // so we might be late here already

            final Set<SettingUpgrader<?>> settingsUpgraders = pluginsService.flatMap(Plugin::getSettingUpgraders)
                .collect(Collectors.toSet());

            final SettingsModule settingsModule = new SettingsModule(
                settings,
                additionalSettings,
                pluginsService.flatMap(Plugin::getSettingsFilter).toList(),
                settingsUpgraders
            );

            // creating `NodeEnvironment` breaks the ability to rollback to 7.x on an 8.0 upgrade (`upgradeLegacyNodeFolders`) so do this
            // after settings validation.
            nodeEnvironment = new NodeEnvironment(tmpSettings, environment);
            logger.info(
                "node name [{}], node ID [{}], cluster name [{}], roles {}",
                NODE_NAME_SETTING.get(tmpSettings),
                nodeEnvironment.nodeId(),
                ClusterName.CLUSTER_NAME_SETTING.get(tmpSettings).value(),
                DiscoveryNode.getRolesFromSettings(settings)
                    .stream()
                    .map(DiscoveryNodeRole::roleName)
                    .collect(Collectors.toCollection(LinkedHashSet::new))
            );
            resourcesToClose.add(nodeEnvironment);
            localNodeFactory = new LocalNodeFactory(settings, nodeEnvironment.nodeId());

            ScriptModule.registerClusterSettingsListeners(scriptService, settingsModule.getClusterSettings());
            final NetworkService networkService = new NetworkService(
                getCustomNameResolvers(pluginsService.filterPlugins(DiscoveryPlugin.class))
            );

            List<ClusterPlugin> clusterPlugins = pluginsService.filterPlugins(ClusterPlugin.class);
            final ClusterService clusterService = new ClusterService(
                settings,
                settingsModule.getClusterSettings(),
                threadPool,
                taskManager
            );
            clusterService.addStateApplier(scriptService);
            resourcesToClose.add(clusterService);

            final Set<Setting<?>> consistentSettings = settingsModule.getConsistentSettings();
            if (consistentSettings.isEmpty() == false) {
                clusterService.addLocalNodeMasterListener(
                    new ConsistentSettingsService(settings, clusterService, consistentSettings).newHashPublisher()
                );
            }
            final IngestService ingestService = new IngestService(
                clusterService,
                threadPool,
                this.environment,
                scriptService,
                analysisModule.getAnalysisRegistry(),
                pluginsService.filterPlugins(IngestPlugin.class),
                client,
                IngestService.createGrokThreadWatchdog(this.environment, threadPool)
            );
            final SetOnce<RepositoriesService> repositoriesServiceReference = new SetOnce<>();
            final ClusterInfoService clusterInfoService = newClusterInfoService(settings, clusterService, threadPool, client);
            final UsageService usageService = new UsageService();

            SearchModule searchModule = new SearchModule(settings, pluginsService.filterPlugins(SearchPlugin.class));
            IndexSearcher.setMaxClauseCount(SearchUtils.calculateMaxClauseValue(threadPool));
            List<NamedWriteableRegistry.Entry> namedWriteables = Stream.of(
                NetworkModule.getNamedWriteables().stream(),
                IndicesModule.getNamedWriteables().stream(),
                searchModule.getNamedWriteables().stream(),
                pluginsService.flatMap(Plugin::getNamedWriteables),
                ClusterModule.getNamedWriteables().stream(),
                SystemIndexMigrationExecutor.getNamedWriteables().stream()
            ).flatMap(Function.identity()).toList();
            final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
            NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(
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
            final List<SystemIndices.Feature> features = pluginsService.filterPlugins(SystemIndexPlugin.class).stream().map(plugin -> {
                SystemIndices.validateFeatureName(plugin.getFeatureName(), plugin.getClass().getCanonicalName());
                return SystemIndices.Feature.fromSystemIndexPlugin(plugin, settings);
            }).toList();
            final SystemIndices systemIndices = new SystemIndices(features);
            final ExecutorSelector executorSelector = systemIndices.getExecutorSelector();

            ModulesBuilder modules = new ModulesBuilder();
            final MonitorService monitorService = new MonitorService(settings, nodeEnvironment, threadPool);
            final FsHealthService fsHealthService = new FsHealthService(
                settings,
                clusterService.getClusterSettings(),
                threadPool,
                nodeEnvironment
            );
            final SetOnce<RerouteService> rerouteServiceReference = new SetOnce<>();
            final InternalSnapshotsInfoService snapshotsInfoService = new InternalSnapshotsInfoService(
                settings,
                clusterService,
                repositoriesServiceReference::get,
                rerouteServiceReference::get
            );
            final WriteLoadForecaster writeLoadForecaster = getWriteLoadForecaster(
                threadPool,
                settings,
                clusterService.getClusterSettings()
            );
            final ClusterModule clusterModule = new ClusterModule(
                settings,
                clusterService,
                clusterPlugins,
                clusterInfoService,
                snapshotsInfoService,
                threadPool,
                systemIndices,
                writeLoadForecaster
            );
            modules.add(clusterModule);
            IndicesModule indicesModule = new IndicesModule(pluginsService.filterPlugins(MapperPlugin.class));
            modules.add(indicesModule);

            List<BreakerSettings> pluginCircuitBreakers = pluginsService.filterPlugins(CircuitBreakerPlugin.class)
                .stream()
                .map(plugin -> plugin.getCircuitBreaker(settings))
                .toList();
            final CircuitBreakerService circuitBreakerService = createCircuitBreakerService(
                settingsModule.getSettings(),
                pluginCircuitBreakers,
                settingsModule.getClusterSettings()
            );
            pluginsService.filterPlugins(CircuitBreakerPlugin.class).forEach(plugin -> {
                CircuitBreaker breaker = circuitBreakerService.getBreaker(plugin.getCircuitBreaker(settings).getName());
                plugin.setCircuitBreaker(breaker);
            });
            resourcesToClose.add(circuitBreakerService);
            modules.add(new GatewayModule());

            PageCacheRecycler pageCacheRecycler = createPageCacheRecycler(settings);
            BigArrays bigArrays = createBigArrays(pageCacheRecycler, circuitBreakerService);
            modules.add(settingsModule);
            final MetaStateService metaStateService = new MetaStateService(nodeEnvironment, xContentRegistry);
            final PersistedClusterStateService persistedClusterStateService = newPersistedClusterStateService(
                xContentRegistry,
                clusterService.getClusterSettings(),
                threadPool
            );

            // collect engine factory providers from plugins
            final Collection<EnginePlugin> enginePlugins = pluginsService.filterPlugins(EnginePlugin.class);
            final Collection<Function<IndexSettings, Optional<EngineFactory>>> engineFactoryProviders = enginePlugins.stream()
                .map(plugin -> (Function<IndexSettings, Optional<EngineFactory>>) plugin::getEngineFactory)
                .toList();

            final Map<String, IndexStorePlugin.DirectoryFactory> indexStoreFactories = pluginsService.filterPlugins(IndexStorePlugin.class)
                .stream()
                .map(IndexStorePlugin::getDirectoryFactories)
                .flatMap(m -> m.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            final Map<String, IndexStorePlugin.RecoveryStateFactory> recoveryStateFactories = pluginsService.filterPlugins(
                IndexStorePlugin.class
            )
                .stream()
                .map(IndexStorePlugin::getRecoveryStateFactories)
                .flatMap(m -> m.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            final List<IndexStorePlugin.IndexFoldersDeletionListener> indexFoldersDeletionListeners = pluginsService.filterPlugins(
                IndexStorePlugin.class
            ).stream().map(IndexStorePlugin::getIndexFoldersDeletionListeners).flatMap(List::stream).toList();

            final Map<String, IndexStorePlugin.SnapshotCommitSupplier> snapshotCommitSuppliers = pluginsService.filterPlugins(
                IndexStorePlugin.class
            )
                .stream()
                .map(IndexStorePlugin::getSnapshotCommitSuppliers)
                .flatMap(m -> m.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            if (DiscoveryNode.isMasterNode(settings)) {
                clusterService.addListener(new SystemIndexMappingUpdateService(systemIndices, client));
                clusterService.addListener(new TransportVersionsFixupListener(clusterService, client.admin().cluster(), threadPool));
            }

            final RerouteService rerouteService = new BatchedRerouteService(clusterService, clusterModule.getAllocationService()::reroute);
            rerouteServiceReference.set(rerouteService);
            clusterService.setRerouteService(rerouteService);

            final IndicesService indicesService = new IndicesService(
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
                searchModule.getRequestCacheKeyDifferentiator()
            );

            final var parameters = new IndexSettingProvider.Parameters(indicesService::createIndexMapperServiceForValidation);
            IndexSettingProviders indexSettingProviders = new IndexSettingProviders(
                pluginsService.flatMap(p -> p.getAdditionalIndexSettingProviders(parameters)).collect(Collectors.toSet())
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

            final MetadataCreateDataStreamService metadataCreateDataStreamService = new MetadataCreateDataStreamService(
                threadPool,
                clusterService,
                metadataCreateIndexService
            );
            final MetadataDataStreamsService metadataDataStreamsService = new MetadataDataStreamsService(clusterService, indicesService);

            final MetadataUpdateSettingsService metadataUpdateSettingsService = new MetadataUpdateSettingsService(
                clusterService,
                clusterModule.getAllocationService(),
                settingsModule.getIndexScopedSettings(),
                indicesService,
                shardLimitValidator,
                threadPool
            );

            Collection<Object> pluginComponents = pluginsService.flatMap(
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
                    tracer,
                    clusterModule.getAllocationService()
                )
            ).toList();

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

            List<TerminationHandler> terminationHandlers = pluginsService.loadServiceProviders(TerminationHandlerProvider.class)
                .stream()
                .map(prov -> prov.handler())
                .toList();
            if (terminationHandlers.size() == 1) {
                this.terminationHandler.set(terminationHandlers.get(0));
            } else if (terminationHandlers.size() > 1) {
                throw new IllegalStateException(
                    Strings.format(
                        "expected at most one termination handler, but found %s: [%s]",
                        terminationHandlers.size(),
                        terminationHandlers.stream().map(it -> it.getClass().getCanonicalName())
                    )
                );
            }

            ActionModule actionModule = new ActionModule(
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
                reservedStateHandlers
            );
            modules.add(actionModule);

            final RestController restController = actionModule.getRestController();
            final NetworkModule networkModule = new NetworkModule(
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
            Collection<UnaryOperator<Map<String, IndexTemplateMetadata>>> indexTemplateMetadataUpgraders = pluginsService.map(
                Plugin::getIndexTemplateMetadataUpgrader
            ).toList();
            final MetadataUpgrader metadataUpgrader = new MetadataUpgrader(indexTemplateMetadataUpgraders);
            final IndexMetadataVerifier indexMetadataVerifier = new IndexMetadataVerifier(
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
            new TemplateUpgradeService(client, clusterService, threadPool, indexTemplateMetadataUpgraders);
            final Transport transport = networkModule.getTransportSupplier().get();
            final TransportService transportService = newTransportService(
                settings,
                transport,
                threadPool,
                networkModule.getTransportInterceptor(),
                localNodeFactory,
                settingsModule.getClusterSettings(),
                taskManager,
                tracer
            );
            final GatewayMetaState gatewayMetaState = new GatewayMetaState();
            final ResponseCollectorService responseCollectorService = new ResponseCollectorService(clusterService);
            final SearchTransportService searchTransportService = new SearchTransportService(
                transportService,
                client,
                SearchExecutionStatsCollector.makeWrapper(responseCollectorService)
            );
            final HttpServerTransport httpServerTransport = newHttpTransport(networkModule);
            final IndexingPressure indexingLimits = new IndexingPressure(settings);

            final RecoverySettings recoverySettings = new RecoverySettings(settings, settingsModule.getClusterSettings());
            RepositoriesModule repositoriesModule = new RepositoriesModule(
                this.environment,
                pluginsService.filterPlugins(RepositoryPlugin.class),
                transportService,
                clusterService,
                bigArrays,
                xContentRegistry,
                recoverySettings
            );
            RepositoriesService repositoryService = repositoriesModule.getRepositoryService();
            repositoriesServiceReference.set(repositoryService);
            SnapshotsService snapshotsService = new SnapshotsService(
                settings,
                clusterService,
                clusterModule.getIndexNameExpressionResolver(),
                repositoryService,
                transportService,
                actionModule.getActionFilters(),
                systemIndices
            );
            SnapshotShardsService snapshotShardsService = new SnapshotShardsService(
                settings,
                clusterService,
                repositoryService,
                transportService,
                indicesService
            );

            actionModule.getReservedClusterStateService().installStateHandler(new ReservedRepositoryAction(repositoryService));
            actionModule.getReservedClusterStateService().installStateHandler(new ReservedPipelineAction());

            FileSettingsService fileSettingsService = new FileSettingsService(
                clusterService,
                actionModule.getReservedClusterStateService(),
                environment
            );

            RestoreService restoreService = new RestoreService(
                clusterService,
                repositoryService,
                clusterModule.getAllocationService(),
                metadataCreateIndexService,
                clusterModule.getMetadataDeleteIndexService(),
                indexMetadataVerifier,
                shardLimitValidator,
                systemIndices,
                indicesService,
                fileSettingsService,
                threadPool
            );
            final DiskThresholdMonitor diskThresholdMonitor = new DiskThresholdMonitor(
                settings,
                clusterService::state,
                clusterService.getClusterSettings(),
                client,
                threadPool::relativeTimeInMillis,
                rerouteService
            );
            clusterInfoService.addListener(diskThresholdMonitor::onNewInfo);

            final DiscoveryModule discoveryModule = new DiscoveryModule(
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
                rerouteService,
                fsHealthService,
                circuitBreakerService
            );
            this.nodeService = new NodeService(
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
                searchModule.getValuesSourceRegistry().getUsageService()
            );

            final SearchService searchService = newSearchService(
                clusterService,
                indicesService,
                threadPool,
                scriptService,
                bigArrays,
                searchModule.getFetchPhase(),
                responseCollectorService,
                circuitBreakerService,
                executorSelector,
                tracer
            );

            final PersistentTasksService persistentTasksService = new PersistentTasksService(clusterService, threadPool, client);
            final SystemIndexMigrationExecutor systemIndexMigrationExecutor = new SystemIndexMigrationExecutor(
                client,
                clusterService,
                systemIndices,
                metadataUpdateSettingsService,
                metadataCreateIndexService,
                settingsModule.getIndexScopedSettings()
            );
            final HealthNodeTaskExecutor healthNodeTaskExecutor = HealthNodeTaskExecutor.create(
                clusterService,
                persistentTasksService,
                settings,
                clusterService.getClusterSettings()
            );
            final List<PersistentTasksExecutor<?>> builtinTaskExecutors = List.of(systemIndexMigrationExecutor, healthNodeTaskExecutor);
            final List<PersistentTasksExecutor<?>> pluginTaskExecutors = pluginsService.filterPlugins(PersistentTaskPlugin.class)
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
            final PersistentTasksExecutorRegistry registry = new PersistentTasksExecutorRegistry(
                concatLists(pluginTaskExecutors, builtinTaskExecutors)
            );
            final PersistentTasksClusterService persistentTasksClusterService = new PersistentTasksClusterService(
                settings,
                registry,
                clusterService,
                threadPool
            );
            resourcesToClose.add(persistentTasksClusterService);

            final List<ShutdownAwarePlugin> shutdownAwarePlugins = pluginsService.filterPlugins(ShutdownAwarePlugin.class);
            final PluginShutdownService pluginShutdownService = new PluginShutdownService(shutdownAwarePlugins);
            clusterService.addListener(pluginShutdownService);

            final RecoveryPlannerService recoveryPlannerService = getRecoveryPlannerService(threadPool, clusterService, repositoryService);
            final DesiredNodesSettingsValidator desiredNodesSettingsValidator = new DesiredNodesSettingsValidator(
                clusterService.getClusterSettings()
            );

            MasterHistoryService masterHistoryService = new MasterHistoryService(transportService, threadPool, clusterService);
            CoordinationDiagnosticsService coordinationDiagnosticsService = new CoordinationDiagnosticsService(
                clusterService,
                transportService,
                discoveryModule.getCoordinator(),
                masterHistoryService
            );
            HealthService healthService = createHealthService(
                clusterService,
                clusterModule,
                coordinationDiagnosticsService,
                threadPool,
                systemIndices
            );

            HealthPeriodicLogger healthPeriodicLogger = createHealthPeriodicLogger(clusterService, settings, client, healthService);
            healthPeriodicLogger.init();

            HealthMetadataService healthMetadataService = HealthMetadataService.create(clusterService, settings);
            LocalHealthMonitor localHealthMonitor = LocalHealthMonitor.create(settings, clusterService, nodeService, threadPool, client);
            HealthInfoCache nodeHealthOverview = HealthInfoCache.create(clusterService);
            HealthApiStats healthApiStats = new HealthApiStats();

            List<ReloadablePlugin> reloadablePlugins = pluginsService.filterPlugins(ReloadablePlugin.class);
            pluginsService.filterPlugins(ReloadAwarePlugin.class).forEach(p -> p.setReloadCallback(wrapPlugins(reloadablePlugins)));

            modules.add(b -> {
                b.bind(Node.class).toInstance(this);
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
                {
                    processRecoverySettings(settingsModule.getClusterSettings(), recoverySettings);
                    final SnapshotFilesProvider snapshotFilesProvider = new SnapshotFilesProvider(repositoryService);
                    b.bind(PeerRecoverySourceService.class)
                        .toInstance(
                            new PeerRecoverySourceService(transportService, indicesService, recoverySettings, recoveryPlannerService)
                        );
                    b.bind(PeerRecoveryTargetService.class)
                        .toInstance(
                            new PeerRecoveryTargetService(
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
                b.bind(PersistentTasksExecutorRegistry.class).toInstance(registry);
                b.bind(RepositoriesService.class).toInstance(repositoryService);
                b.bind(SnapshotsService.class).toInstance(snapshotsService);
                b.bind(SnapshotShardsService.class).toInstance(snapshotShardsService);
                b.bind(RestoreService.class).toInstance(restoreService);
                b.bind(RerouteService.class).toInstance(rerouteService);
                b.bind(ShardLimitValidator.class).toInstance(shardLimitValidator);
                b.bind(FsHealthService.class).toInstance(fsHealthService);
                b.bind(SystemIndices.class).toInstance(systemIndices);
                b.bind(PluginShutdownService.class).toInstance(pluginShutdownService);
                b.bind(ExecutorSelector.class).toInstance(executorSelector);
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
            });

            if (ReadinessService.enabled(environment)) {
                modules.add(b -> b.bind(ReadinessService.class).toInstance(new ReadinessService(clusterService, environment)));
            }

            injector = modules.createInjector();

            // We allocate copies of existing shards by looking for a viable copy of the shard in the cluster and assigning the shard there.
            // The search for viable copies is triggered by an allocation attempt (i.e. a reroute) and is performed asynchronously. When it
            // completes we trigger another reroute to try the allocation again. This means there is a circular dependency: the allocation
            // service needs access to the existing shards allocators (e.g. the GatewayAllocator) which need to be able to trigger a
            // reroute, which needs to call into the allocation service. We close the loop here:
            clusterModule.setExistingShardsAllocators(injector.getInstance(GatewayAllocator.class));

            List<LifecycleComponent> pluginLifecycleComponents = pluginComponents.stream().map(p -> {
                if (p instanceof PluginComponentBinding<?, ?> pcb) {
                    return pcb.impl();
                }
                return p;
            }).filter(p -> p instanceof LifecycleComponent).map(p -> (LifecycleComponent) p).toList();
            resourcesToClose.addAll(pluginLifecycleComponents);
            resourcesToClose.add(injector.getInstance(PeerRecoverySourceService.class));
            this.pluginLifecycleComponents = Collections.unmodifiableList(pluginLifecycleComponents);

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
            this.namedWriteableRegistry = namedWriteableRegistry;
            this.namedXContentRegistry = xContentRegistry;

            logger.debug("initializing HTTP handlers ...");
            actionModule.initRestHandlers(() -> clusterService.state().nodesIfRecovered());
            logger.info("initialized");

            success = true;
        } catch (IOException ex) {
            throw new ElasticsearchException("failed to bind service", ex);
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(resourcesToClose);
            }
        }
    }

    /**
     * If the JVM was started with the Elastic APM agent and a config file argument was specified, then
     * delete the config file. The agent only reads it once, when supplied in this fashion, and it
     * may contain a secret token.
     * <p>
     * Public for testing only
     */
    @SuppressForbidden(reason = "Cannot guarantee that the temp config path is relative to the environment")
    public static void deleteTemporaryApmConfig(JvmInfo jvmInfo, BiConsumer<Exception, Path> errorHandler) {
        for (String inputArgument : jvmInfo.getInputArguments()) {
            if (inputArgument.startsWith("-javaagent:")) {
                final String agentArg = inputArgument.substring(11);
                final String[] parts = agentArg.split("=", 2);
                String APM_AGENT_CONFIG_FILE_REGEX = String.join(
                    "\\" + File.separator,
                    ".*modules",
                    "apm",
                    "elastic-apm-agent-\\d+\\.\\d+\\.\\d+\\.jar"
                );
                if (parts[0].matches(APM_AGENT_CONFIG_FILE_REGEX)) {
                    if (parts.length == 2 && parts[1].startsWith("c=")) {
                        final Path apmConfig = PathUtils.get(parts[1].substring(2));
                        if (apmConfig.getFileName().toString().matches("^\\.elstcapm\\..*\\.tmp")) {
                            try {
                                Files.deleteIfExists(apmConfig);
                            } catch (IOException e) {
                                errorHandler.accept(e, apmConfig);
                            }
                        }
                    }
                    return;
                }
            }
        }
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

    private Tracer getTracer(PluginsService pluginsService, Settings settings) {
        final List<TracerPlugin> tracerPlugins = pluginsService.filterPlugins(TracerPlugin.class);

        if (tracerPlugins.size() > 1) {
            throw new IllegalStateException("A single TracerPlugin was expected but got: " + tracerPlugins);
        }

        return tracerPlugins.isEmpty() ? Tracer.NOOP : tracerPlugins.get(0).getTracer(settings);
    }

    private HealthService createHealthService(
        ClusterService clusterService,
        ClusterModule clusterModule,
        CoordinationDiagnosticsService coordinationDiagnosticsService,
        ThreadPool threadPool,
        SystemIndices systemIndices
    ) {
        List<HealthIndicatorService> preflightHealthIndicatorServices = Collections.singletonList(
            new StableMasterHealthIndicatorService(coordinationDiagnosticsService, clusterService)
        );
        var serverHealthIndicatorServices = new ArrayList<>(
            List.of(
                new RepositoryIntegrityHealthIndicatorService(clusterService),
                new ShardsAvailabilityHealthIndicatorService(clusterService, clusterModule.getAllocationService(), systemIndices)
            )
        );
        serverHealthIndicatorServices.add(new DiskHealthIndicatorService(clusterService));
        serverHealthIndicatorServices.add(new ShardsCapacityHealthIndicatorService(clusterService));
        var pluginHealthIndicatorServices = pluginsService.filterPlugins(HealthPlugin.class)
            .stream()
            .flatMap(plugin -> plugin.getHealthIndicatorServices().stream())
            .toList();
        return new HealthService(
            preflightHealthIndicatorServices,
            concatLists(serverHealthIndicatorServices, pluginHealthIndicatorServices),
            threadPool
        );
    }

    private HealthPeriodicLogger createHealthPeriodicLogger(
        ClusterService clusterService,
        Settings settings,
        NodeClient client,
        HealthService healthService
    ) {
        return new HealthPeriodicLogger(settings, clusterService, client, healthService);
    }

    private RecoveryPlannerService getRecoveryPlannerService(
        ThreadPool threadPool,
        ClusterService clusterService,
        RepositoriesService repositoryService
    ) {
        final List<RecoveryPlannerService> recoveryPlannerServices = pluginsService.filterPlugins(RecoveryPlannerPlugin.class)
            .stream()
            .map(
                plugin -> plugin.createRecoveryPlannerService(
                    new ShardSnapshotsService(client, repositoryService, threadPool, clusterService)
                )
            )
            .filter(Optional::isPresent)
            .map(Optional::get)
            .toList();
        if (recoveryPlannerServices.isEmpty()) {
            return new PeerOnlyRecoveryPlannerService();
        } else if (recoveryPlannerServices.size() > 1) {
            throw new IllegalStateException("Expected a single RecoveryPlannerService but got: " + recoveryPlannerServices.size());
        }
        return recoveryPlannerServices.get(0);
    }

    private WriteLoadForecaster getWriteLoadForecaster(ThreadPool threadPool, Settings settings, ClusterSettings clusterSettings) {
        final List<ClusterPlugin> clusterPlugins = pluginsService.filterPlugins(ClusterPlugin.class);
        final List<WriteLoadForecaster> writeLoadForecasters = clusterPlugins.stream()
            .flatMap(clusterPlugin -> clusterPlugin.createWriteLoadForecasters(threadPool, settings, clusterSettings).stream())
            .toList();

        if (writeLoadForecasters.isEmpty()) {
            return WriteLoadForecaster.DEFAULT;
        }

        if (writeLoadForecasters.size() > 1) {
            throw new IllegalStateException("A single WriteLoadForecaster was expected but got: " + writeLoadForecasters);
        }

        return writeLoadForecasters.get(0);
    }

    private PersistedClusterStateService newPersistedClusterStateService(
        NamedXContentRegistry xContentRegistry,
        ClusterSettings clusterSettings,
        ThreadPool threadPool
    ) {
        final List<ClusterCoordinationPlugin.PersistedClusterStateServiceFactory> persistedClusterStateServiceFactories = pluginsService
            .filterPlugins(ClusterCoordinationPlugin.class)
            .stream()
            .map(ClusterCoordinationPlugin::getPersistedClusterStateServiceFactory)
            .flatMap(Optional::stream)
            .toList();

        if (persistedClusterStateServiceFactories.size() > 1) {
            throw new IllegalStateException("multiple persisted-state-service factories found: " + persistedClusterStateServiceFactories);
        }

        if (persistedClusterStateServiceFactories.size() == 1) {
            return persistedClusterStateServiceFactories.get(0)
                .newPersistedClusterStateService(nodeEnvironment, xContentRegistry, clusterSettings, threadPool);
        }

        return new PersistedClusterStateService(nodeEnvironment, xContentRegistry, clusterSettings, threadPool::relativeTimeInMillis);
    }

    protected TransportService newTransportService(
        Settings settings,
        Transport transport,
        ThreadPool threadPool,
        TransportInterceptor interceptor,
        Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
        ClusterSettings clusterSettings,
        TaskManager taskManager,
        Tracer tracer
    ) {
        return new TransportService(settings, transport, threadPool, interceptor, localNodeFactory, clusterSettings, taskManager, tracer);
    }

    protected void processRecoverySettings(ClusterSettings clusterSettings, RecoverySettings recoverySettings) {
        // Noop in production, overridden by tests
    }

    /**
     * The settings that are used by this node. Contains original settings as well as additional settings provided by plugins.
     */
    public Settings settings() {
        return this.environment.settings();
    }

    /**
     * A client that can be used to execute actions (operations) against the cluster.
     */
    public Client client() {
        return client;
    }

    /**
     * Returns the environment of the node
     */
    public Environment getEnvironment() {
        return environment;
    }

    /**
     * Returns the {@link NodeEnvironment} instance of this node
     */
    public NodeEnvironment getNodeEnvironment() {
        return nodeEnvironment;
    }

    /**
     * Start the node. If the node is already started, this method is no-op.
     */
    public Node start() throws NodeValidationException {
        if (lifecycle.moveToStarted() == false) {
            return this;
        }

        logger.info("starting ...");
        pluginLifecycleComponents.forEach(LifecycleComponent::start);

        if (ReadinessService.enabled(environment)) {
            injector.getInstance(ReadinessService.class).start();
        }
        injector.getInstance(MappingUpdatedAction.class).setClient(client);
        injector.getInstance(IndicesService.class).start();
        injector.getInstance(IndicesClusterStateService.class).start();
        injector.getInstance(SnapshotsService.class).start();
        injector.getInstance(SnapshotShardsService.class).start();
        injector.getInstance(RepositoriesService.class).start();
        injector.getInstance(SearchService.class).start();
        injector.getInstance(FsHealthService.class).start();
        nodeService.getMonitorService().start();

        final ClusterService clusterService = injector.getInstance(ClusterService.class);

        final NodeConnectionsService nodeConnectionsService = injector.getInstance(NodeConnectionsService.class);
        nodeConnectionsService.start();
        clusterService.setNodeConnectionsService(nodeConnectionsService);

        injector.getInstance(GatewayService.class).start();
        final Coordinator coordinator = injector.getInstance(Coordinator.class);
        clusterService.getMasterService().setClusterStatePublisher(coordinator);

        // Start the transport service now so the publish address will be added to the local disco node in ClusterService
        TransportService transportService = injector.getInstance(TransportService.class);
        transportService.getTaskManager().setTaskResultsService(injector.getInstance(TaskResultsService.class));
        transportService.getTaskManager().setTaskCancellationService(new TaskCancellationService(transportService));
        transportService.start();
        assert localNodeFactory.getNode() != null;
        assert transportService.getLocalNode().equals(localNodeFactory.getNode())
            : "transportService has a different local node than the factory provided";
        injector.getInstance(PeerRecoverySourceService.class).start();

        // Load (and maybe upgrade) the metadata stored on disk
        final GatewayMetaState gatewayMetaState = injector.getInstance(GatewayMetaState.class);
        gatewayMetaState.start(
            settings(),
            transportService,
            clusterService,
            injector.getInstance(MetaStateService.class),
            injector.getInstance(IndexMetadataVerifier.class),
            injector.getInstance(MetadataUpgrader.class),
            injector.getInstance(PersistedClusterStateService.class),
            pluginsService.filterPlugins(ClusterCoordinationPlugin.class)
        );
        // TODO: Do not expect that the legacy metadata file is always present https://github.com/elastic/elasticsearch/issues/95211
        if (Assertions.ENABLED && DiscoveryNode.isStateless(settings()) == false) {
            try {
                assert injector.getInstance(MetaStateService.class).loadFullState().v1().isEmpty();
                final NodeMetadata nodeMetadata = NodeMetadata.FORMAT.loadLatestState(
                    logger,
                    NamedXContentRegistry.EMPTY,
                    nodeEnvironment.nodeDataPaths()
                );
                assert nodeMetadata != null;
                assert nodeMetadata.nodeVersion().equals(Version.CURRENT);
                assert nodeMetadata.nodeId().equals(localNodeFactory.getNode().getId());
            } catch (IOException e) {
                assert false : e;
            }
        }
        // we load the global state here (the persistent part of the cluster state stored on disk) to
        // pass it to the bootstrap checks to allow plugins to enforce certain preconditions based on the recovered state.
        final Metadata onDiskMetadata = gatewayMetaState.getPersistedState().getLastAcceptedState().metadata();
        assert onDiskMetadata != null : "metadata is null but shouldn't"; // this is never null
        validateNodeBeforeAcceptingRequests(
            new BootstrapContext(environment, onDiskMetadata),
            transportService.boundAddress(),
            pluginsService.flatMap(Plugin::getBootstrapChecks).toList()
        );

        final FileSettingsService fileSettingsService = injector.getInstance(FileSettingsService.class);
        fileSettingsService.start();
        // if we are using the readiness service, listen for the file settings being applied
        if (ReadinessService.enabled(environment)) {
            fileSettingsService.addFileChangedListener(injector.getInstance(ReadinessService.class));
        }

        clusterService.addStateApplier(transportService.getTaskManager());
        // start after transport service so the local disco is known
        coordinator.start(); // start before cluster service so that it can set initial state on ClusterApplierService
        clusterService.start();
        assert clusterService.localNode().equals(localNodeFactory.getNode())
            : "clusterService has a different local node than the factory provided";
        transportService.acceptIncomingRequests();
        /*
         * CoordinationDiagnosticsService expects to be able to send transport requests and use the cluster state, so it is important to
         * start it here after the clusterService and transportService have been started.
         */
        injector.getInstance(CoordinationDiagnosticsService.class).start();
        coordinator.startInitialJoin();
        final TimeValue initialStateTimeout = INITIAL_STATE_TIMEOUT_SETTING.get(settings());
        configureNodeAndClusterIdStateListener(clusterService);

        if (initialStateTimeout.millis() > 0) {
            final ThreadPool thread = injector.getInstance(ThreadPool.class);
            ClusterState clusterState = clusterService.state();
            ClusterStateObserver observer = new ClusterStateObserver(clusterState, clusterService, null, logger, thread.getThreadContext());

            if (clusterState.nodes().getMasterNodeId() == null) {
                logger.debug("waiting to join the cluster. timeout [{}]", initialStateTimeout);
                final CountDownLatch latch = new CountDownLatch(1);
                observer.waitForNextChange(new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        latch.countDown();
                    }

                    @Override
                    public void onClusterServiceClose() {
                        latch.countDown();
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        logger.warn("timed out while waiting for initial discovery state - timeout: {}", initialStateTimeout);
                        latch.countDown();
                    }
                }, state -> state.nodes().getMasterNodeId() != null, initialStateTimeout);

                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new ElasticsearchTimeoutException("Interrupted while waiting for initial discovery state");
                }
            }
        }

        injector.getInstance(HttpServerTransport.class).start();

        if (WRITE_PORTS_FILE_SETTING.get(settings())) {
            TransportService transport = injector.getInstance(TransportService.class);
            writePortsFile("transport", transport.boundAddress());
            HttpServerTransport http = injector.getInstance(HttpServerTransport.class);
            writePortsFile("http", http.boundAddress());

            if (ReadinessService.enabled(environment)) {
                ReadinessService readiness = injector.getInstance(ReadinessService.class);
                readiness.addBoundAddressListener(address -> writePortsFile("readiness", address));
            }

            if (RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED.get(environment.settings())) {
                writePortsFile("remote_cluster", transport.boundRemoteAccessAddress());
            }
        }

        logger.info("started {}", transportService.getLocalNode());

        pluginsService.filterPlugins(ClusterPlugin.class).forEach(ClusterPlugin::onNodeStarted);

        return this;
    }

    protected void configureNodeAndClusterIdStateListener(ClusterService clusterService) {
        NodeAndClusterIdStateListener.getAndSetNodeIdAndClusterId(
            clusterService,
            injector.getInstance(ThreadPool.class).getThreadContext()
        );
    }

    private Node stop() {
        if (lifecycle.moveToStopped() == false) {
            return this;
        }
        logger.info("stopping ...");

        if (ReadinessService.enabled(environment)) {
            injector.getInstance(ReadinessService.class).stop();
        }
        injector.getInstance(FileSettingsService.class).stop();
        injector.getInstance(ResourceWatcherService.class).close();
        injector.getInstance(HttpServerTransport.class).stop();

        injector.getInstance(SnapshotsService.class).stop();
        injector.getInstance(SnapshotShardsService.class).stop();
        injector.getInstance(RepositoriesService.class).stop();
        // stop any changes happening as a result of cluster state changes
        injector.getInstance(IndicesClusterStateService.class).stop();
        // close cluster coordinator early to not react to pings anymore.
        // This can confuse other nodes and delay things - mostly if we're the master and we're running tests.
        injector.getInstance(Coordinator.class).stop();
        // we close indices first, so operations won't be allowed on it
        injector.getInstance(ClusterService.class).stop();
        injector.getInstance(NodeConnectionsService.class).stop();
        injector.getInstance(FsHealthService.class).stop();
        nodeService.getMonitorService().stop();
        injector.getInstance(GatewayService.class).stop();
        injector.getInstance(SearchService.class).stop();
        injector.getInstance(TransportService.class).stop();

        pluginLifecycleComponents.forEach(LifecycleComponent::stop);
        // we should stop this last since it waits for resources to get released
        // if we had scroll searchers etc or recovery going on we wait for to finish.
        injector.getInstance(IndicesService.class).stop();
        logger.info("stopped");

        return this;
    }

    // During concurrent close() calls we want to make sure that all of them return after the node has completed it's shutdown cycle.
    // If not, the hook that is added in Bootstrap#setup() will be useless:
    // close() might not be executed, in case another (for example api) call to close() has already set some lifecycles to stopped.
    // In this case the process will be terminated even if the first call to close() has not finished yet.
    @Override
    public synchronized void close() throws IOException {
        synchronized (lifecycle) {
            if (lifecycle.started()) {
                stop();
            }
            if (lifecycle.moveToClosed() == false) {
                return;
            }
        }

        logger.info("closing ...");
        List<Closeable> toClose = new ArrayList<>();
        StopWatch stopWatch = new StopWatch("node_close");
        toClose.add(() -> stopWatch.start("node_service"));
        toClose.add(nodeService);
        toClose.add(() -> stopWatch.stop().start("http"));
        toClose.add(injector.getInstance(HttpServerTransport.class));
        toClose.add(() -> stopWatch.stop().start("snapshot_service"));
        toClose.add(injector.getInstance(SnapshotsService.class));
        toClose.add(injector.getInstance(SnapshotShardsService.class));
        toClose.add(injector.getInstance(RepositoriesService.class));
        toClose.add(() -> stopWatch.stop().start("client"));
        Releasables.close(injector.getInstance(Client.class));
        toClose.add(() -> stopWatch.stop().start("indices_cluster"));
        toClose.add(injector.getInstance(IndicesClusterStateService.class));
        toClose.add(() -> stopWatch.stop().start("indices"));
        toClose.add(injector.getInstance(IndicesService.class));
        // close filter/fielddata caches after indices
        toClose.add(injector.getInstance(IndicesStore.class));
        toClose.add(injector.getInstance(PeerRecoverySourceService.class));
        toClose.add(() -> stopWatch.stop().start("cluster"));
        toClose.add(injector.getInstance(ClusterService.class));
        toClose.add(() -> stopWatch.stop().start("node_connections_service"));
        toClose.add(injector.getInstance(NodeConnectionsService.class));
        toClose.add(() -> stopWatch.stop().start("cluster_coordinator"));
        toClose.add(injector.getInstance(Coordinator.class));
        toClose.add(() -> stopWatch.stop().start("monitor"));
        toClose.add(nodeService.getMonitorService());
        toClose.add(() -> stopWatch.stop().start("fsHealth"));
        toClose.add(injector.getInstance(FsHealthService.class));
        toClose.add(() -> stopWatch.stop().start("gateway"));
        toClose.add(injector.getInstance(GatewayService.class));
        toClose.add(() -> stopWatch.stop().start("search"));
        toClose.add(injector.getInstance(SearchService.class));
        toClose.add(() -> stopWatch.stop().start("transport"));
        toClose.add(injector.getInstance(TransportService.class));
        if (ReadinessService.enabled(environment)) {
            toClose.add(injector.getInstance(ReadinessService.class));
        }
        toClose.add(injector.getInstance(FileSettingsService.class));
        toClose.add(injector.getInstance(HealthPeriodicLogger.class));

        for (LifecycleComponent plugin : pluginLifecycleComponents) {
            toClose.add(() -> stopWatch.stop().start("plugin(" + plugin.getClass().getName() + ")"));
            toClose.add(plugin);
        }
        toClose.addAll(pluginsService.filterPlugins(Plugin.class));

        toClose.add(() -> stopWatch.stop().start("script"));
        toClose.add(injector.getInstance(ScriptService.class));

        toClose.add(() -> stopWatch.stop().start("thread_pool"));
        toClose.add(() -> injector.getInstance(ThreadPool.class).shutdown());
        // Don't call shutdownNow here, it might break ongoing operations on Lucene indices.
        // See https://issues.apache.org/jira/browse/LUCENE-7248. We call shutdownNow in
        // awaitClose if the node doesn't finish closing within the specified time.

        toClose.add(() -> stopWatch.stop().start("gateway_meta_state"));
        toClose.add(injector.getInstance(GatewayMetaState.class));

        toClose.add(() -> stopWatch.stop().start("node_environment"));
        toClose.add(injector.getInstance(NodeEnvironment.class));
        toClose.add(stopWatch::stop);

        if (logger.isTraceEnabled()) {
            toClose.add(() -> logger.trace("Close times for each service:\n{}", stopWatch.prettyPrint()));
        }
        IOUtils.close(toClose);
        logger.info("closed");
    }

    /**
     * Invokes hooks to prepare this node to be closed. This should be called when Elasticsearch receives a request to shut down
     * gracefully from the underlying operating system, before system resources are closed. This method will block
     * until the node is ready to shut down.
     *
     * Note that this class is part of infrastructure to react to signals from the operating system - most graceful shutdown
     * logic should use Node Shutdown, see {@link org.elasticsearch.cluster.metadata.NodesShutdownMetadata}.
     */
    public void prepareForClose() {
        HttpServerTransport httpServerTransport = injector.getInstance(HttpServerTransport.class);
        FutureTask<Void> stopper = new FutureTask<>(() -> {
            httpServerTransport.stop();
            return null;
        });
        new Thread(stopper, "http-server-transport-stop").start();

        Optional.ofNullable(terminationHandler.get()).ifPresent(TerminationHandler::handleTermination);

        try {
            stopper.get();
        } catch (Exception e) {
            logger.warn("unexpected exception while waiting for http server to close", e);
        }
    }

    /**
     * Wait for this node to be effectively closed.
     */
    // synchronized to prevent running concurrently with close()
    public synchronized boolean awaitClose(long timeout, TimeUnit timeUnit) throws InterruptedException {
        if (lifecycle.closed() == false) {
            // We don't want to shutdown the threadpool or interrupt threads on a node that is not
            // closed yet.
            throw new IllegalStateException("Call close() first");
        }

        ThreadPool threadPool = injector.getInstance(ThreadPool.class);
        final boolean terminated = ThreadPool.terminate(threadPool, timeout, timeUnit);
        if (terminated) {
            // All threads terminated successfully. Because search, recovery and all other operations
            // that run on shards run in the threadpool, indices should be effectively closed by now.
            if (nodeService.awaitClose(0, TimeUnit.MILLISECONDS) == false) {
                throw new IllegalStateException(
                    "Some shards are still open after the threadpool terminated. "
                        + "Something is leaking index readers or store references."
                );
            }
        }
        return terminated;
    }

    /**
     * Returns {@code true} if the node is closed.
     */
    public boolean isClosed() {
        return lifecycle.closed();
    }

    public Injector injector() {
        return this.injector;
    }

    /**
     * Hook for validating the node after network
     * services are started but before the cluster service is started
     * and before the network service starts accepting incoming network
     * requests.
     *
     * @param context               the bootstrap context for this node
     * @param boundTransportAddress the network addresses the node is
     *                              bound and publishing to
     */
    @SuppressWarnings("unused")
    protected void validateNodeBeforeAcceptingRequests(
        final BootstrapContext context,
        final BoundTransportAddress boundTransportAddress,
        List<BootstrapCheck> bootstrapChecks
    ) throws NodeValidationException {}

    /**
     * Writes a file to the logs dir containing the ports for the given transport type
     */
    private void writePortsFile(String type, BoundTransportAddress boundAddress) {
        Path tmpPortsFile = environment.logsFile().resolve(type + ".ports.tmp");
        try (BufferedWriter writer = Files.newBufferedWriter(tmpPortsFile, Charset.forName("UTF-8"))) {
            for (TransportAddress address : boundAddress.boundAddresses()) {
                InetAddress inetAddress = InetAddress.getByName(address.getAddress());
                writer.write(NetworkAddress.format(new InetSocketAddress(inetAddress, address.getPort())) + "\n");
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write ports file", e);
        }
        Path portsFile = environment.logsFile().resolve(type + ".ports");
        try {
            Files.move(tmpPortsFile, portsFile, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new RuntimeException("Failed to rename ports file", e);
        }
    }

    /**
     * The {@link PluginsService} used to build this node's components.
     */
    protected PluginsService getPluginsService() {
        return pluginsService;
    }

    /**
     * Plugins can provide additional settings for the node, but two plugins
     * cannot provide the same setting.
     * @param pluginMap A map of plugin names to plugin instances
     * @param originalSettings The node's original settings, which silently override any setting provided by the plugins.
     * @return A {@link Settings} with the merged node and plugin settings
     * @throws IllegalArgumentException if two plugins provide the same additional setting key
     */
    static Settings mergePluginSettings(Map<String, Plugin> pluginMap, Settings originalSettings) {
        Map<String, String> foundSettings = new HashMap<>();
        final Settings.Builder builder = Settings.builder();
        for (Map.Entry<String, Plugin> entry : pluginMap.entrySet()) {
            Settings settings = entry.getValue().additionalSettings();
            for (String setting : settings.keySet()) {
                String oldPlugin = foundSettings.put(setting, entry.getKey());
                if (oldPlugin != null) {
                    throw new IllegalArgumentException(
                        "Cannot have additional setting ["
                            + setting
                            + "] "
                            + "in plugin ["
                            + entry.getKey()
                            + "], already added in plugin ["
                            + oldPlugin
                            + "]"
                    );
                }
            }
            builder.put(settings);
        }
        return builder.put(originalSettings).build();
    }

    /**
     * Creates a new {@link CircuitBreakerService} based on the settings provided.
     *
     * @see #BREAKER_TYPE_KEY
     */
    private static CircuitBreakerService createCircuitBreakerService(
        Settings settings,
        List<BreakerSettings> breakerSettings,
        ClusterSettings clusterSettings
    ) {
        String type = BREAKER_TYPE_KEY.get(settings);
        if (type.equals("hierarchy")) {
            return new HierarchyCircuitBreakerService(settings, breakerSettings, clusterSettings);
        } else if (type.equals("none")) {
            return new NoneCircuitBreakerService();
        } else {
            throw new IllegalArgumentException("Unknown circuit breaker type [" + type + "]");
        }
    }

    /**
     * Creates a new {@link BigArrays} instance used for this node.
     * This method can be overwritten by subclasses to change their {@link BigArrays} implementation for instance for testing
     */
    BigArrays createBigArrays(PageCacheRecycler pageCacheRecycler, CircuitBreakerService circuitBreakerService) {
        return new BigArrays(pageCacheRecycler, circuitBreakerService, CircuitBreaker.REQUEST);
    }

    /**
     * Creates a new {@link BigArrays} instance used for this node.
     * This method can be overwritten by subclasses to change their {@link BigArrays} implementation for instance for testing
     */
    PageCacheRecycler createPageCacheRecycler(Settings settings) {
        return new PageCacheRecycler(settings);
    }

    /**
     * Creates a new the SearchService. This method can be overwritten by tests to inject mock implementations.
     */
    protected SearchService newSearchService(
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ScriptService scriptService,
        BigArrays bigArrays,
        FetchPhase fetchPhase,
        ResponseCollectorService responseCollectorService,
        CircuitBreakerService circuitBreakerService,
        ExecutorSelector executorSelector,
        Tracer tracer
    ) {
        return new SearchService(
            clusterService,
            indicesService,
            threadPool,
            scriptService,
            bigArrays,
            fetchPhase,
            responseCollectorService,
            circuitBreakerService,
            executorSelector,
            tracer
        );
    }

    /**
     * Creates a new the ScriptService. This method can be overwritten by tests to inject mock implementations.
     */
    protected ScriptService newScriptService(
        Settings settings,
        Map<String, ScriptEngine> engines,
        Map<String, ScriptContext<?>> contexts,
        LongSupplier timeProvider
    ) {
        return new ScriptService(settings, engines, contexts, timeProvider);
    }

    /**
     * Get Custom Name Resolvers list based on a Discovery Plugins list
     *
     * @param discoveryPlugins Discovery plugins list
     */
    private List<NetworkService.CustomNameResolver> getCustomNameResolvers(List<DiscoveryPlugin> discoveryPlugins) {
        List<NetworkService.CustomNameResolver> customNameResolvers = new ArrayList<>();
        for (DiscoveryPlugin discoveryPlugin : discoveryPlugins) {
            NetworkService.CustomNameResolver customNameResolver = discoveryPlugin.getCustomNameResolver(settings());
            if (customNameResolver != null) {
                customNameResolvers.add(customNameResolver);
            }
        }
        return customNameResolvers;
    }

    /**
     * Constructs a ClusterInfoService which may be mocked for tests.
     */
    protected ClusterInfoService newClusterInfoService(
        Settings settings,
        ClusterService clusterService,
        ThreadPool threadPool,
        NodeClient client
    ) {
        final InternalClusterInfoService service = new InternalClusterInfoService(settings, clusterService, threadPool, client);
        if (DiscoveryNode.isMasterNode(settings)) {
            // listen for state changes (this node starts/stops being the elected master, or new nodes are added)
            clusterService.addListener(service);
        }
        return service;
    }

    /**
     * Constructs a {@link org.elasticsearch.http.HttpServerTransport} which may be mocked for tests.
     */
    protected HttpServerTransport newHttpTransport(NetworkModule networkModule) {
        return networkModule.getHttpServerTransportSupplier().get();
    }

    private static class LocalNodeFactory implements Function<BoundTransportAddress, DiscoveryNode> {
        private final SetOnce<DiscoveryNode> localNode = new SetOnce<>();
        private final String persistentNodeId;
        private final Settings settings;

        private LocalNodeFactory(Settings settings, String persistentNodeId) {
            this.persistentNodeId = persistentNodeId;
            this.settings = settings;
        }

        @Override
        public DiscoveryNode apply(BoundTransportAddress boundTransportAddress) {
            localNode.set(DiscoveryNode.createLocal(settings, boundTransportAddress.publishAddress(), persistentNodeId));
            return localNode.get();
        }

        DiscoveryNode getNode() {
            assert localNode.get() != null;
            return localNode.get();
        }
    }
}
