/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.node;

import org.apache.lucene.util.Constants;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.Build;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.MasterNodeChangePredicate;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.gateway.GatewayModule;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.http.HttpServer;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.store.IndicesStore;
import org.elasticsearch.indices.ttl.IndicesTTLService;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.monitor.MonitorService;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.repositories.RepositoriesModule;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.snapshots.SnapshotShardsService;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.tasks.TaskResultsService;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.tribe.TribeService;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.net.Inet6Address;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A node represent a node within a cluster (<tt>cluster.name</tt>). The {@link #client()} can be used
 * in order to use a {@link Client} to perform actions/operations against the cluster.
 */
public class Node implements Closeable {


    public static final Setting<Boolean> WRITE_PORTS_FIELD_SETTING =
        Setting.boolSetting("node.portsfile", false, Property.NodeScope);
    public static final Setting<Boolean> NODE_DATA_SETTING = Setting.boolSetting("node.data", true, Property.NodeScope);
    public static final Setting<Boolean> NODE_MASTER_SETTING =
        Setting.boolSetting("node.master", true, Property.NodeScope);
    public static final Setting<Boolean> NODE_INGEST_SETTING =
        Setting.boolSetting("node.ingest", true, Property.NodeScope);

    /**
    * controls whether the node is allowed to persist things like metadata to disk
    * Note that this does not control whether the node stores actual indices (see
    * {@link #NODE_DATA_SETTING}). However, if this is false, {@link #NODE_DATA_SETTING}
    * and {@link #NODE_MASTER_SETTING} must also be false.
    *
    */
    public static final Setting<Boolean> NODE_LOCAL_STORAGE_SETTING = Setting.boolSetting("node.local_storage", true, Property.NodeScope);
    public static final Setting<String> NODE_NAME_SETTING = Setting.simpleString("node.name", Property.NodeScope);
    public static final Setting<Settings> NODE_ATTRIBUTES = Setting.groupSetting("node.attr.", Property.NodeScope);
    public static final Setting<String> BREAKER_TYPE_KEY = new Setting<>("indices.breaker.type", "hierarchy", (s) -> {
        switch (s) {
            case "hierarchy":
            case "none":
                return s;
            default:
                throw new IllegalArgumentException("indices.breaker.type must be one of [hierarchy, none] but was: " + s);
        }
    }, Setting.Property.NodeScope);

    /**
     * Adds a default node name to the given setting, if it doesn't already exist
     * @return the given setting if node name is already set, or a new copy with a default node name set.
     */
    public static final Settings addNodeNameIfNeeded(Settings settings, final String nodeId) {
        if (NODE_NAME_SETTING.exists(settings)) {
            return settings;
        }
        return Settings.builder().put(settings).put(NODE_NAME_SETTING.getKey(), nodeId.substring(0, 7)).build();
    }

    private static final String CLIENT_TYPE = "node";
    private final Lifecycle lifecycle = new Lifecycle();
    private final Injector injector;
    private final Settings settings;
    private final Environment environment;
    private final NodeEnvironment nodeEnvironment;
    private final PluginsService pluginsService;
    private final NodeClient client;
    private final Collection<LifecycleComponent> pluginLifecycleComponents;

    /**
     * Constructs a node with the given settings.
     *
     * @param preparedSettings Base settings to configure the node with
     */
    public Node(Settings preparedSettings) {
        this(InternalSettingsPreparer.prepareEnvironment(preparedSettings, null));
    }

    public Node(Environment environment) {
        this(environment, Collections.emptyList());
    }

    protected Node(final Environment environment, Collection<Class<? extends Plugin>> classpathPlugins) {
        final List<Closeable> resourcesToClose = new ArrayList<>(); // register everything we need to release in the case of an error
        boolean success = false;
        {
            // use temp logger just to say we are starting. we can't use it later on because the node name might not be set
            ESLogger logger = Loggers.getLogger(Node.class, NODE_NAME_SETTING.get(environment.settings()));
            logger.info("initializing ...");

        }
        try {
            Settings tmpSettings = Settings.builder().put(environment.settings())
                .put(Client.CLIENT_TYPE_SETTING_S.getKey(), CLIENT_TYPE).build();

            tmpSettings = TribeService.processSettings(tmpSettings);

            // create the node environment as soon as possible, to recover the node id and enable logging
            try {
                nodeEnvironment = new NodeEnvironment(tmpSettings, environment);
                resourcesToClose.add(nodeEnvironment);
            } catch (IOException ex) {
                throw new IllegalStateException("Failed to created node environment", ex);
            }

            final boolean hadPredefinedNodeName = NODE_NAME_SETTING.exists(tmpSettings);
                tmpSettings = addNodeNameIfNeeded(tmpSettings, nodeEnvironment.nodeId());
            ESLogger logger = Loggers.getLogger(Node.class, tmpSettings);
            if (hadPredefinedNodeName == false) {
                logger.info("node name [{}] derived from node ID; set [{}] to override",
                    NODE_NAME_SETTING.get(tmpSettings), NODE_NAME_SETTING.getKey());
            }

            final String displayVersion = Version.CURRENT + (Build.CURRENT.isSnapshot() ? "-SNAPSHOT" : "");
            final JvmInfo jvmInfo = JvmInfo.jvmInfo();
            logger.info(
                "version[{}], pid[{}], build[{}/{}], OS[{}/{}/{}], JVM[{}/{}/{}/{}]",
                displayVersion,
                jvmInfo.pid(),
                Build.CURRENT.shortHash(),
                Build.CURRENT.date(),
                Constants.OS_NAME,
                Constants.OS_VERSION,
                Constants.OS_ARCH,
                Constants.JVM_VENDOR,
                Constants.JVM_NAME,
                Constants.JAVA_VERSION,
                Constants.JVM_VERSION);


            if (logger.isDebugEnabled()) {
                logger.debug("using config [{}], data [{}], logs [{}], plugins [{}]",
                    environment.configFile(), Arrays.toString(environment.dataFiles()), environment.logsFile(), environment.pluginsFile());
            }
            // TODO: Remove this in Elasticsearch 6.0.0
            if (JsonXContent.unquotedFieldNamesSet) {
                DeprecationLogger dLogger = new DeprecationLogger(logger);
                dLogger.deprecated("[{}] has been set, but will be removed in Elasticsearch 6.0.0",
                    JsonXContent.JSON_ALLOW_UNQUOTED_FIELD_NAMES);
            }

            this.pluginsService = new PluginsService(tmpSettings, environment.modulesFile(), environment.pluginsFile(), classpathPlugins);
            this.settings = pluginsService.updatedSettings();
            // create the environment based on the finalized (processed) view of the settings
            // this is just to makes sure that people get the same settings, no matter where they ask them from
            this.environment = new Environment(this.settings);
            Environment.assertEquivalent(environment, this.environment);


            final List<ExecutorBuilder<?>> executorBuilders = pluginsService.getExecutorBuilders(settings);

            final ThreadPool threadPool = new ThreadPool(settings, executorBuilders.toArray(new ExecutorBuilder[0]));
            resourcesToClose.add(() -> ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));
            // adds the context to the DeprecationLogger so that it does not need to be injected everywhere
            DeprecationLogger.setThreadContext(threadPool.getThreadContext());
            resourcesToClose.add(() -> DeprecationLogger.removeThreadContext(threadPool.getThreadContext()));

            final List<Setting<?>> additionalSettings = new ArrayList<>();
            final List<String> additionalSettingsFilter = new ArrayList<>();
            additionalSettings.addAll(pluginsService.getPluginSettings());
            additionalSettingsFilter.addAll(pluginsService.getPluginSettingsFilter());
            for (final ExecutorBuilder<?> builder : threadPool.builders()) {
                additionalSettings.addAll(builder.getRegisteredSettings());
            }
            final ResourceWatcherService resourceWatcherService = new ResourceWatcherService(settings, threadPool);
            final ScriptModule scriptModule = ScriptModule.create(settings, this.environment, resourceWatcherService,
                pluginsService.filterPlugins(ScriptPlugin.class));
            AnalysisModule analysisModule = new AnalysisModule(this.environment, pluginsService.filterPlugins(AnalysisPlugin.class));
            additionalSettings.addAll(scriptModule.getSettings());
            // this is as early as we can validate settings at this point. we already pass them to ScriptModule as well as ThreadPool
            // so we might be late here already
            final SettingsModule settingsModule = new SettingsModule(this.settings, additionalSettings, additionalSettingsFilter);
            scriptModule.registerClusterSettingsListeners(settingsModule.getClusterSettings());
            resourcesToClose.add(resourceWatcherService);
            final NetworkService networkService = new NetworkService(settings,
                getCustomNameResolvers(pluginsService.filterPlugins(DiscoveryPlugin.class)));
            final ClusterService clusterService = new ClusterService(settings, settingsModule.getClusterSettings(), threadPool);
            clusterService.add(scriptModule.getScriptService());
            resourcesToClose.add(clusterService);
            final TribeService tribeService = new TribeService(settings, clusterService, nodeEnvironment.nodeId());
            resourcesToClose.add(tribeService);
            final IngestService ingestService = new IngestService(settings, threadPool, this.environment,
                scriptModule.getScriptService(), pluginsService.filterPlugins(IngestPlugin.class));

            ModulesBuilder modules = new ModulesBuilder();
            // plugin modules must be added here, before others or we can get crazy injection errors...
            for (Module pluginModule : pluginsService.createGuiceModules()) {
                modules.add(pluginModule);
            }
            final MonitorService monitorService = new MonitorService(settings, nodeEnvironment, threadPool);
            modules.add(new NodeModule(this, monitorService));
            NetworkModule networkModule = new NetworkModule(networkService, settings, false);
            modules.add(networkModule);
            modules.add(new DiscoveryModule(this.settings));
            ClusterModule clusterModule = new ClusterModule(settings, clusterService);
            modules.add(clusterModule);
            IndicesModule indicesModule = new IndicesModule(pluginsService.filterPlugins(MapperPlugin.class));
            modules.add(indicesModule);
            SearchModule searchModule = new SearchModule(settings, false, pluginsService.filterPlugins(SearchPlugin.class));
            modules.add(searchModule);
            modules.add(new ActionModule(DiscoveryNode.isIngestNode(settings), false, settings,
                clusterModule.getIndexNameExpressionResolver(), settingsModule.getClusterSettings(),
                pluginsService.filterPlugins(ActionPlugin.class)));
            modules.add(new GatewayModule());
            modules.add(new RepositoriesModule(this.environment, pluginsService.filterPlugins(RepositoryPlugin.class)));
            pluginsService.processModules(modules);
            CircuitBreakerService circuitBreakerService = createCircuitBreakerService(settingsModule.getSettings(),
                settingsModule.getClusterSettings());
            resourcesToClose.add(circuitBreakerService);
            BigArrays bigArrays = createBigArrays(settings, circuitBreakerService);
            resourcesToClose.add(bigArrays);
            modules.add(settingsModule);
            List<NamedWriteableRegistry.Entry> namedWriteables = Stream.of(
                networkModule.getNamedWriteables().stream(),
                indicesModule.getNamedWriteables().stream(),
                searchModule.getNamedWriteables().stream(),
                pluginsService.filterPlugins(Plugin.class).stream()
                    .flatMap(p -> p.getNamedWriteables().stream()))
                .flatMap(Function.identity()).collect(Collectors.toList());
            final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
            client = new NodeClient(settings, threadPool);
            Collection<Object> pluginComponents = pluginsService.filterPlugins(Plugin.class).stream()
                .flatMap(p -> p.createComponents(client, clusterService, threadPool, resourceWatcherService,
                                                 scriptModule.getScriptService(), searchModule.getSearchRequestParsers()).stream())
                .collect(Collectors.toList());
            modules.add(b -> {
                    b.bind(PluginsService.class).toInstance(pluginsService);
                    b.bind(Client.class).toInstance(client);
                    b.bind(NodeClient.class).toInstance(client);
                    b.bind(Environment.class).toInstance(this.environment);
                    b.bind(ThreadPool.class).toInstance(threadPool);
                    b.bind(NodeEnvironment.class).toInstance(nodeEnvironment);
                    b.bind(TribeService.class).toInstance(tribeService);
                    b.bind(ResourceWatcherService.class).toInstance(resourceWatcherService);
                    b.bind(CircuitBreakerService.class).toInstance(circuitBreakerService);
                    b.bind(BigArrays.class).toInstance(bigArrays);
                    b.bind(ScriptService.class).toInstance(scriptModule.getScriptService());
                    b.bind(AnalysisRegistry.class).toInstance(analysisModule.getAnalysisRegistry());
                    b.bind(IngestService.class).toInstance(ingestService);
                    b.bind(NamedWriteableRegistry.class).toInstance(namedWriteableRegistry);
                    Class<? extends SearchService> searchServiceImpl = pickSearchServiceImplementation();
                    if (searchServiceImpl == SearchService.class) {
                        b.bind(SearchService.class).asEagerSingleton();
                    } else {
                        b.bind(SearchService.class).to(searchServiceImpl).asEagerSingleton();
                    }
                    pluginComponents.stream().forEach(p -> b.bind((Class) p.getClass()).toInstance(p));
                }
            );
            injector = modules.createInjector();

            List<LifecycleComponent> pluginLifecycleComponents = pluginComponents.stream()
                .filter(p -> p instanceof LifecycleComponent)
                .map(p -> (LifecycleComponent) p).collect(Collectors.toList());
            pluginLifecycleComponents.addAll(pluginsService.getGuiceServiceClasses().stream()
                .map(injector::getInstance).collect(Collectors.toList()));
            resourcesToClose.addAll(pluginLifecycleComponents);
            this.pluginLifecycleComponents = Collections.unmodifiableList(pluginLifecycleComponents);

            client.intialize(injector.getInstance(new Key<Map<GenericAction, TransportAction>>() {}));

            logger.info("initialized");

            success = true;
        } catch (IOException ex) {
            throw new ElasticsearchException("failed to bind service", ex);
        } finally {
            if (!success) {
                IOUtils.closeWhileHandlingException(resourcesToClose);
            }
        }
    }

    /**
     * The settings that were used to create the node.
     */
    public Settings settings() {
        return this.settings;
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
    public Node start() {
        if (!lifecycle.moveToStarted()) {
            return this;
        }

        ESLogger logger = Loggers.getLogger(Node.class, NODE_NAME_SETTING.get(settings));
        logger.info("starting ...");
        // hack around dependency injection problem (for now...)
        injector.getInstance(Discovery.class).setAllocationService(injector.getInstance(AllocationService.class));
        pluginLifecycleComponents.forEach(LifecycleComponent::start);

        injector.getInstance(MappingUpdatedAction.class).setClient(client);
        injector.getInstance(IndicesService.class).start();
        injector.getInstance(IndicesClusterStateService.class).start();
        injector.getInstance(IndicesTTLService.class).start();
        injector.getInstance(SnapshotsService.class).start();
        injector.getInstance(SnapshotShardsService.class).start();
        injector.getInstance(RoutingService.class).start();
        injector.getInstance(SearchService.class).start();
        injector.getInstance(MonitorService.class).start();
        injector.getInstance(RestController.class).start();

        final ClusterService clusterService = injector.getInstance(ClusterService.class);

        final NodeConnectionsService nodeConnectionsService = injector.getInstance(NodeConnectionsService.class);
        nodeConnectionsService.start();
        clusterService.setNodeConnectionsService(nodeConnectionsService);

        // TODO hack around circular dependencies problems
        injector.getInstance(GatewayAllocator.class).setReallocation(clusterService, injector.getInstance(RoutingService.class));

        injector.getInstance(ResourceWatcherService.class).start();
        injector.getInstance(GatewayService.class).start();
        Discovery discovery = injector.getInstance(Discovery.class);
        clusterService.addInitialStateBlock(discovery.getDiscoverySettings().getNoMasterBlock());
        clusterService.setClusterStatePublisher(discovery::publish);

        // start before the cluster service since it adds/removes initial Cluster state blocks
        final TribeService tribeService = injector.getInstance(TribeService.class);
        tribeService.start();

        // Start the transport service now so the publish address will be added to the local disco node in ClusterService
        TransportService transportService = injector.getInstance(TransportService.class);
        transportService.getTaskManager().setTaskResultsService(injector.getInstance(TaskResultsService.class));
        transportService.start();

        validateNodeBeforeAcceptingRequests(settings, transportService.boundAddress());

        DiscoveryNode localNode = DiscoveryNode.createLocal(settings,
            transportService.boundAddress().publishAddress(), injector.getInstance(NodeEnvironment.class).nodeId());

        // TODO: need to find a cleaner way to start/construct a service with some initial parameters,
        // playing nice with the life cycle interfaces
        clusterService.setLocalNode(localNode);
        transportService.setLocalNode(localNode);
        clusterService.add(transportService.getTaskManager());

        clusterService.start();

        // start after cluster service so the local disco is known
        discovery.start();
        transportService.acceptIncomingRequests();
        discovery.startInitialJoin();
        // tribe nodes don't have a master so we shouldn't register an observer
        if (DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING.get(settings).millis() > 0) {
            final ThreadPool thread = injector.getInstance(ThreadPool.class);
            ClusterStateObserver observer = new ClusterStateObserver(clusterService, null, logger, thread.getThreadContext());
            if (observer.observedState().nodes().getMasterNodeId() == null) {
                final CountDownLatch latch = new CountDownLatch(1);
                observer.waitForNextChange(new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) { latch.countDown(); }

                    @Override
                    public void onClusterServiceClose() {
                        latch.countDown();
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        logger.warn("timed out while waiting for initial discovery state - timeout: {}",
                            DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING.get(settings));
                        latch.countDown();
                    }
                }, MasterNodeChangePredicate.INSTANCE, DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING.get(settings));

                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new ElasticsearchTimeoutException("Interrupted while waiting for initial discovery state");
                }
            }
        }

        if (NetworkModule.HTTP_ENABLED.get(settings)) {
            injector.getInstance(HttpServer.class).start();
        }

        // start nodes now, after the http server, because it may take some time
        tribeService.startNodes();


        if (WRITE_PORTS_FIELD_SETTING.get(settings)) {
            if (NetworkModule.HTTP_ENABLED.get(settings)) {
                HttpServerTransport http = injector.getInstance(HttpServerTransport.class);
                writePortsFile("http", http.boundAddress());
            }
            TransportService transport = injector.getInstance(TransportService.class);
            writePortsFile("transport", transport.boundAddress());
        }

        logger.info("started");

        return this;
    }

    private Node stop() {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        ESLogger logger = Loggers.getLogger(Node.class, NODE_NAME_SETTING.get(settings));
        logger.info("stopping ...");

        injector.getInstance(TribeService.class).stop();
        injector.getInstance(ResourceWatcherService.class).stop();
        if (NetworkModule.HTTP_ENABLED.get(settings)) {
            injector.getInstance(HttpServer.class).stop();
        }

        injector.getInstance(SnapshotsService.class).stop();
        injector.getInstance(SnapshotShardsService.class).stop();
        // stop any changes happening as a result of cluster state changes
        injector.getInstance(IndicesClusterStateService.class).stop();
        // we close indices first, so operations won't be allowed on it
        injector.getInstance(IndicesTTLService.class).stop();
        injector.getInstance(RoutingService.class).stop();
        injector.getInstance(ClusterService.class).stop();
        injector.getInstance(Discovery.class).stop();
        injector.getInstance(NodeConnectionsService.class).stop();
        injector.getInstance(MonitorService.class).stop();
        injector.getInstance(GatewayService.class).stop();
        injector.getInstance(SearchService.class).stop();
        injector.getInstance(RestController.class).stop();
        injector.getInstance(TransportService.class).stop();

        pluginLifecycleComponents.forEach(LifecycleComponent::stop);
        // we should stop this last since it waits for resources to get released
        // if we had scroll searchers etc or recovery going on we wait for to finish.
        injector.getInstance(IndicesService.class).stop();
        logger.info("stopped");

        return this;
    }

    // During concurrent close() calls we want to make sure that all of them return after the node has completed it's shutdown cycle.
    // If not, the hook that is added in Bootstrap#setup() will be useless: close() might not be executed, in case another (for example api) call
    // to close() has already set some lifecycles to stopped. In this case the process will be terminated even if the first call to close() has not finished yet.
    @Override
    public synchronized void close() throws IOException {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.moveToClosed()) {
            return;
        }

        ESLogger logger = Loggers.getLogger(Node.class, NODE_NAME_SETTING.get(settings));
        logger.info("closing ...");
        List<Closeable> toClose = new ArrayList<>();
        StopWatch stopWatch = new StopWatch("node_close");
        toClose.add(() -> stopWatch.start("tribe"));
        toClose.add(injector.getInstance(TribeService.class));
        toClose.add(() -> stopWatch.stop().start("node_service"));
        toClose.add(injector.getInstance(NodeService.class));
        toClose.add(() -> stopWatch.stop().start("http"));
        if (NetworkModule.HTTP_ENABLED.get(settings)) {
            toClose.add(injector.getInstance(HttpServer.class));
        }
        toClose.add(() -> stopWatch.stop().start("snapshot_service"));
        toClose.add(injector.getInstance(SnapshotsService.class));
        toClose.add(injector.getInstance(SnapshotShardsService.class));
        toClose.add(() -> stopWatch.stop().start("client"));
        Releasables.close(injector.getInstance(Client.class));
        toClose.add(() -> stopWatch.stop().start("indices_cluster"));
        toClose.add(injector.getInstance(IndicesClusterStateService.class));
        toClose.add(() -> stopWatch.stop().start("indices"));
        toClose.add(injector.getInstance(IndicesTTLService.class));
        toClose.add(injector.getInstance(IndicesService.class));
        // close filter/fielddata caches after indices
        toClose.add(injector.getInstance(IndicesStore.class));
        toClose.add(() -> stopWatch.stop().start("routing"));
        toClose.add(injector.getInstance(RoutingService.class));
        toClose.add(() -> stopWatch.stop().start("cluster"));
        toClose.add(injector.getInstance(ClusterService.class));
        toClose.add(() -> stopWatch.stop().start("node_connections_service"));
        toClose.add(injector.getInstance(NodeConnectionsService.class));
        toClose.add(() -> stopWatch.stop().start("discovery"));
        toClose.add(injector.getInstance(Discovery.class));
        toClose.add(() -> stopWatch.stop().start("monitor"));
        toClose.add(injector.getInstance(MonitorService.class));
        toClose.add(() -> stopWatch.stop().start("gateway"));
        toClose.add(injector.getInstance(GatewayService.class));
        toClose.add(() -> stopWatch.stop().start("search"));
        toClose.add(injector.getInstance(SearchService.class));
        toClose.add(() -> stopWatch.stop().start("rest"));
        toClose.add(injector.getInstance(RestController.class));
        toClose.add(() -> stopWatch.stop().start("transport"));
        toClose.add(injector.getInstance(TransportService.class));

        for (LifecycleComponent plugin : pluginLifecycleComponents) {
            toClose.add(() -> stopWatch.stop().start("plugin(" + plugin.getClass().getName() + ")"));
            toClose.add(plugin);
        }
        toClose.addAll(pluginsService.filterPlugins(Closeable.class));

        toClose.add(() -> stopWatch.stop().start("script"));
        toClose.add(injector.getInstance(ScriptService.class));

        toClose.add(() -> stopWatch.stop().start("thread_pool"));
        // TODO this should really use ThreadPool.terminate()
        toClose.add(() -> injector.getInstance(ThreadPool.class).shutdown());
        toClose.add(() -> {
            try {
                injector.getInstance(ThreadPool.class).awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // ignore
            }
        });

        toClose.add(() -> stopWatch.stop().start("thread_pool_force_shutdown"));
        toClose.add(() -> injector.getInstance(ThreadPool.class).shutdownNow());
        toClose.add(() -> stopWatch.stop());


        toClose.add(injector.getInstance(NodeEnvironment.class));
        toClose.add(injector.getInstance(BigArrays.class));

        if (logger.isTraceEnabled()) {
            logger.trace("Close times for each service:\n{}", stopWatch.prettyPrint());
        }
        IOUtils.close(toClose);
        logger.info("closed");
    }


    /**
     * Returns <tt>true</tt> if the node is closed.
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
     * @param settings              the fully-resolved settings
     * @param boundTransportAddress the network addresses the node is
     *                              bound and publishing to
     */
    @SuppressWarnings("unused")
    protected void validateNodeBeforeAcceptingRequests(Settings settings, BoundTransportAddress boundTransportAddress) {
    }

    /** Writes a file to the logs dir containing the ports for the given transport type */
    private void writePortsFile(String type, BoundTransportAddress boundAddress) {
        Path tmpPortsFile = environment.logsFile().resolve(type + ".ports.tmp");
        try (BufferedWriter writer = Files.newBufferedWriter(tmpPortsFile, Charset.forName("UTF-8"))) {
            for (TransportAddress address : boundAddress.boundAddresses()) {
                InetAddress inetAddress = InetAddress.getByName(address.getAddress());
                if (inetAddress instanceof Inet6Address && inetAddress.isLinkLocalAddress()) {
                    // no link local, just causes problems
                    continue;
                }
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
     * Creates a new {@link CircuitBreakerService} based on the settings provided.
     * @see #BREAKER_TYPE_KEY
     */
    public static CircuitBreakerService createCircuitBreakerService(Settings settings, ClusterSettings clusterSettings) {
        String type = BREAKER_TYPE_KEY.get(settings);
        if (type.equals("hierarchy")) {
            return new HierarchyCircuitBreakerService(settings, clusterSettings);
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
    BigArrays createBigArrays(Settings settings, CircuitBreakerService circuitBreakerService) {
        return new BigArrays(settings, circuitBreakerService);
    }

    /**
     * Select the search service implementation. Overrided by tests.
     */
    protected Class<? extends SearchService> pickSearchServiceImplementation() {
        return SearchService.class;
    }

    /**
     * Get Custom Name Resolvers list based on a Discovery Plugins list
     * @param discoveryPlugins Discovery plugins list
     */
    private List<NetworkService.CustomNameResolver> getCustomNameResolvers(List<DiscoveryPlugin> discoveryPlugins) {
        List<NetworkService.CustomNameResolver> customNameResolvers = new ArrayList<>();
        for (DiscoveryPlugin discoveryPlugin : discoveryPlugins) {
            NetworkService.CustomNameResolver customNameResolver = discoveryPlugin.getCustomNameResolver(settings);
            if (customNameResolver != null) {
                customNameResolvers.add(customNameResolver);
            }
        }
        return customNameResolvers;
    }
}
