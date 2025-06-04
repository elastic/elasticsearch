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
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.exception.ElasticsearchTimeoutException;
import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.coordination.CoordinationDiagnosticsService;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.metadata.IndexMetadataVerifier;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.NodeAndClusterIdStateListener;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.BuildVersion;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.NodeMetadata;
import org.elasticsearch.gateway.GatewayMetaState;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.gateway.MetaStateService;
import org.elasticsearch.gateway.PersistedClusterStateService;
import org.elasticsearch.health.HealthPeriodicLogger;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.recovery.PeerRecoverySourceService;
import org.elasticsearch.indices.store.IndicesStore;
import org.elasticsearch.injection.guice.Injector;
import org.elasticsearch.monitor.fs.FsHealthService;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.metrics.IndicesMetrics;
import org.elasticsearch.monitor.metrics.NodeMetrics;
import org.elasticsearch.node.internal.TerminationHandler;
import org.elasticsearch.plugins.ClusterCoordinationPlugin;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.MetadataUpgrader;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsLoader;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.readiness.ReadinessService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.reservedstate.service.FileSettingsService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.snapshots.SnapshotShardsService;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.tasks.TaskCancellationService;
import org.elasticsearch.tasks.TaskResultsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterPortSettings;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;

import javax.net.ssl.SNIHostName;

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
    private final Injector injector;
    private final Environment environment;
    private final NodeEnvironment nodeEnvironment;
    private final PluginsService pluginsService;
    private final NodeClient client;
    private final Collection<LifecycleComponent> pluginLifecycleComponents;
    private final LocalNodeFactory localNodeFactory;
    private final NodeService nodeService;
    private final TerminationHandler terminationHandler;

    // for testing
    final NamedWriteableRegistry namedWriteableRegistry;
    final NamedXContentRegistry namedXContentRegistry;

    /**
     * Constructs a node
     *
     * @param environment         the initial environment for this node, which will be added to by plugins
     */
    public Node(Environment environment, PluginsLoader pluginsLoader) {
        this(NodeConstruction.prepareConstruction(environment, pluginsLoader, new NodeServiceProvider(), true));
    }

    /**
     * Constructs a node using information from {@code construction}
     */
    Node(NodeConstruction construction) {
        injector = construction.injector();
        environment = construction.environment();
        nodeEnvironment = construction.nodeEnvironment();
        pluginsService = construction.pluginsService();
        client = construction.client();
        pluginLifecycleComponents = construction.pluginLifecycleComponents();
        localNodeFactory = construction.localNodeFactory();
        nodeService = construction.nodeService();
        terminationHandler = construction.terminationHandler();
        namedWriteableRegistry = construction.namedWriteableRegistry();
        namedXContentRegistry = construction.namedXContentRegistry();
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
            pluginsService.filterPlugins(ClusterCoordinationPlugin.class).toList(),
            injector.getInstance(CompatibilityVersions.class)
        );
        // TODO: Do not expect that the legacy metadata file is always present https://github.com/elastic/elasticsearch/issues/95211
        if (Assertions.ENABLED && DiscoveryNode.isStateless(settings()) == false) {
            try {
                final NodeMetadata nodeMetadata = NodeMetadata.FORMAT.loadLatestState(
                    logger,
                    NamedXContentRegistry.EMPTY,
                    nodeEnvironment.nodeDataPaths()
                );
                assert nodeMetadata != null;
                assert nodeMetadata.nodeVersion().equals(BuildVersion.current());
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
                        logger.warn(
                            "timed out after [{}={}] while waiting for initial discovery state; for troubleshooting guidance see [{}]",
                            INITIAL_STATE_TIMEOUT_SETTING.getKey(),
                            initialStateTimeout,
                            ReferenceDocs.DISCOVERY_TROUBLESHOOTING
                        );
                        latch.countDown();
                    }
                }, state -> state.nodes().getMasterNodeId() != null, initialStateTimeout);

                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
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

        injector.getInstance(NodeMetrics.class).start();
        injector.getInstance(IndicesMetrics.class).start();
        injector.getInstance(HealthPeriodicLogger.class).start();

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

    private void stop() {
        if (lifecycle.moveToStopped() == false) {
            return;
        }
        logger.info("stopping ...");

        if (ReadinessService.enabled(environment)) {
            stopIfStarted(ReadinessService.class);
        }
        // We stop the health periodic logger first since certain checks won't be possible anyway
        stopIfStarted(HealthPeriodicLogger.class);
        stopIfStarted(FileSettingsService.class);
        injector.getInstance(ResourceWatcherService.class).close();
        stopIfStarted(HttpServerTransport.class);

        stopIfStarted(SnapshotsService.class);
        stopIfStarted(SnapshotShardsService.class);
        stopIfStarted(RepositoriesService.class);
        // stop any changes happening as a result of cluster state changes
        stopIfStarted(IndicesClusterStateService.class);
        // close cluster coordinator early to not react to pings anymore.
        // This can confuse other nodes and delay things - mostly if we're the master and we're running tests.
        stopIfStarted(Coordinator.class);
        // we close indices first, so operations won't be allowed on it
        stopIfStarted(ClusterService.class);
        stopIfStarted(NodeConnectionsService.class);
        stopIfStarted(FsHealthService.class);
        stopIfStarted(nodeService.getMonitorService());
        stopIfStarted(GatewayService.class);
        stopIfStarted(SearchService.class);
        stopIfStarted(TransportService.class);
        stopIfStarted(NodeMetrics.class);
        stopIfStarted(IndicesMetrics.class);

        pluginLifecycleComponents.forEach(Node::stopIfStarted);
        // we should stop this last since it waits for resources to get released
        // if we had scroll searchers etc or recovery going on we wait for to finish.
        stopIfStarted(IndicesService.class);
        logger.info("stopped");
    }

    private <T extends LifecycleComponent> void stopIfStarted(Class<T> componentClass) {
        stopIfStarted(injector.getInstance(componentClass));
    }

    private static void stopIfStarted(LifecycleComponent component) {
        // if we failed during startup then some of our components might not have started yet
        if (component.lifecycleState() == Lifecycle.State.STARTED) {
            component.stop();
        }
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
        toClose.add(injector.getInstance(NodeMetrics.class));
        toClose.add(injector.getInstance(IndicesMetrics.class));
        if (ReadinessService.enabled(environment)) {
            toClose.add(injector.getInstance(ReadinessService.class));
        }
        toClose.add(injector.getInstance(FileSettingsService.class));
        toClose.add(injector.getInstance(HealthPeriodicLogger.class));

        for (LifecycleComponent plugin : pluginLifecycleComponents) {
            toClose.add(() -> stopWatch.stop().start("plugin(" + plugin.getClass().getName() + ")"));
            toClose.add(plugin);
        }
        pluginsService.filterPlugins(Plugin.class).forEach(toClose::add);

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
     * <p>
     * Note that this class is part of infrastructure to react to signals from the operating system - most graceful shutdown
     * logic should use Node Shutdown, see {@link org.elasticsearch.cluster.metadata.NodesShutdownMetadata}.
     */
    public void prepareForClose() {
        injector.getInstance(ShutdownPrepareService.class)
            .prepareForShutdown(injector.getInstance(TransportService.class).getTaskManager());
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
        Path tmpPortsFile = environment.logsDir().resolve(type + ".ports.tmp");
        try (BufferedWriter writer = Files.newBufferedWriter(tmpPortsFile, StandardCharsets.UTF_8)) {
            for (TransportAddress address : boundAddress.boundAddresses()) {
                InetAddress inetAddress = InetAddress.getByName(address.getAddress());
                writer.write(NetworkAddress.format(new InetSocketAddress(inetAddress, address.getPort())) + "\n");
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write ports file", e);
        }
        Path portsFile = environment.logsDir().resolve(type + ".ports");
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

    static class LocalNodeFactory implements Function<BoundTransportAddress, DiscoveryNode> {
        private final SetOnce<DiscoveryNode> localNode = new SetOnce<>();
        private final String persistentNodeId;
        private final Settings settings;

        LocalNodeFactory(Settings settings, String persistentNodeId) {
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
