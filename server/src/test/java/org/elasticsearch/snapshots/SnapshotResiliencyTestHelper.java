/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.RequestValidators;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.admin.cluster.repositories.cleanup.TransportCleanupRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.snapshots.clone.TransportCloneSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.TransportCreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.delete.TransportDeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.TransportGetSnapshotsAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.TransportRestoreSnapshotAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.TransportClusterStateAction;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.mapping.put.TransportAutoPutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.admin.indices.shards.TransportIndicesShardStoresAction;
import org.elasticsearch.action.bulk.FailureStoreMetrics;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.resync.TransportResyncReplicationAction;
import org.elasticsearch.action.search.OnlinePrewarmingService;
import org.elasticsearch.action.search.SearchExecutionStatsCollector;
import org.elasticsearch.action.search.SearchPhaseController;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.coordination.ClusterBootstrapService;
import org.elasticsearch.cluster.coordination.CoordinationState;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.ElectionStrategy;
import org.elasticsearch.cluster.coordination.InMemoryPersistedState;
import org.elasticsearch.cluster.coordination.LeaderHeartbeatService;
import org.elasticsearch.cluster.coordination.PreVoteCollector;
import org.elasticsearch.cluster.coordination.Reconfigurator;
import org.elasticsearch.cluster.coordination.StatefulPreVoteCollector;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamFailureStoreSettings;
import org.elasticsearch.cluster.metadata.IndexMetadataVerifier;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataDeleteIndexService;
import org.elasticsearch.cluster.metadata.MetadataMappingService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.BatchedRerouteService;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.FakeThreadPoolMasterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.cluster.version.CompatibilityVersionsUtils;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.logging.activity.ActivityLogWriterProvider;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.gateway.MetaStateService;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettingProviders;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.engine.MergeMetrics;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.MapperRegistry;
import org.elasticsearch.index.seqno.GlobalCheckpointSyncAction;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.IndicesServiceBuilder;
import org.elasticsearch.indices.IndicesServiceTests;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.recovery.PeerRecoverySourceService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.recovery.SnapshotFilesProvider;
import org.elasticsearch.indices.recovery.plan.PeerOnlyRecoveryPlannerService;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.SamplingService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.monitor.StatusInfo;
import org.elasticsearch.node.ResponseCollectorService;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.repositories.LocalPrimarySnapshotShardContextFactory;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.SnapshotMetrics;
import org.elasticsearch.repositories.SnapshotShardContextFactory;
import org.elasticsearch.repositories.VerifyNodeRepositoryAction;
import org.elasticsearch.repositories.VerifyNodeRepositoryCoordinationAction;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.reservedstate.service.FileSettingsService;
import org.elasticsearch.rest.action.search.SearchResponseMetrics;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.transport.DisruptableMockTransport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.junit.Assert;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.env.Environment.PATH_HOME_SETTING;
import static org.elasticsearch.monitor.StatusInfo.Status.HEALTHY;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomLongBetween;
import static org.mockito.Mockito.mock;

public class SnapshotResiliencyTestHelper {

    /**
     * Create a {@link Environment} with random path.home and path.repo
     **/
    private static Environment createEnvironment(String nodeName, Path tempDir, Settings extraSettings) {
        return TestEnvironment.newEnvironment(
            Settings.builder()
                .put(NODE_NAME_SETTING.getKey(), nodeName)
                .put(PATH_HOME_SETTING.getKey(), tempDir.resolve(nodeName).toAbsolutePath())
                .put(Environment.PATH_REPO_SETTING.getKey(), tempDir.resolve("repo").toAbsolutePath())
                .putList(
                    ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING.getKey(),
                    ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING.get(Settings.EMPTY)
                )
                .put(MappingUpdatedAction.INDICES_MAX_IN_FLIGHT_UPDATES_SETTING.getKey(), 1000) // o.w. some tests might block
                .put(extraSettings)
                .build()
        );
    }

    private static ClusterState stateForNode(ClusterState state, DiscoveryNode node) {
        // Remove and add back local node to update ephemeral id on restarts
        return ClusterState.builder(state)
            .nodes(DiscoveryNodes.builder(state.nodes()).remove(node.getId()).add(node).localNodeId(node.getId()))
            .build();
    }

    public static class TestClusterNodes {

        protected static final Logger logger = LogManager.getLogger(TestClusterNodes.class);

        protected final Path tempDir;

        protected final DeterministicTaskQueue deterministicTaskQueue;

        protected final Consumer<String[]> warningConsumer;

        // LinkedHashMap so we have deterministic ordering when iterating over the map in tests
        protected final Map<String, TestClusterNode> nodes = new LinkedHashMap<>();

        /**
         * Node names that are disconnected from all other nodes.
         */
        protected final Set<String> disconnectedNodes = new HashSet<>();

        @SuppressWarnings("this-escape")
        public TestClusterNodes(
            int masterNodes,
            int dataNodes,
            Path tempDir,
            DeterministicTaskQueue deterministicTaskQueue,
            TransportInterceptorFactory transportInterceptorFactory,
            Consumer<String[]> warningConsumer
        ) {
            this.tempDir = tempDir;
            this.deterministicTaskQueue = deterministicTaskQueue;
            this.warningConsumer = warningConsumer;
            for (int i = 0; i < masterNodes; ++i) {
                nodes.computeIfAbsent("node" + i, nodeName -> {
                    try {
                        return newMasterNode(nodeName, transportInterceptorFactory);
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                });
            }
            for (int i = 0; i < dataNodes; ++i) {
                nodes.computeIfAbsent("data-node" + i, nodeName -> {
                    try {
                        return newDataNode(nodeName, transportInterceptorFactory);
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                });
            }
        }

        public Map<String, TestClusterNode> nodes() {
            return nodes;
        }

        public Set<String> disconnectedNodes() {
            return disconnectedNodes;
        }

        public TestClusterNode nodeById(final String nodeId) {
            return nodes.values()
                .stream()
                .filter(n -> n.node.getId().equals(nodeId))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Could not find node by id [" + nodeId + ']'));
        }

        protected DiscoveryNodeRole dataNodeRole() {
            return DiscoveryNodeRole.DATA_ROLE;
        }

        private TestClusterNode newMasterNode(String nodeName, TransportInterceptorFactory transportInterceptorFactory) throws IOException {
            return newNode(nodeName, DiscoveryNodeRole.MASTER_ROLE, transportInterceptorFactory);
        }

        private TestClusterNode newDataNode(String nodeName, TransportInterceptorFactory transportInterceptorFactory) throws IOException {
            return newNode(nodeName, dataNodeRole(), transportInterceptorFactory);
        }

        protected TestClusterNode newNode(String nodeName, DiscoveryNodeRole role, TransportInterceptorFactory transportInterceptorFactory)
            throws IOException {
            final var testClusterNode = newNode(
                DiscoveryNodeUtils.builder(randomAlphaOfLength(10)).name(nodeName).roles(Collections.singleton(role)).build(),
                transportInterceptorFactory
            );
            testClusterNode.init();
            return testClusterNode;
        }

        protected TestClusterNode newNode(DiscoveryNode node, TransportInterceptorFactory transportInterceptorFactory) throws IOException {
            return new TestClusterNode(node, transportInterceptorFactory);
        }

        public TestClusterNode randomMasterNodeSafe() {
            return randomMasterNode().orElseThrow(() -> new AssertionError("Expected to find at least one connected master node"));
        }

        public Optional<TestClusterNode> randomMasterNode() {
            // Select from sorted list of data-nodes here to not have deterministic behaviour
            final List<TestClusterNode> masterNodes = nodes.values()
                .stream()
                .filter(n -> n.node.isMasterNode())
                .filter(n -> disconnectedNodes.contains(n.node.getName()) == false)
                .sorted(Comparator.comparing(n -> n.node.getName()))
                .toList();
            return masterNodes.isEmpty() ? Optional.empty() : Optional.of(randomFrom(masterNodes));
        }

        public void stopNode(TestClusterNode node) {
            node.stop();
            nodes.remove(node.node.getName());
        }

        public TestClusterNode randomDataNodeSafe(String... excludedNames) {
            return randomDataNode(excludedNames).orElseThrow(() -> new AssertionError("Could not find another data node."));
        }

        public Optional<TestClusterNode> randomDataNode(String... excludedNames) {
            // Select from sorted list of data-nodes here to not have deterministic behaviour
            final List<TestClusterNode> dataNodes = nodes.values().stream().filter(n -> n.node.canContainData()).filter(n -> {
                for (final String nodeName : excludedNames) {
                    if (n.node.getName().equals(nodeName)) {
                        return false;
                    }
                }
                return true;
            }).sorted(Comparator.comparing(n -> n.node.getName())).toList();
            return dataNodes.isEmpty() ? Optional.empty() : Optional.ofNullable(randomFrom(dataNodes));
        }

        public void disconnectNode(TestClusterNode node) {
            if (disconnectedNodes.contains(node.node.getName())) {
                return;
            }
            nodes.values().forEach(n -> n.transportService.getConnectionManager().disconnectFromNode(node.node));
            disconnectedNodes.add(node.node.getName());
        }

        public void clearNetworkDisruptions() {
            final Set<String> disconnectedNodes = new HashSet<>(this.disconnectedNodes);
            this.disconnectedNodes.clear();
            disconnectedNodes.forEach(nodeName -> {
                if (nodes.containsKey(nodeName)) {
                    final DiscoveryNode node = nodes.get(nodeName).node;
                    nodes.values().forEach(n -> n.transportService.openConnection(node, null, ActionTestUtils.assertNoFailureListener(c -> {
                        logger.debug("--> Connected [{}] to [{}]", n.node, node);
                        n.mockTransport.deliverBlackholedRequests();
                    })));
                }
            });
        }

        /**
         * Builds a {@link DiscoveryNodes} instance that holds the nodes in this test cluster.
         * @return DiscoveryNodes
         */
        public DiscoveryNodes discoveryNodes() {
            DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
            nodes.values().forEach(node -> builder.add(node.node));
            return builder.build();
        }

        /**
         * Returns the {@link TestClusterNode} for the master node in the given {@link ClusterState}.
         * @param state ClusterState
         * @return Master Node
         */
        public TestClusterNode currentMaster(ClusterState state) {
            TestClusterNode master = nodes.get(state.nodes().getMasterNode().getName());
            Assert.assertNotNull(master);
            Assert.assertTrue(master.node.isMasterNode());
            return master;
        }

        protected Settings nodeSettings(DiscoveryNode node) {
            return Settings.EMPTY;
        }

        protected Set<Setting<?>> clusterSettings() {
            return ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
        }

        protected PluginsService createPluginsService(Settings settings, Environment environment) {
            return mock(PluginsService.class);
        }

        public interface TransportInterceptorFactory {
            TransportInterceptor createTransportInterceptor(DiscoveryNode node);
        }

        public class TestClusterNode {

            protected final ProjectResolver projectResolver = TestProjectResolvers.DEFAULT_PROJECT_ONLY;

            private final DiscoveryNode node;

            protected final NamedXContentRegistry namedXContentRegistry = new NamedXContentRegistry(
                CollectionUtils.concatLists(ClusterModule.getNamedXWriteables(), IndicesModule.getNamedXContents())
            );

            private final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(
                Stream.concat(ClusterModule.getNamedWriteables().stream(), NetworkModule.getNamedWriteables().stream()).toList()
            );

            private final TransportInterceptorFactory transportInterceptorFactory;

            protected final Environment environment;

            protected final Settings settings;

            protected final PluginsService pluginsService;

            private TransportService transportService;

            private ClusterService clusterService;

            protected SearchService searchService;

            private RecoverySettings recoverySettings;

            private PeerRecoverySourceService peerRecoverySourceService;

            protected ShardStateAction shardStateAction;

            private NodeConnectionsService nodeConnectionsService;

            protected RepositoriesService repositoriesService;

            private SnapshotsService snapshotsService;

            private SnapshotShardsService snapshotShardsService;

            protected IndicesService indicesService;

            protected PeerRecoveryTargetService peerRecoveryTargetService;

            private IndicesClusterStateService indicesClusterStateService;

            private final MasterService masterService;

            protected AllocationService allocationService;

            protected RerouteService rerouteService;

            protected final NodeClient client;

            protected NodeEnvironment nodeEnv;

            private DisruptableMockTransport mockTransport;

            protected final ThreadPool threadPool;

            protected IndexNameExpressionResolver indexNameExpressionResolver;

            protected BigArrays bigArrays;

            private final UsageService usageService;

            private Coordinator coordinator;

            public TestClusterNode(DiscoveryNode node, TransportInterceptorFactory transportInterceptorFactory) {
                this.node = node;
                this.transportInterceptorFactory = transportInterceptorFactory;
                this.environment = createEnvironment(node.getName(), tempDir, nodeSettings(node));
                this.settings = environment.settings();
                this.pluginsService = createPluginsService(settings, environment);
                this.threadPool = deterministicTaskQueue.getThreadPool(runnable -> DeterministicTaskQueue.onNodeLog(this.node, runnable));
                this.masterService = new FakeThreadPoolMasterService(node.getName(), threadPool, deterministicTaskQueue::scheduleNow);
                this.client = new NodeClient(settings, threadPool, projectResolver);
                this.usageService = new UsageService();
            }

            public final void init() throws IOException {
                final ClusterSettings clusterSettings = new ClusterSettings(settings, clusterSettings());
                clusterService = new ClusterService(
                    settings,
                    clusterSettings,
                    masterService,
                    new ClusterApplierService(node.getName(), settings, clusterSettings, threadPool) {
                        @Override
                        protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
                            return deterministicTaskQueue.getPrioritizedEsThreadPoolExecutor(command -> new Runnable() {
                                @Override
                                public void run() {
                                    try (
                                        var ignored = DeterministicTaskQueue.getLogContext('{' + node.getName() + "}{" + node.getId() + '}')
                                    ) {
                                        command.run();
                                    }
                                }

                                @Override
                                public String toString() {
                                    return "TestClusterNode.ClusterApplierService[" + command + "]";
                                }
                            });
                        }

                        @Override
                        protected void connectToNodesAndWait(ClusterState newClusterState) {
                            connectToNodesAsync(newClusterState, () -> {
                                // no need to block waiting for handshakes etc. to complete, it's enough to let the NodeConnectionsService
                                // take charge of these connections
                            });
                        }
                    }
                );
                recoverySettings = new RecoverySettings(settings, clusterSettings);
                mockTransport = new DisruptableMockTransport(node, deterministicTaskQueue) {
                    @Override
                    protected ConnectionStatus getConnectionStatus(DiscoveryNode destination) {
                        if (node.equals(destination)) {
                            return ConnectionStatus.CONNECTED;
                        }
                        // Check if both nodes are still part of the cluster
                        if (nodes.containsKey(node.getName()) == false || nodes.containsKey(destination.getName()) == false) {
                            return ConnectionStatus.DISCONNECTED;
                        }
                        return disconnectedNodes.contains(node.getName()) || disconnectedNodes.contains(destination.getName())
                            ? ConnectionStatus.DISCONNECTED
                            : ConnectionStatus.CONNECTED;
                    }

                    @Override
                    protected Optional<DisruptableMockTransport> getDisruptableMockTransport(TransportAddress address) {
                        return nodes.values()
                            .stream()
                            .map(cn -> cn.mockTransport)
                            .filter(transport -> transport.getLocalNode().getAddress().equals(address))
                            .findAny();
                    }

                    @Override
                    protected void execute(Runnable runnable) {
                        final Runnable wrappedRunnable = DeterministicTaskQueue.onNodeLog(getLocalNode(), runnable);
                        if (maybeExecuteTransportRunnable(wrappedRunnable) == false) {
                            scheduleNow(wrappedRunnable);
                        }
                    }

                    @Override
                    protected NamedWriteableRegistry writeableRegistry() {
                        return namedWriteableRegistry;
                    }

                    @Override
                    public RecyclerBytesStreamOutput newNetworkBytesStream() {
                        // skip leak checks in these tests since they do indeed leak
                        return new RecyclerBytesStreamOutput(BytesRefRecycler.NON_RECYCLING_INSTANCE);
                        // TODO fix these leaks and implement leak checking
                    }
                };
                transportService = mockTransport.createTransportService(
                    settings,
                    threadPool,
                    transportInterceptorFactory.createTransportInterceptor(node),
                    a -> node,
                    null,
                    emptySet()
                );
                indexNameExpressionResolver = TestIndexNameExpressionResolver.newInstance(threadPool.getThreadContext(), projectResolver);
                bigArrays = new BigArrays(new PageCacheRecycler(settings), null, "test");
                repositoriesService = new RepositoriesService(
                    settings,
                    clusterService,
                    Collections.singletonMap(
                        FsRepository.TYPE,
                        (projectId, metadata) -> new FsRepository(
                            projectId,
                            metadata,
                            environment,
                            namedXContentRegistry,
                            clusterService,
                            bigArrays,
                            recoverySettings
                        )
                    ),
                    emptyMap(),
                    threadPool,
                    client,
                    List.of(),
                    SnapshotMetrics.NOOP
                );
                snapshotsService = new SnapshotsService(
                    settings,
                    clusterService,
                    (reason, priority, listener) -> listener.onResponse(null),
                    indexNameExpressionResolver,
                    repositoriesService,
                    transportService,
                    EmptySystemIndices.INSTANCE,
                    false,
                    SnapshotMetrics.NOOP
                );
                nodeEnv = new NodeEnvironment(settings, environment);
                final NamedXContentRegistry namedXContentRegistry = new NamedXContentRegistry(Collections.emptyList());
                final ScriptService scriptService = new ScriptService(settings, emptyMap(), emptyMap(), () -> 1L, projectResolver);

                final SetOnce<RerouteService> rerouteServiceSetOnce = new SetOnce<>();
                final SnapshotsInfoService snapshotsInfoService = new InternalSnapshotsInfoService(
                    settings,
                    clusterService,
                    repositoriesService,
                    rerouteServiceSetOnce::get
                );
                allocationService = createAllocationService(
                    Settings.builder()
                        .put(settings)
                        .put("cluster.routing.allocation.type", "balanced") // TODO fix for desired_balance
                        .build(),
                    snapshotsInfoService
                );
                rerouteService = new BatchedRerouteService(clusterService, allocationService::reroute);
                rerouteServiceSetOnce.set(rerouteService);
                final IndexScopedSettings indexScopedSettings = new IndexScopedSettings(
                    settings,
                    IndexScopedSettings.BUILT_IN_INDEX_SETTINGS
                );
                final MapperRegistry mapperRegistry = new IndicesModule(Collections.emptyList()).getMapperRegistry();

                indicesService = new IndicesServiceBuilder().settings(settings)
                    .pluginsService(pluginsService)
                    .nodeEnvironment(nodeEnv)
                    .xContentRegistry(namedXContentRegistry)
                    .analysisRegistry(
                        new AnalysisRegistry(
                            environment,
                            emptyMap(),
                            emptyMap(),
                            emptyMap(),
                            emptyMap(),
                            emptyMap(),
                            emptyMap(),
                            emptyMap(),
                            emptyMap(),
                            emptyMap()
                        )
                    )
                    .indexNameExpressionResolver(indexNameExpressionResolver)
                    .mapperRegistry(mapperRegistry)
                    .namedWriteableRegistry(namedWriteableRegistry)
                    .threadPool(threadPool)
                    .indexScopedSettings(indexScopedSettings)
                    .circuitBreakerService(new NoneCircuitBreakerService())
                    .bigArrays(bigArrays)
                    .scriptService(scriptService)
                    .clusterService(clusterService)
                    .projectResolver(projectResolver)
                    .client(client)
                    .metaStateService(new MetaStateService(nodeEnv, namedXContentRegistry))
                    .mapperMetrics(MapperMetrics.NOOP)
                    .mergeMetrics(MergeMetrics.NOOP)
                    .build();

                this.searchService = new SearchService(
                    clusterService,
                    indicesService,
                    threadPool,
                    scriptService,
                    bigArrays,
                    new FetchPhase(Collections.emptyList()),
                    new NoneCircuitBreakerService(),
                    EmptySystemIndices.INSTANCE.getExecutorSelector(),
                    Tracer.NOOP,
                    OnlinePrewarmingService.NOOP
                );

                final SnapshotFilesProvider snapshotFilesProvider = new SnapshotFilesProvider(repositoriesService);
                peerRecoveryTargetService = new PeerRecoveryTargetService(
                    client,
                    threadPool,
                    transportService,
                    recoverySettings,
                    clusterService,
                    snapshotFilesProvider
                );

                final ActionFilters actionFilters = new ActionFilters(emptySet());
                Map<ActionType<?>, TransportAction<?, ?>> actions = new HashMap<>();

                // Inject initialization from subclass which may be needed by initializations after this point.
                doInit(actions, actionFilters);

                snapshotShardsService = new SnapshotShardsService(
                    settings,
                    clusterService,
                    repositoriesService,
                    transportService,
                    indicesService,
                    createSnapshotShardContextFactory()
                );
                shardStateAction = new ShardStateAction(clusterService, transportService, allocationService, rerouteService, threadPool);
                nodeConnectionsService = new NodeConnectionsService(clusterService.getSettings(), threadPool, transportService);
                actions.put(
                    TransportUpdateSnapshotStatusAction.TYPE,
                    new TransportUpdateSnapshotStatusAction(transportService, clusterService, threadPool, snapshotsService, actionFilters)
                );
                actions.put(
                    GlobalCheckpointSyncAction.TYPE,
                    new GlobalCheckpointSyncAction(
                        settings,
                        transportService,
                        clusterService,
                        indicesService,
                        threadPool,
                        shardStateAction,
                        actionFilters
                    )
                );
                actions.put(
                    VerifyNodeRepositoryAction.TYPE,
                    new VerifyNodeRepositoryAction.TransportAction(
                        transportService,
                        actionFilters,
                        threadPool,
                        clusterService,
                        repositoriesService,
                        projectResolver
                    )
                );
                actions.put(
                    VerifyNodeRepositoryCoordinationAction.TYPE,
                    new VerifyNodeRepositoryCoordinationAction.LocalAction(actionFilters, transportService, clusterService, client)
                );
                final MetadataMappingService metadataMappingService = new MetadataMappingService(
                    clusterService,
                    indicesService,
                    IndexSettingProviders.EMPTY
                );

                peerRecoverySourceService = new PeerRecoverySourceService(
                    transportService,
                    indicesService,
                    clusterService,
                    recoverySettings,
                    PeerOnlyRecoveryPlannerService.INSTANCE
                );

                final ResponseCollectorService responseCollectorService = new ResponseCollectorService(clusterService);
                final SearchTransportService searchTransportService = new SearchTransportService(
                    transportService,
                    client,
                    SearchExecutionStatsCollector.makeWrapper(responseCollectorService)
                );

                indicesClusterStateService = new IndicesClusterStateService(
                    settings,
                    indicesService,
                    clusterService,
                    threadPool,
                    peerRecoveryTargetService,
                    shardStateAction,
                    repositoriesService,
                    searchService,
                    peerRecoverySourceService,
                    snapshotShardsService,
                    new PrimaryReplicaSyncer(
                        transportService,
                        new TransportResyncReplicationAction(
                            settings,
                            transportService,
                            clusterService,
                            indicesService,
                            threadPool,
                            shardStateAction,
                            actionFilters,
                            new IndexingPressure(settings),
                            EmptySystemIndices.INSTANCE,
                            projectResolver
                        )
                    ),
                    RetentionLeaseSyncer.EMPTY,
                    client
                );
                final ShardLimitValidator shardLimitValidator = new ShardLimitValidator(settings, clusterService);
                final MetadataCreateIndexService metadataCreateIndexService = new MetadataCreateIndexService(
                    settings,
                    clusterService,
                    indicesService,
                    allocationService,
                    shardLimitValidator,
                    environment,
                    indexScopedSettings,
                    threadPool,
                    namedXContentRegistry,
                    EmptySystemIndices.INSTANCE,
                    false,
                    new IndexSettingProviders(getIndexSettingProviders())
                );
                actions.put(
                    TransportCreateIndexAction.TYPE,
                    new TransportCreateIndexAction(
                        transportService,
                        clusterService,
                        threadPool,
                        metadataCreateIndexService,
                        actionFilters,
                        EmptySystemIndices.INSTANCE,
                        projectResolver
                    )
                );
                final MappingUpdatedAction mappingUpdatedAction = new MappingUpdatedAction(settings, clusterSettings);
                final IndexingPressure indexingMemoryLimits = new IndexingPressure(settings);
                mappingUpdatedAction.setClient(client);
                actions.put(
                    TransportBulkAction.TYPE,
                    new TransportBulkAction(
                        threadPool,
                        transportService,
                        clusterService,
                        new IngestService(
                            clusterService,
                            threadPool,
                            environment,
                            scriptService,
                            new AnalysisModule(environment, Collections.emptyList(), new StablePluginsRegistry()).getAnalysisRegistry(),
                            Collections.emptyList(),
                            client,
                            null,
                            FailureStoreMetrics.NOOP,
                            projectResolver,
                            new FeatureService(List.of()) {
                                @Override
                                public boolean clusterHasFeature(ClusterState state, NodeFeature feature) {
                                    return DataStream.DATA_STREAM_FAILURE_STORE_FEATURE.equals(feature);
                                }
                            },
                            mock(SamplingService.class)
                        ),
                        client,
                        actionFilters,
                        indexNameExpressionResolver,
                        new IndexingPressure(settings),
                        EmptySystemIndices.INSTANCE,
                        projectResolver,
                        FailureStoreMetrics.NOOP,
                        DataStreamFailureStoreSettings.create(ClusterSettings.createBuiltInClusterSettings()),
                        new FeatureService(List.of()) {
                            @Override
                            public boolean clusterHasFeature(ClusterState state, NodeFeature feature) {
                                return DataStream.DATA_STREAM_FAILURE_STORE_FEATURE.equals(feature);
                            }
                        },
                        mock(SamplingService.class)
                    )
                );
                final TransportShardBulkAction transportShardBulkAction = new TransportShardBulkAction(
                    settings,
                    transportService,
                    clusterService,
                    indicesService,
                    threadPool,
                    shardStateAction,
                    mappingUpdatedAction,
                    new UpdateHelper(scriptService),
                    actionFilters,
                    indexingMemoryLimits,
                    EmptySystemIndices.INSTANCE,
                    projectResolver,
                    DocumentParsingProvider.EMPTY_INSTANCE
                );
                actions.put(TransportShardBulkAction.TYPE, transportShardBulkAction);
                final RestoreService restoreService = new RestoreService(
                    clusterService,
                    repositoriesService,
                    allocationService,
                    metadataCreateIndexService,
                    new IndexMetadataVerifier(
                        settings,
                        clusterService,
                        namedXContentRegistry,
                        mapperRegistry,
                        indexScopedSettings,
                        ScriptCompiler.NONE,
                        MapperMetrics.NOOP
                    ),
                    shardLimitValidator,
                    EmptySystemIndices.INSTANCE,
                    indicesService,
                    mock(FileSettingsService.class),
                    threadPool,
                    false,
                    IndexMetadataRestoreTransformer.NoOpRestoreTransformer.getInstance()
                );
                actions.put(
                    TransportPutMappingAction.TYPE,
                    new TransportPutMappingAction(
                        transportService,
                        clusterService,
                        threadPool,
                        metadataMappingService,
                        actionFilters,
                        indexNameExpressionResolver,
                        new RequestValidators<>(Collections.emptyList()),
                        EmptySystemIndices.INSTANCE,
                        projectResolver
                    )
                );
                actions.put(
                    TransportAutoPutMappingAction.TYPE,
                    new TransportAutoPutMappingAction(
                        transportService,
                        clusterService,
                        threadPool,
                        metadataMappingService,
                        actionFilters,
                        projectResolver,
                        EmptySystemIndices.INSTANCE
                    )
                );

                SearchPhaseController searchPhaseController = new SearchPhaseController(searchService::aggReduceContextBuilder);
                actions.put(
                    TransportSearchAction.TYPE,
                    new TransportSearchAction(
                        threadPool,
                        new NoneCircuitBreakerService(),
                        transportService,
                        searchService,
                        responseCollectorService,
                        searchTransportService,
                        searchPhaseController,
                        clusterService,
                        actionFilters,
                        projectResolver,
                        indexNameExpressionResolver,
                        namedWriteableRegistry,
                        EmptySystemIndices.INSTANCE.getExecutorSelector(),
                        new SearchResponseMetrics(TelemetryProvider.NOOP.getMeterRegistry()),
                        client,
                        usageService,
                        new IndicesServiceTests.TestActionActionLoggingFieldsProvider(),
                        ActivityLogWriterProvider.NOOP
                    )
                );
                actions.put(
                    TransportRestoreSnapshotAction.TYPE,
                    new TransportRestoreSnapshotAction(
                        transportService,
                        clusterService,
                        threadPool,
                        restoreService,
                        actionFilters,
                        projectResolver
                    )
                );
                actions.put(
                    TransportDeleteIndexAction.TYPE,
                    new TransportDeleteIndexAction(
                        transportService,
                        clusterService,
                        threadPool,
                        new MetadataDeleteIndexService(settings, clusterService, allocationService),
                        actionFilters,
                        projectResolver,
                        indexNameExpressionResolver,
                        new DestructiveOperations(settings, clusterSettings)
                    )
                );
                actions.put(
                    TransportPutRepositoryAction.TYPE,
                    new TransportPutRepositoryAction(
                        transportService,
                        clusterService,
                        repositoriesService,
                        threadPool,
                        actionFilters,
                        projectResolver
                    )
                );
                actions.put(
                    TransportCleanupRepositoryAction.TYPE,
                    new TransportCleanupRepositoryAction(
                        transportService,
                        clusterService,
                        repositoriesService,
                        threadPool,
                        actionFilters,
                        projectResolver
                    )
                );
                actions.put(
                    TransportCreateSnapshotAction.TYPE,
                    new TransportCreateSnapshotAction(
                        transportService,
                        clusterService,
                        threadPool,
                        snapshotsService,
                        actionFilters,
                        projectResolver
                    )
                );
                actions.put(
                    TransportCloneSnapshotAction.TYPE,
                    new TransportCloneSnapshotAction(
                        transportService,
                        clusterService,
                        threadPool,
                        snapshotsService,
                        actionFilters,
                        projectResolver
                    )
                );
                actions.put(
                    TransportGetSnapshotsAction.TYPE,
                    new TransportGetSnapshotsAction(
                        transportService,
                        clusterService,
                        threadPool,
                        repositoriesService,
                        actionFilters,
                        projectResolver
                    )
                );
                actions.put(
                    TransportClusterRerouteAction.TYPE,
                    new TransportClusterRerouteAction(
                        transportService,
                        clusterService,
                        threadPool,
                        allocationService,
                        actionFilters,
                        projectResolver
                    )
                );
                actions.put(
                    ClusterStateAction.INSTANCE,
                    new TransportClusterStateAction(
                        transportService,
                        clusterService,
                        threadPool,
                        actionFilters,
                        indexNameExpressionResolver,
                        projectResolver,
                        new NoOpClient(threadPool)
                    )
                );
                actions.put(
                    TransportIndicesShardStoresAction.TYPE,
                    new TransportIndicesShardStoresAction(
                        transportService,
                        clusterService,
                        threadPool,
                        actionFilters,
                        indexNameExpressionResolver,
                        client
                    )
                );
                actions.put(
                    TransportNodesListGatewayStartedShards.TYPE,
                    new TransportNodesListGatewayStartedShards(
                        settings,
                        threadPool,
                        clusterService,
                        transportService,
                        actionFilters,
                        nodeEnv,
                        indicesService,
                        namedXContentRegistry
                    )
                );
                actions.put(
                    TransportDeleteSnapshotAction.TYPE,
                    new TransportDeleteSnapshotAction(
                        transportService,
                        clusterService,
                        threadPool,
                        snapshotsService,
                        actionFilters,
                        projectResolver
                    )
                );
                actions.put(
                    TransportClusterHealthAction.TYPE,
                    new TransportClusterHealthAction(
                        transportService,
                        clusterService,
                        threadPool,
                        actionFilters,
                        indexNameExpressionResolver,
                        allocationService,
                        projectResolver
                    )
                );

                client.initialize(
                    actions,
                    transportService.getTaskManager(),
                    () -> clusterService.localNode().getId(),
                    transportService.getLocalNodeConnection(),
                    transportService.getRemoteClusterService()
                );

                warningConsumer.accept(getWarningHeaders());
            }

            protected String[] getWarningHeaders() {
                return new String[] {
                    "[cluster.routing.allocation.type] setting was deprecated in Elasticsearch and will be removed in a future release. "
                        + "See the breaking changes documentation for the next major version." };
            }

            protected void doInit(Map<ActionType<?>, TransportAction<?, ?>> actions, ActionFilters actionFilters) {}

            protected SnapshotShardContextFactory createSnapshotShardContextFactory() {
                return new LocalPrimarySnapshotShardContextFactory(clusterService, indicesService);
            }

            /**
             * Maybe execute the given transport runnable inline. If executed, return true, else return false to schedule it.
             * @param runnable The transport layer runnable, i.e. request and response.
             * @return true if executed, false to schedule it.
             */
            protected boolean maybeExecuteTransportRunnable(Runnable runnable) {
                return false;
            }

            public void restart() {
                restart(() -> true);
            }

            /**
             * Restart this node by stopping, recreation and starting with the cluster state before stop.
             * @param scheduleCondition A condition to satisfy before actually starting the node. If the condition is not satisfied,
             *                          reschedule the start at a future time.
             */
            public void restart(BooleanSupplier scheduleCondition) {
                disconnectNode(this);
                final ClusterState oldState = this.clusterService.state();
                stop();
                nodes.remove(node.getName());

                final Runnable startRunnable = new Runnable() {
                    @Override
                    public void run() {
                        if (scheduleCondition.getAsBoolean() == false) {
                            TestClusterNode.this.scheduleSoon(this);
                            return;
                        }
                        try {
                            final TestClusterNode restartedNode = newNode(
                                DiscoveryNodeUtils.create(node.getName(), node.getId(), node.getAddress(), emptyMap(), node.getRoles()),
                                transportInterceptorFactory
                            );
                            restartedNode.init();
                            nodes.put(node.getName(), restartedNode);
                            restartedNode.start(oldState);
                        } catch (IOException e) {
                            throw new AssertionError(e);
                        }
                    }
                };
                scheduleSoon(startRunnable);
            }

            public DiscoveryNode node() {
                return node;
            }

            public NodeClient client() {
                return client;
            }

            public RepositoriesService repositoriesService() {
                return repositoriesService;
            }

            public ClusterService clusterService() {
                return clusterService;
            }

            public Coordinator coordinator() {
                return coordinator;
            }

            public TransportService transportService() {
                return transportService;
            }

            protected AllocationService createAllocationService(Settings settings, SnapshotsInfoService snapshotsInfoService) {
                return ESAllocationTestCase.createAllocationService(settings, snapshotsInfoService);
            }

            protected Set<IndexSettingProvider> getIndexSettingProviders() {
                return Set.of();
            }

            protected Supplier<CoordinationState.PersistedState> getPersistedStateSupplier(ClusterState initialState, DiscoveryNode node) {
                final CoordinationState.PersistedState persistedState = new InMemoryPersistedState(
                    initialState.term(),
                    stateForNode(initialState, node)
                );
                return () -> persistedState;
            }

            protected ElectionStrategy createElectionStrategy() {
                return ElectionStrategy.DEFAULT_INSTANCE;
            }

            protected LeaderHeartbeatService getLeaderHeartbeatService() {
                return LeaderHeartbeatService.NO_OP;
            }

            protected Reconfigurator createReconfigurator() {
                return new Reconfigurator(clusterService.getSettings(), clusterService.getClusterSettings());
            }

            protected PreVoteCollector.Factory createPrevoteCollector() {
                return StatefulPreVoteCollector::new;
            }

            public void stop() {
                disconnectNode(this);
                indicesService.close();
                clusterService.close();
                nodeConnectionsService.stop();
                indicesClusterStateService.close();
                peerRecoverySourceService.stop();
                if (coordinator != null) {
                    coordinator.close();
                }
                nodeEnv.close();
            }

            public void start(ClusterState initialState) {
                transportService.start();
                transportService.acceptIncomingRequests();
                snapshotsService.start();
                snapshotShardsService.start();
                repositoriesService.start();
                coordinator = new Coordinator(
                    node.getName(),
                    clusterService.getSettings(),
                    clusterService.getClusterSettings(),
                    transportService,
                    client,
                    namedWriteableRegistry,
                    allocationService,
                    masterService,
                    getPersistedStateSupplier(initialState, node),
                    hostsResolver -> nodes.values().stream().filter(n -> n.node.isMasterNode()).map(n -> n.node.getAddress()).toList(),
                    clusterService.getClusterApplierService(),
                    Collections.emptyList(),
                    LuceneTestCase.random(),
                    rerouteService,
                    createElectionStrategy(),
                    () -> new StatusInfo(HEALTHY, "healthy-info"),
                    new NoneCircuitBreakerService(),
                    createReconfigurator(),
                    getLeaderHeartbeatService(),
                    createPrevoteCollector(),
                    CompatibilityVersionsUtils.staticCurrent(),
                    new FeatureService(List.of()),
                    this.clusterService
                );
                masterService.setClusterStatePublisher(coordinator);
                coordinator.start();
                clusterService.getClusterApplierService().setNodeConnectionsService(nodeConnectionsService);
                nodeConnectionsService.start();
                clusterService.start();
                indicesService.start();
                indicesClusterStateService.start();
                coordinator.startInitialJoin();
                peerRecoverySourceService.start();
            }

            private void scheduleNow(Runnable runnable) {
                deterministicTaskQueue.scheduleNow(runnable);
            }

            protected void scheduleSoon(Runnable runnable) {
                deterministicTaskQueue.scheduleAt(deterministicTaskQueue.getCurrentTimeMillis() + randomLongBetween(0, 100L), runnable);
            }
        }
    }
}
