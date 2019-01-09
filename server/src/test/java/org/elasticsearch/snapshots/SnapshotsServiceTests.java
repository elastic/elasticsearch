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

package org.elasticsearch.snapshots;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.bootstrap.BootstrapConfiguration;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.TransportCreateSnapshotAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.resync.TransportResyncReplicationAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.action.index.NodeMappingRefreshAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.coordination.ClusterBootstrapService;
import org.elasticsearch.cluster.coordination.CoordinationState;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.CoordinatorTests;
import org.elasticsearch.cluster.coordination.DeterministicTaskQueue;
import org.elasticsearch.cluster.coordination.InMemoryPersistedState;
import org.elasticsearch.cluster.coordination.MockSinglePrioritizingExecutor;
import org.elasticsearch.cluster.metadata.AliasValidator;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetaDataMappingService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.gateway.MetaStateService;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.seqno.GlobalCheckpointSyncAction;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.cluster.FakeThreadPoolMasterService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.flush.SyncedFlushService;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.indices.recovery.PeerRecoverySourceService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.disruption.DisruptableMockTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.env.Environment.PATH_HOME_SETTING;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.transport.TransportService.HANDSHAKE_ACTION_NAME;
import static org.elasticsearch.transport.TransportService.NOOP_TRANSPORT_INTERCEPTOR;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;

public class SnapshotsServiceTests extends ESTestCase {

    private DeterministicTaskQueue deterministicTaskQueue;

    private TestClusterNodes testClusterNodes;

    private Path tempDir;

    @Before
    public void createServices() {
        tempDir = createTempDir();
        deterministicTaskQueue =
            new DeterministicTaskQueue(Settings.builder().put(NODE_NAME_SETTING.getKey(), "shared").build(), random());
    }

    @After
    public void stopServices() {
        testClusterNodes.nodes.values().forEach(
            n -> {
                n.indicesService.close();
                n.clusterService.close();
                n.indicesClusterStateService.close();
                n.nodeEnv.close();
                n.coordinator.close();
            }
        );
    }

    public void testSuccessfulSnapshot() {
        setupTestCluster(randomFrom(1, 3, 5), randomIntBetween(2, 10));

        String repoName = "repo";
        String snapshotName = "snapshot";
        final String index = "test";

        final int shards = randomIntBetween(1, 10);

        TestClusterNode masterNode =
            testClusterNodes.currentMaster(testClusterNodes.nodes.values().iterator().next().clusterService.state());
        final AtomicBoolean createdSnapshot = new AtomicBoolean();
        masterNode.client.admin().cluster().preparePutRepository(repoName)
            .setType(FsRepository.TYPE).setSettings(Settings.builder().put("location", randomAlphaOfLength(10)))
            .execute(
                assertNoFailureListener(
                    () -> masterNode.client.admin().indices().create(
                        new CreateIndexRequest(index).waitForActiveShards(ActiveShardCount.ALL).settings(
                            Settings.builder()
                                .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), shards)
                                .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)),
                        assertNoFailureListener(
                            () -> masterNode.client.admin().cluster().prepareCreateSnapshot(repoName, snapshotName)
                                .execute(assertNoFailureListener(() -> createdSnapshot.set(true)))))));

        deterministicTaskQueue.runAllRunnableTasks();

        assertTrue(createdSnapshot.get());
        SnapshotsInProgress finalSnapshotsInProgress = masterNode.clusterService.state().custom(SnapshotsInProgress.TYPE);
        assertFalse(finalSnapshotsInProgress.entries().stream().anyMatch(entry -> entry.state().completed() == false));
        final Repository repository = masterNode.repositoriesService.repository(repoName);
        Collection<SnapshotId> snapshotIds = repository.getRepositoryData().getSnapshotIds();
        assertThat(snapshotIds, hasSize(1));

        final SnapshotInfo snapshotInfo = repository.getSnapshotInfo(snapshotIds.iterator().next());
        assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
        assertThat(snapshotInfo.indices(), containsInAnyOrder(index));
        assertEquals(shards, snapshotInfo.successfulShards());
        assertEquals(0, snapshotInfo.failedShards());
    }

    private void startCluster() {
        final ClusterState initialClusterState =
            new ClusterState.Builder(ClusterName.DEFAULT).nodes(testClusterNodes.randomDiscoveryNodes()).build();
        testClusterNodes.nodes.values().forEach(testClusterNode -> testClusterNode.start(initialClusterState));

        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runAllRunnableTasks();

        final BootstrapConfiguration bootstrapConfiguration = new BootstrapConfiguration(
            testClusterNodes.nodes.values().stream().filter(n -> n.node.isMasterNode())
                .map(node -> new BootstrapConfiguration.NodeDescription(node.node))
                .distinct()
                .collect(Collectors.toList()));
        testClusterNodes.nodes.values().stream().filter(n -> n.node.isMasterNode()).forEach(
            testClusterNode -> testClusterNode.coordinator.setInitialConfiguration(bootstrapConfiguration)
        );

        runUntil(
            () -> {
                List<String> masterNodeIds = testClusterNodes.nodes.values().stream()
                    .map(node -> node.clusterService.state().nodes().getMasterNodeId())
                    .distinct().collect(Collectors.toList());
                return masterNodeIds.size() == 1 && masterNodeIds.contains(null) == false;
            },
            TimeUnit.SECONDS.toMillis(30L)
        );
    }

    private void runUntil(Supplier<Boolean> fulfilled, long timeout) {
        final long start = deterministicTaskQueue.getCurrentTimeMillis();
        while (timeout > deterministicTaskQueue.getCurrentTimeMillis() - start) {
            deterministicTaskQueue.runAllRunnableTasks();
            if (fulfilled.get()) {
                return;
            }
            deterministicTaskQueue.advanceTime();
        }
        fail("Condition wasn't fulfilled.");
    }

    private void setupTestCluster(int masterNodes, int dataNodes) {
        testClusterNodes = new TestClusterNodes(masterNodes, dataNodes);
        startCluster();
    }

    private static <T> ActionListener<T> assertNoFailureListener(Runnable r) {
        return new ActionListener<T>() {
            @Override
            public void onResponse(final T t) {
                r.run();
            }

            @Override
            public void onFailure(final Exception e) {
                throw new AssertionError(e);
            }
        };
    }

    /**
     * Create a {@link Environment} with random path.home and path.repo
     **/
    private Environment createEnvironment(String nodeName) {
        return TestEnvironment.newEnvironment(Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), nodeName)
            .put(PATH_HOME_SETTING.getKey(), tempDir.resolve(nodeName).toAbsolutePath())
            .put(Environment.PATH_REPO_SETTING.getKey(), tempDir.resolve("repo").toAbsolutePath())
            .putList(ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING.getKey(),
                ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING.get(Settings.EMPTY))
            .build());
    }

    private TestClusterNode newMasterNode(String nodeName) throws IOException {
        return newNode(nodeName, DiscoveryNode.Role.MASTER);
    }

    private TestClusterNode newDataNode(String nodeName) throws IOException {
        return newNode(nodeName, DiscoveryNode.Role.DATA);
    }

    private TestClusterNode newNode(String nodeName, DiscoveryNode.Role role) throws IOException {
        return new TestClusterNode(
            new DiscoveryNode(nodeName, randomAlphaOfLength(10), buildNewFakeTransportAddress(), emptyMap(),
                Collections.singleton(role), Version.CURRENT)
        );
    }

    private static ClusterState stateForNode(ClusterState state, DiscoveryNode node) {
        return ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes()).localNodeId(node.getId())).build();
    }

    private final class TestClusterNodes {

        // LinkedHashMap so we have deterministic ordering when iterating over the map in tests
        private final Map<String, TestClusterNode> nodes = new LinkedHashMap<>();

        TestClusterNodes(int masterNodes, int dataNodes) {
            for (int i = 0; i < masterNodes; ++i) {
                nodes.computeIfAbsent("node" + i, nodeName -> {
                    try {
                        return SnapshotsServiceTests.this.newMasterNode(nodeName);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            }
            for (int i = 0; i < dataNodes; ++i) {
                nodes.computeIfAbsent("data-node" + i, nodeName -> {
                    try {
                        return SnapshotsServiceTests.this.newDataNode(nodeName);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            }
        }

        /**
         * Builds a {@link DiscoveryNodes} instance that has one master eligible node set as its master
         * by random.
         * @return DiscoveryNodes with set master node
         */
        public DiscoveryNodes randomDiscoveryNodes() {
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
            assertNotNull(master);
            assertTrue(master.node.isMasterNode());
            return master;
        }
    }

    private final class TestClusterNode {

        private final Logger logger = LogManager.getLogger(TestClusterNode.class);

        private final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(ClusterModule.getNamedWriteables());

        private final TransportService transportService;

        private final ClusterService clusterService;

        private final RepositoriesService repositoriesService;

        private final SnapshotsService snapshotsService;

        private final SnapshotShardsService snapshotShardsService;

        private final IndicesService indicesService;

        private final IndicesClusterStateService indicesClusterStateService;

        private final DiscoveryNode node;

        private final MasterService masterService;

        private final AllocationService allocationService;

        private final NodeClient client;

        private final NodeEnvironment nodeEnv;

        private final DisruptableMockTransport mockTransport;

        private final ThreadPool threadPool;

        private Coordinator coordinator;

        TestClusterNode(DiscoveryNode node) throws IOException {
            this.node = node;
            final Environment environment = createEnvironment(node.getName());
            masterService = new FakeThreadPoolMasterService(node.getName(), "test", deterministicTaskQueue::scheduleNow);
            final Settings settings = environment.settings();
            final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            threadPool = deterministicTaskQueue.getThreadPool();
            clusterService = new ClusterService(settings, clusterSettings, masterService,
                new ClusterApplierService(node.getName(), settings, clusterSettings, threadPool) {
                    @Override
                    protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
                        return new MockSinglePrioritizingExecutor(node.getName(), deterministicTaskQueue);
                    }
                });
            mockTransport = new DisruptableMockTransport(logger) {
                @Override
                protected DiscoveryNode getLocalNode() {
                    return node;
                }

                @Override
                protected ConnectionStatus getConnectionStatus(DiscoveryNode sender, DiscoveryNode destination) {
                    return ConnectionStatus.CONNECTED;
                }

                @Override
                protected Optional<DisruptableMockTransport> getDisruptedCapturingTransport(DiscoveryNode node, String action) {
                    final Predicate<TestClusterNode> matchesDestination;
                    if (action.equals(HANDSHAKE_ACTION_NAME)) {
                        matchesDestination = n -> n.transportService.getLocalNode().getAddress().equals(node.getAddress());
                    } else {
                        matchesDestination = n -> n.transportService.getLocalNode().equals(node);
                    }
                    return testClusterNodes.nodes.values().stream().filter(matchesDestination).findAny().map(cn -> cn.mockTransport);
                }

                @Override
                protected void handle(DiscoveryNode sender, DiscoveryNode destination, String action, Runnable doDelivery) {
                    // handshake needs to run inline as the caller blockingly waits on the result
                    final Runnable runnable = CoordinatorTests.onNode(destination, doDelivery);
                    if (action.equals(HANDSHAKE_ACTION_NAME)) {
                        runnable.run();
                    } else {
                        deterministicTaskQueue.scheduleNow(runnable);
                    }
                }
            };
            transportService = mockTransport.createTransportService(
                settings, deterministicTaskQueue.getThreadPool(runnable -> CoordinatorTests.onNode(node, runnable)),
                NOOP_TRANSPORT_INTERCEPTOR,
                a -> node, null, emptySet()
            );
            final IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver();
            repositoriesService = new RepositoriesService(
                settings, clusterService, transportService,
                Collections.singletonMap(FsRepository.TYPE, metaData -> {
                        final Repository repository = new FsRepository(metaData, environment, xContentRegistry()) {
                            @Override
                            protected void assertSnapshotOrGenericThread() {
                                // eliminate thread name check as we create repo in the test thread
                            }
                        };
                        repository.start();
                        return repository;
                    }
                ),
                emptyMap(),
                threadPool
            );
            snapshotsService =
                new SnapshotsService(settings, clusterService, indexNameExpressionResolver, repositoriesService, threadPool);
            nodeEnv = new NodeEnvironment(settings, environment);
            final NamedXContentRegistry namedXContentRegistry = new NamedXContentRegistry(Collections.emptyList());
            final ScriptService scriptService = new ScriptService(settings, emptyMap(), emptyMap());
            client = new NodeClient(settings, threadPool);
            allocationService = ESAllocationTestCase.createAllocationService(settings);
            final IndexScopedSettings indexScopedSettings =
                new IndexScopedSettings(settings, IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
            indicesService = new IndicesService(
                settings,
                mock(PluginsService.class),
                nodeEnv,
                namedXContentRegistry,
                new AnalysisRegistry(environment, emptyMap(), emptyMap(), emptyMap(), emptyMap(), emptyMap(),
                    emptyMap(), emptyMap(), emptyMap(), emptyMap()),
                indexNameExpressionResolver,
                new MapperRegistry(emptyMap(), emptyMap(), MapperPlugin.NOOP_FIELD_FILTER),
                namedWriteableRegistry,
                threadPool,
                indexScopedSettings,
                new NoneCircuitBreakerService(),
                new BigArrays(new PageCacheRecycler(settings), null, "test"),
                scriptService,
                client,
                new MetaStateService(nodeEnv, namedXContentRegistry),
                Collections.emptyList(),
                emptyMap()
            );
            final RecoverySettings recoverySettings = new RecoverySettings(settings, clusterSettings);
            final ActionFilters actionFilters = new ActionFilters(emptySet());
            snapshotShardsService = new SnapshotShardsService(
                settings, clusterService, snapshotsService, threadPool,
                transportService, indicesService, actionFilters, indexNameExpressionResolver);
            final ShardStateAction shardStateAction = new ShardStateAction(
                clusterService, transportService, allocationService,
                new RoutingService(settings, clusterService, allocationService),
                deterministicTaskQueue.getThreadPool()
            );
            indicesClusterStateService = new IndicesClusterStateService(
                settings, indicesService, clusterService, threadPool,
                new PeerRecoveryTargetService(
                    deterministicTaskQueue.getThreadPool(), transportService, recoverySettings,
                    clusterService
                ),
                shardStateAction,
                new NodeMappingRefreshAction(transportService, new MetaDataMappingService(clusterService, indicesService)),
                repositoriesService,
                mock(SearchService.class),
                new SyncedFlushService(indicesService, clusterService, transportService, indexNameExpressionResolver),
                new PeerRecoverySourceService(transportService, indicesService, recoverySettings),
                snapshotShardsService,
                new PrimaryReplicaSyncer(
                    transportService,
                    new TransportResyncReplicationAction(
                        settings, transportService, clusterService, indicesService, threadPool,
                        shardStateAction, actionFilters, indexNameExpressionResolver)
                ),
                new GlobalCheckpointSyncAction(
                    settings, transportService, clusterService, indicesService, threadPool,
                    shardStateAction, actionFilters, indexNameExpressionResolver)
            );
            Map<Action, TransportAction> actions = new HashMap<>();
            actions.put(CreateIndexAction.INSTANCE,
                new TransportCreateIndexAction(
                    transportService, clusterService, threadPool,
                    new MetaDataCreateIndexService(settings, clusterService, indicesService,
                        allocationService, new AliasValidator(), environment, indexScopedSettings,
                        threadPool, namedXContentRegistry, false),
                    actionFilters, indexNameExpressionResolver
                ));
            actions.put(PutRepositoryAction.INSTANCE,
                new TransportPutRepositoryAction(
                    transportService, clusterService, repositoriesService, threadPool,
                    actionFilters, indexNameExpressionResolver
                ));
            actions.put(CreateSnapshotAction.INSTANCE,
                new TransportCreateSnapshotAction(
                    transportService, clusterService, threadPool,
                    snapshotsService, actionFilters, indexNameExpressionResolver
                ));
            client.initialize(actions, () -> clusterService.localNode().getId(), transportService.getRemoteClusterService());
        }

        public void start(ClusterState initialState) {
            transportService.start();
            transportService.acceptIncomingRequests();
            snapshotsService.start();
            snapshotShardsService.start();
            final CoordinationState.PersistedState persistedState =
                new InMemoryPersistedState(0L, stateForNode(initialState, node));
            coordinator = new Coordinator(node.getName(), clusterService.getSettings(),
                clusterService.getClusterSettings(), transportService, namedWriteableRegistry,
                allocationService, masterService, () -> persistedState,
                hostsResolver -> testClusterNodes.nodes.values().stream().filter(n -> n.node.isMasterNode())
                    .map(n -> n.node.getAddress()).collect(Collectors.toList()),
                clusterService.getClusterApplierService(), random());
            masterService.setClusterStatePublisher(coordinator);
            coordinator.start();
            masterService.start();
            clusterService.getClusterApplierService().setNodeConnectionsService(
                new NodeConnectionsService(clusterService.getSettings(), threadPool, transportService));
            clusterService.getClusterApplierService().start();
            indicesService.start();
            indicesClusterStateService.start();
            coordinator.startInitialJoin();
        }
    }
}
