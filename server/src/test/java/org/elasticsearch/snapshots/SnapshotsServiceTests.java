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

import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.resync.TransportResyncReplicationAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.action.index.NodeMappingRefreshAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.coordination.DeterministicTaskQueue;
import org.elasticsearch.cluster.metadata.AliasValidator;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetaDataMappingService;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
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
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.transport.RequestHandlerRegistry;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.env.Environment.PATH_HOME_SETTING;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.transport.TransportService.NOOP_TRANSPORT_INTERCEPTOR;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SnapshotsServiceTests extends ESTestCase {

    private TestClusterNodes testClusterNodes;

    @Before
    public void createServices() {
        // TODO: Random number of master nodes and simulate master failover states
        testClusterNodes = new TestClusterNodes(1, randomIntBetween(2, 10));
    }

    @After
    public void stopServices() {
        testClusterNodes.nodes.values().forEach(
            n -> {
                n.indicesService.close();
                n.clusterService.close();
                n.indicesClusterStateService.close();
                n.nodeEnv.close();
            }
        );
    }

    @TestLogging("_root:debug")
    public void testSuccessfulSnapshot() {
        // Set up fake repository
        String repoName = "repo";
        String snapshotName = "snapshot";
        final int shards = randomIntBetween(1, 10);
        final Repository repository = createRepository();
        testClusterNodes.nodes.values().forEach(
            node -> when(node.repositoriesService.repository(repoName)).thenReturn(repository)
        );
        String index = "test";
        MetaData metaData = MetaData.builder().putCustom(
            RepositoriesMetaData.TYPE,
            new RepositoriesMetaData(
                Collections.singletonList(
                    new RepositoryMetaData(
                        repoName, randomAlphaOfLength(10), Settings.EMPTY
                    )
                )
            )
        ).build();

        ClusterState initialClusterState =
            new ClusterState.Builder(ClusterName.DEFAULT)
                .nodes(testClusterNodes.randomDiscoveryNodes())
                .metaData(metaData)
                .build();
        startServices(initialClusterState);

        TestClusterNode masterNode = testClusterNodes.currentMaster(initialClusterState);
        ActionFuture<CreateIndexResponse> createIndexResponseActionFuture = masterNode.client.admin().indices().create(
            new CreateIndexRequest(index).settings(
                Settings.builder()
                    .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), shards)
                    .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
            )
        );

        while (testClusterNodes.nodes.values().stream().anyMatch(node -> node.deterministicTaskQueue.hasRunnableTasks())) {
            testClusterNodes.nodes.values().forEach(node -> {
                if (node.deterministicTaskQueue.hasRunnableTasks()) {
                    node.deterministicTaskQueue.runRandomTask();
                }
            });
        }

        createIndexResponseActionFuture.actionGet();

        SetOnce<Boolean> successfulSnapshotStart = new SetOnce<>();
        masterNode.snapshotsService.createSnapshot(
            new SnapshotsService.SnapshotRequest(repoName, snapshotName, ""),
            new SnapshotsService.CreateSnapshotListener() {
                @Override
                public void onResponse() {
                    successfulSnapshotStart.set(true);
                }

                @Override
                public void onFailure(final Exception e) {
                    throw new AssertionError("Snapshot failed.");
                }
            });
        assertTrue(
            "Expected a begin snapshot task in the master node's threadpool.", masterNode.deterministicTaskQueue.hasRunnableTasks());
        while (successfulSnapshotStart.get() == null || masterNode.deterministicTaskQueue.hasRunnableTasks()) {
            masterNode.deterministicTaskQueue.runRandomTask();
        }
        assertTrue("Snapshot did not start successfully.", successfulSnapshotStart.get());

        while (testClusterNodes.nodes.values().stream().anyMatch(node -> node.deterministicTaskQueue.hasRunnableTasks())) {
            testClusterNodes.nodes.values().forEach(node -> {
                if (node.deterministicTaskQueue.hasRunnableTasks()) {
                    node.deterministicTaskQueue.runRandomTask();
                }
            });
        }
        assertNoSnapshotsInProgress(masterNode.currentState.get());
        Collection<SnapshotId> snapshotIds = repository.getRepositoryData().getSnapshotIds();
        assertThat(snapshotIds, hasSize(1));
        SnapshotInfo data = repository.getSnapshotInfo(snapshotIds.stream().findFirst().get());
        assertEquals(SnapshotState.SUCCESS, data.state());
    }

    /**
     * Create a {@link Repository} with a random name
     **/
    private Repository createRepository() {
        Settings settings = Settings.builder().put("location", randomAlphaOfLength(10)).build();
        RepositoryMetaData repositoryMetaData = new RepositoryMetaData(randomAlphaOfLength(10), FsRepository.TYPE, settings);
        final FsRepository repository = new FsRepository(repositoryMetaData, createEnvironment(), xContentRegistry()) {
            @Override
            protected void assertSnapshotOrGenericThread() {
                // eliminate thread name check as we create repo manually
            }
        };
        repository.start();
        return repository;
    }

    /**
     * Create a {@link Environment} with random path.home and path.repo
     **/
    private Environment createEnvironment() {
        Path home = createTempDir();
        return TestEnvironment.newEnvironment(Settings.builder()
            .put(PATH_HOME_SETTING.getKey(), home.toAbsolutePath())
            .put(Environment.PATH_REPO_SETTING.getKey(), home.resolve("repo").toAbsolutePath())
            .build());
    }

    private static void assertNoSnapshotsInProgress(ClusterState clusterState) {
        SnapshotsInProgress finalSnapshotsInProgress = clusterState.custom(SnapshotsInProgress.TYPE);
        assertFalse(finalSnapshotsInProgress.entries().stream().anyMatch(entry -> !entry.state().completed()));
    }

    private void startServices(ClusterState initialState) {
        testClusterNodes.nodes.values().forEach(testClusterNode -> testClusterNode.start(initialState));
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
                Collections.singleton(role), Version.CURRENT),
            new DeterministicTaskQueue(Settings.builder().put(NODE_NAME_SETTING.getKey(), nodeName).build(), random())
        );
    }

    private final class TestClusterNodes {

        private final Map<String, TestClusterNode> nodes = new HashMap<>();

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
            for (int i = masterNodes; i < dataNodes + masterNodes; ++i) {
                nodes.computeIfAbsent("node" + i, nodeName -> {
                    try {
                        return SnapshotsServiceTests.this.newDataNode(nodeName);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            }
        }

        public DiscoveryNodes randomDiscoveryNodes() {
            DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
            nodes.values().forEach(node -> builder.add(node.node));
            String masterId = randomFrom(nodes.values().stream().map(node -> node.node).filter(DiscoveryNode::isMasterNode)
                .map(DiscoveryNode::getId)
                .collect(Collectors.toList()));
            return builder.localNodeId(masterId).masterNodeId(masterId).build();
        }

        public TestClusterNode currentMaster(ClusterState state) {
            TestClusterNode master = nodes.get(state.nodes().getMasterNode().getName());
            assertNotNull(master);
            assertTrue(master.node.isMasterNode());
            return master;
        }
    }

    private static ClusterState stateForNode(ClusterState state, DiscoveryNode node) {
        return ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes()).localNodeId(node.getId())).build();
    }

    private static ClusterChangedEvent changeEventForNode(ClusterChangedEvent event, DiscoveryNode node) {
        return new ClusterChangedEvent(event.source(), stateForNode(event.state(), node), stateForNode(event.previousState(), node));
    }

    private final class TestClusterNode {

        private final DeterministicTaskQueue deterministicTaskQueue;

        private final TransportService transportService;

        private final ClusterService clusterService;

        private final RepositoriesService repositoriesService = mock(RepositoriesService.class);

        private final SnapshotsService snapshotsService;

        private final SnapshotShardsService snapshotShardsService;

        private final IndicesService indicesService;

        private final IndicesClusterStateService indicesClusterStateService;

        private final DiscoveryNode node;

        private final MasterService masterService;

        private final AllocationService allocationService;

        private final AtomicReference<ClusterState> currentState = new AtomicReference<>();

        private final NodeClient client;

        private final NodeEnvironment nodeEnv;

        private final MockTransport mockTransport = new MockTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                assertFalse(TestClusterNode.this.node.equals(node));
                final RequestHandlerRegistry<TransportRequest> handlerRegistry =
                    testClusterNodes.nodes.get(node.getName()).mockTransport.getRequestHandler(action);
                try {
                    SetOnce<TransportResponse> responseSetOnce = new SetOnce<>();
                    TransportChannel channel = mock(TransportChannel.class);
                    doAnswer(invocation -> {
                            responseSetOnce.set((TransportResponse) invocation.getArguments()[0]);
                            handleResponse(requestId, responseSetOnce.get());
                            return null;
                        }
                    ).when(channel).sendResponse(any(TransportResponse.class));
                    handlerRegistry.processMessageReceived(request, channel);
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            }
        };

        TestClusterNode(DiscoveryNode node, DeterministicTaskQueue deterministicTaskQueue) throws IOException {
            this.node = node;
            this.masterService = new FakeThreadPoolMasterService(node.getName(), "test", deterministicTaskQueue::scheduleNow);
            final Settings settings = Settings.builder()
                .put(NODE_NAME_SETTING.getKey(), node.getName())
                .put(PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath().toString())
                .build();
            this.allocationService = ESAllocationTestCase.createAllocationService(settings);
            ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            clusterService = new ClusterService(
                settings, clusterSettings, deterministicTaskQueue.getThreadPool(), masterService);
            this.deterministicTaskQueue = deterministicTaskQueue;
            transportService = mockTransport.createTransportService(
                settings, deterministicTaskQueue.getThreadPool(runnable -> onNode(node, runnable)), NOOP_TRANSPORT_INTERCEPTOR,
                a -> node, null, emptySet()
            );
            IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver();
            snapshotsService = new SnapshotsService(settings, clusterService, indexNameExpressionResolver,
                repositoriesService, deterministicTaskQueue.getThreadPool());
            final Environment environment = new Environment(settings, createTempDir());
            nodeEnv = new NodeEnvironment(settings, environment);
            final NamedXContentRegistry namedXContentRegistry = new NamedXContentRegistry(Collections.emptyList());
            ScriptService scriptService = new ScriptService(settings, emptyMap(), emptyMap());
            client = new NodeClient(settings, deterministicTaskQueue.getThreadPool());
            IndexScopedSettings indexScopedSettings = new IndexScopedSettings(settings, IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
            indicesService = new IndicesService(
                settings,
                mock(PluginsService.class),
                nodeEnv,
                namedXContentRegistry,
                new AnalysisRegistry(environment, emptyMap(), emptyMap(), emptyMap(), emptyMap(), emptyMap(),
                    emptyMap(), emptyMap(), emptyMap(), emptyMap()),
                indexNameExpressionResolver,
                new MapperRegistry(emptyMap(), emptyMap(), MapperPlugin.NOOP_FIELD_FILTER),
                new NamedWriteableRegistry(Collections.emptyList()),
                deterministicTaskQueue.getThreadPool(),
                indexScopedSettings,
                new NoneCircuitBreakerService(),
                new BigArrays(new PageCacheRecycler(settings), null, "test"),
                scriptService,
                client,
                new MetaStateService(nodeEnv, namedXContentRegistry),
                Collections.emptyList(),
                emptyMap()
            );
            RecoverySettings recoverySettings = new RecoverySettings(settings, clusterSettings);
            ActionFilters actionFilters = new ActionFilters(emptySet());
            snapshotShardsService = new SnapshotShardsService(
                settings, clusterService, snapshotsService, deterministicTaskQueue.getThreadPool(),
                transportService, indicesService, actionFilters, indexNameExpressionResolver);
            ShardStateAction shardStateAction = new ShardStateAction(
                clusterService, transportService, allocationService,
                new RoutingService(settings, clusterService, allocationService),
                deterministicTaskQueue.getThreadPool()
            );
            indicesClusterStateService = new IndicesClusterStateService(
                settings, indicesService, clusterService, deterministicTaskQueue.getThreadPool(),
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
                        settings, transportService, clusterService, indicesService, deterministicTaskQueue.getThreadPool(),
                        shardStateAction, actionFilters, indexNameExpressionResolver)
                ),
                new GlobalCheckpointSyncAction(
                    settings, transportService, clusterService, indicesService, deterministicTaskQueue.getThreadPool(),
                    shardStateAction, actionFilters, indexNameExpressionResolver)
            );
            client.initialize(
                Collections.singletonMap(
                    CreateIndexAction.INSTANCE,
                    new TransportCreateIndexAction(
                        transportService, clusterService, deterministicTaskQueue.getThreadPool(),
                        new MetaDataCreateIndexService(settings, clusterService, indicesService,
                            allocationService, new AliasValidator(), environment, indexScopedSettings,
                            deterministicTaskQueue.getThreadPool(), namedXContentRegistry, false),
                        actionFilters, indexNameExpressionResolver
                    )
                ),
                () -> clusterService.localNode().getId(),
                transportService.getRemoteClusterService()
            );
        }

        public void start(ClusterState initialState) {
            transportService.start();
            transportService.acceptIncomingRequests();
            snapshotsService.start();
            snapshotShardsService.start();
            masterService.setClusterStatePublisher((clusterChangedEvent, publishListener, ackListener) -> {
                testClusterNodes.nodes.values().forEach(n -> {
                    ClusterChangedEvent adjustedEvent = changeEventForNode(clusterChangedEvent, n.node);
                    n.snapshotsService.applyClusterState(adjustedEvent);
                    n.snapshotShardsService.clusterChanged(adjustedEvent);
                    n.indicesClusterStateService.applyClusterState(adjustedEvent);
                    n.currentState.set(adjustedEvent.state());
                });
                publishListener.onResponse(null);
                ackListener.onCommit(TimeValue.timeValueMillis(deterministicTaskQueue.getLatestDeferredExecutionTime()));
            });
            masterService.setClusterStateSupplier(currentState::get);
            if (node.isMasterNode()) {
                masterService.start();
            }
            ClusterState stateForNode = stateForNode(initialState, node);
            currentState.set(stateForNode);
            clusterService.getClusterApplierService().setInitialState(stateForNode);
            clusterService.getClusterApplierService().setNodeConnectionsService(new NodeConnectionsService(clusterService.getSettings(),
                deterministicTaskQueue.getThreadPool(), transportService));
            clusterService.getClusterApplierService().start();
            indicesService.start();
            indicesClusterStateService.start();
        }

        private Runnable onNode(DiscoveryNode node, Runnable runnable) {
            final String nodeId = '{' + node.getId() + "}{" + node.getEphemeralId() + '}';
            return () -> {
                try (CloseableThreadContext.Instance ignored = CloseableThreadContext.put("nodeId", nodeId)) {
                    runnable.run();
                }
            };
        }
    }
}
