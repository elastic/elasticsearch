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

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.coordination.DeterministicTaskQueue;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.SnapshotShardsService.UpdateIndexShardSnapshotStatusRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SnapshotsServiceTests extends ESTestCase {

    private AllocationService allocationService;
    private TestClusterNodes testClusterNodes;

    @Before
    public void createServices() {
        testClusterNodes = new TestClusterNodes(1, randomIntBetween(2, 10));
        allocationService = ESAllocationTestCase.createAllocationService(Settings.EMPTY);
    }

    /**
     * Starts multiple nodes (one master and multiple data nodes). Then creates a single index with a single shard
     * and one replica, adds the snapshot in progress for the primary shard's node, then removes
     * the primary shard allocation from the state and ensures that the snapshot completes.
     */
    public void testSnapshotWithOutOfSyncAllocationTable() throws Exception {
        // Set up fake repository
        String repoName = "repo";
        String snapshotName = "snapshot";
        final int shards = randomIntBetween(1, 10);
        final Repository repository = mock(Repository.class);
        when(repository.getRepositoryData()).thenReturn(RepositoryData.EMPTY);
        testClusterNodes.nodes.values().forEach(
            node -> when(node.repositoriesService.repository(repoName)).thenReturn(repository)
        );

        String index = randomAlphaOfLength(10);
        MetaData metaData = MetaData.builder().putCustom(
            RepositoriesMetaData.TYPE,
            new RepositoriesMetaData(
                Collections.singletonList(
                    new RepositoryMetaData(
                        repoName, randomAlphaOfLength(10), Settings.EMPTY
                    )
                )
            )
        ).put(IndexMetaData.builder(index)
            .settings(settings(Version.CURRENT))
            .numberOfShards(shards)
            .numberOfReplicas(1)
        ).build();

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            return new SnapshotInfo(
                (SnapshotId) args[0],
                ((List<IndexId>) args[1]).stream().map(IndexId::getName).collect(Collectors.toList()),
                ((List<IndexShard.ShardFailure>) args[5]).isEmpty() ? SnapshotState.SUCCESS : SnapshotState.FAILED
            );
        }).when(repository).finalizeSnapshot(
            any(), anyListOf(IndexId.class), anyLong(), anyString(), eq(shards), any(), anyLong(), anyBoolean());

        TestClusterState clusterState = new TestClusterState(
            allocateRouting(
                new ClusterState.Builder(ClusterName.DEFAULT)
                    .nodes(testClusterNodes.randomDiscoveryNodes())
                    .metaData(metaData)
                    .routingTable(RoutingTable.builder().addAsNew(metaData.index(index)).build())
                    .build()
            )
        );

        startServices();

        TestClusterNode masterNode = testClusterNodes.currentMaster(clusterState);

        SetOnce<Boolean> successfulSnapshotStart = new SetOnce<>();
        ClusterStateUpdateTask createSnapshotTask = expectOneUpdateTask(
            masterNode.clusterService,
            () -> masterNode.snapshotsService.createSnapshot(
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
                }));
        clusterState.updateAndGet(createSnapshotTask);
        assertTrue(masterNode.deterministicTaskQueue.hasRunnableTasks());
        ClusterStateUpdateTask beginSnapshotTask = expectOneUpdateTask(
            masterNode.clusterService, () -> masterNode.deterministicTaskQueue.runAllTasks());
        clusterState.updateAndGet(beginSnapshotTask);

        assertTrue(successfulSnapshotStart.get());
        List<ShardId> shardIds = clusterState.current.routingTable().allShards(index).stream()
            .map(ShardRouting::shardId)
            .distinct()
            .collect(Collectors.toList());
        final Map<Integer, IndexShard> indexShards = new HashMap<>();
        final Map<Index, IndexService> indicesServices = new HashMap<>();
        String masterNodeId = masterNode.node.getId();
        final Runnable masterSnapshotServiceUpdate =
            () -> clusterState.applyLatestChange(masterNodeId, masterNode.snapshotsService);

        for (final ShardId shardId : shardIds) {
            if (snapshots(clusterState).entries().get(0).state().completed()) {
                // If the snapshot is completed just break out
                break;
            }
            Optional<TestClusterNode> primaryNodeHolder = testClusterNodes.primaryForShard(clusterState, shardId);
            if (primaryNodeHolder.isPresent() == false) {
                // The shard's primary is not assigned, if it hasn't been snapshotted yet we trigger an update
                // on the master's snapshot service.
                if (snapshots(clusterState).entries().get(0).shards().get(shardId).state().completed() == false) {
                    clusterState.updateAndGet(expectOneUpdateTask(masterNode.clusterService, masterSnapshotServiceUpdate));
                }
                continue;
            }
            TestClusterNode primaryNode = primaryNodeHolder.get();
            String primaryNodeId = primaryNode.node.getId();
            final boolean disconnectNode = randomBoolean();
            if (disconnectNode) {
                if (snapshots(clusterState).entries().get(0).shards().get(shardId).state().completed()) {
                    continue;
                }
                clusterState.disconnectNode(primaryNodeId);
                ClusterStateUpdateTask adjustSnapshotTask = expectOneUpdateTask(masterNode.clusterService, masterSnapshotServiceUpdate);
                // Notify the previous primary shard's SnapshotShardsService of the new Snapshot by reconnecting
                clusterState.connectNode(primaryNode.node, primaryNode.snapshotShardsService);
                // Now fix the SnapshotsInProgress by running the cluster state update task on master
                clusterState.updateAndGet(adjustSnapshotTask);
            } else {
                clusterState.handleLatestChange(primaryNodeId, primaryNode.snapshotShardsService);
                when(primaryNode.clusterService.state()).thenReturn(clusterState.currentState(masterNode.node.getId()));
                doAnswer(
                    invocation -> indicesServices.computeIfAbsent((Index) invocation.getArguments()[0], k -> {
                        IndexService indexService = mock(IndexService.class);
                        doAnswer(in -> {
                            Integer key = (Integer) in.getArguments()[0];
                            return indexShards.computeIfAbsent(
                                key, kk -> {
                                    IndexShard indexShard = mock(IndexShard.class);
                                    when(indexShard.acquireLastIndexCommit(anyBoolean())).thenReturn(
                                        new Engine.IndexCommitRef(null,
                                            () -> {
                                            })
                                    );
                                    doAnswer(ignored ->
                                        clusterState.currentState(primaryNodeId).routingTable().index(index)
                                            .shard(shardId.id()).primaryShard()
                                    ).when(indexShard).routingEntry();
                                    return indexShard;
                                });
                        }).when(indexService).getShardOrNull(anyInt());
                        return indexService;
                    })).when(primaryNode.indicesService).indexServiceSafe(any());
            }
            // Use linked hash map to get deterministic order in snapshot state update tasks
            Map<UpdateIndexShardSnapshotStatusRequest, ClusterStateTaskExecutor<UpdateIndexShardSnapshotStatusRequest>>
                removeSnapshotTaskHolder = new LinkedHashMap<>();
            doAnswer(invocation -> {
                Object[] arguments = invocation.getArguments();
                assertNull(
                    removeSnapshotTaskHolder.put(
                        (UpdateIndexShardSnapshotStatusRequest) arguments[1],
                        (ClusterStateTaskExecutor<UpdateIndexShardSnapshotStatusRequest>) arguments[3]));
                return null;
            }).when(primaryNode.clusterService).submitStateUpdateTask(anyString(), any(), any(), any(), any());

            // Allow the state update task to run on the non-master node to avoid having to mock the master node action request
            // handling.
            // This should be safe since we only capture the state update executor and task anyway and then manually run it.
            when(primaryNode.clusterService.state()).thenReturn(clusterState.currentState(masterNodeId));
            primaryNode.deterministicTaskQueue.runAllTasks();
            // Remove state update hack
            when(primaryNode.clusterService.state()).thenReturn(clusterState.currentState(primaryNodeId));

            // The enqueued shard snapshot action must fail since we changed the routing table and the node isn't primary anymore.
            removeSnapshotTaskHolder.forEach((statusUpdateRequest, failSnapshotExecutor) -> {
                // Apply latest cluster state
                clusterState.applyLatestChange(masterNodeId, masterNode.snapshotsService);

                // Make sure we're not missing any tasks that could run in parallel to the state updates
                assertNoWaitingTasks(testClusterNodes.nodes.values());

                // Run the cluster state update that was caused by the failing or successful shard
                clusterState.updateState(
                    currentClusterState -> {
                        try {
                            return failSnapshotExecutor.execute(
                                currentClusterState, Collections.singletonList(statusUpdateRequest)).resultingState;
                        } catch (Exception e) {
                            throw new IllegalStateException(e);
                        }
                    }
                );
            });
        }
        assertThat(snapshots(clusterState).entries(), hasSize(1));
        if (snapshots(clusterState).entries().get(0).state().completed() == false) {
            List<TestClusterNode> primariesWithTasks = testClusterNodes.nodes.values().stream()
                .filter(node -> node.deterministicTaskQueue.hasRunnableTasks()).collect(Collectors.toList());
            for (TestClusterNode primary : primariesWithTasks) {
                when(primary.clusterService.state()).thenReturn(clusterState.currentState(masterNodeId));
                if (clusterState.current.<SnapshotsInProgress>custom(SnapshotsInProgress.TYPE).entries().isEmpty()) {
                    break;
                }
                ClusterStateUpdateTask updateTask =
                    expectOneUpdateTask(primary.clusterService, primary.deterministicTaskQueue::runAllTasks);
                clusterState.updateAndGet(updateTask);
                when(primary.clusterService.state()).thenReturn(clusterState.currentState(primary.node.getId()));
            }
            if (snapshots(clusterState).entries().get(0).state().completed() == false) {
                ClusterStateUpdateTask adjustSnapshotTask = expectOneUpdateTask(masterNode.clusterService, masterSnapshotServiceUpdate);
                clusterState.updateAndGet(adjustSnapshotTask);
            }
        }
        assertNoSnapshotsInProgress(clusterState.current);
    }

    private static SnapshotsInProgress snapshots(TestClusterState state) {
        return state.current.custom(SnapshotsInProgress.TYPE);
    }

    private static void assertNoWaitingTasks(Collection<TestClusterNode> nodes) {
        for (TestClusterNode node : nodes) {
            assertFalse(node.deterministicTaskQueue.hasRunnableTasks());
        }
    }

    private static void assertNoSnapshotsInProgress(ClusterState clusterState) {
        SnapshotsInProgress finalSnapshotsInProgress = clusterState.custom(SnapshotsInProgress.TYPE);
        assertFalse(finalSnapshotsInProgress.entries().stream().anyMatch(entry -> !entry.state().completed()));
    }

    /**
     * Execute given {@link Runnable} and expect a single {@link ClusterStateUpdateTask} to be
     * submitted to the given {@link ClusterService}.
     * @param clusterService ClusterService that should receive state update task
     * @param action Action to run
     * @return ClusterStateUpdateTask received by cluster service
     */
    private static ClusterStateUpdateTask expectOneUpdateTask(ClusterService clusterService, Runnable action) {
        SetOnce<ClusterStateUpdateTask> taskHolder = new SetOnce<>();
        doAnswer(invocation -> {
            taskHolder.set((ClusterStateUpdateTask) invocation.getArguments()[1]);
            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), any());
        action.run();
        ClusterStateUpdateTask updateTask = taskHolder.get();
        assertNotNull("Expected a new cluster state update task to be submitted", updateTask);
        return updateTask;
    }

    private void startServices() {
        testClusterNodes.nodes.values().forEach(TestClusterNode::start);
    }

    private static TestClusterNode newMasterNode(String nodeName) {
        return newNode(nodeName, DiscoveryNode.Role.MASTER);
    }

    private static TestClusterNode newDataNode(String nodeName) {
        return newNode(nodeName, DiscoveryNode.Role.DATA);
    }

    private static TestClusterNode newNode(String nodeName, DiscoveryNode.Role role) {
        return new TestClusterNode(
            new DiscoveryNode(nodeName, randomAlphaOfLength(10), buildNewFakeTransportAddress(), emptyMap(),
                Collections.singleton(role), Version.CURRENT),
            new DeterministicTaskQueue(Settings.builder().put(NODE_NAME_SETTING.getKey(), nodeName).build(), random())
        );
    }

    private ClusterState allocateRouting(ClusterState state) {
        allocationService.deassociateDeadNodes(state, false, "");
        state = allocationService.reroute(state, "reroute");
        // starting primaries
        state = allocationService.applyStartedShards(
            state, state.getRoutingNodes().shardsWithState(INITIALIZING));
        // starting replicas
        state = allocationService.applyStartedShards(
            state, state.getRoutingNodes().shardsWithState(INITIALIZING));
        return state;
    }

    private final class TestClusterNodes {

        private final Map<String, TestClusterNode> nodes = new HashMap<>();

        TestClusterNodes(int masterNodes, int dataNodes) {
            for (int i = 0; i < masterNodes; ++i) {
                nodes.computeIfAbsent("node" + i, SnapshotsServiceTests::newMasterNode);
            }
            for (int i = masterNodes; i < dataNodes + masterNodes; ++i) {
                nodes.computeIfAbsent("node" + i, SnapshotsServiceTests::newDataNode);
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

        public TestClusterNode currentMaster(TestClusterState clusterState) {
            TestClusterNode master = nodes.get(clusterState.current.nodes().getMasterNode().getName());
            assertNotNull(master);
            assertTrue(master.node.isMasterNode());
            return master;
        }

        public Optional<TestClusterNode> primaryForShard(TestClusterState clusterState, ShardId shardId) {
            return clusterState.current.routingTable().allShards().stream()
                .filter(
                    shardRouting -> shardRouting.assignedToNode() && shardRouting.primary() && shardId.equals(shardRouting.shardId()))
                .findFirst()
                .flatMap(
                    shardRouting -> nodes.values().stream()
                        .filter(node -> shardRouting.currentNodeId().equals(node.node.getId()))
                        .findFirst()
                );
        }
    }

    /**
     * Holds the current cluster state, its predecessor and the initial cluster state.
     */
    private final class TestClusterState {

        private final ClusterState initialState;

        private ClusterState current;

        private ClusterState previous;

        TestClusterState(ClusterState initialState) {
            this.initialState = initialState;
            this.current = initialState;
        }

        public ClusterState currentState(String nodeId) {
            return ClusterState.builder(current).nodes(DiscoveryNodes.builder(current.nodes()).localNodeId(nodeId)).build();
        }

        public ClusterState previousState(String nodeId) {
            return ClusterState.builder(previous).nodes(DiscoveryNodes.builder(previous.nodes()).localNodeId(nodeId)).build();
        }

        public ClusterState initialState(String nodeId) {
            return ClusterState.builder(initialState).nodes(DiscoveryNodes.builder(previous.nodes()).localNodeId(nodeId)).build();
        }

        /**
         * Run given {@link ClusterStateUpdateTask} and return the resulting {@link ClusterState}
         * on the master node
         * @param stateUpdateTask ClusterStateUpdateTask to run
         * @return Resulting ClusterState on master
         */
        public ClusterState updateAndGet(ClusterStateUpdateTask stateUpdateTask) throws Exception {
            previous = currentState(current.nodes().getMasterNodeId());
            ClusterState currentClusterState = stateUpdateTask.execute(previous);
            stateUpdateTask.clusterStateProcessed("", previous, currentClusterState);
            current = ClusterState.builder(currentClusterState).incrementVersion().build();
            return currentClusterState;
        }

        public ClusterState updateState(Function<ClusterState, ClusterState> updater) {
            previous = currentState(current.nodes().getMasterNodeId());
            current = ClusterState.builder(updater.apply(previous)).incrementVersion().build();
            return current;
        }

        /**
         * Remove given node from cluster state
         * @param nodeId Node id to remove from cluster state
         */
        public void disconnectNode(String nodeId) {
            previous = current;
            current = allocationService.deassociateDeadNodes(
                ClusterState.builder(current)
                    .nodes(DiscoveryNodes.builder(current.nodes()).remove(nodeId).build()).incrementVersion().build()
                , false, ""
            );
        }

        /**
         * Reconnect a node and invoke {@link ClusterStateListener#clusterChanged(ClusterChangedEvent)} for the given listener
         * after reconnect.
         * @param node Node to connect
         * @param clusterStateListener ClusterStateListener to invoke
         */
        public void connectNode(DiscoveryNode node, ClusterStateListener clusterStateListener) {
            previous = current;
            current = allocateRouting(ClusterState.builder(current)
                .nodes(DiscoveryNodes.builder(current.nodes()).add(node).build()).incrementVersion().build());
            clusterStateListener.clusterChanged(
                new ClusterChangedEvent("", currentState(node.getId()), initialState(node.getId()))
            );
        }

        public void applyLatestChange(String nodeId, ClusterStateApplier clusterStateApplier) {
            clusterStateApplier.applyClusterState(
                new ClusterChangedEvent("", currentState(nodeId), previousState(nodeId))
            );
        }

        public void handleLatestChange(String nodeId, ClusterStateListener clusterStateApplier) {
            clusterStateApplier.clusterChanged(
                new ClusterChangedEvent("", currentState(nodeId), previousState(nodeId))
            );
        }
    }

    private static final class TestClusterNode {

        private final DeterministicTaskQueue deterministicTaskQueue;

        private final MockTransportService transportService;

        private final ClusterService clusterService = mock(ClusterService.class);

        private final RepositoriesService repositoriesService = mock(RepositoriesService.class);

        private final SnapshotsService snapshotsService;

        private final SnapshotShardsService snapshotShardsService;

        private final IndicesService indicesService;

        private final DiscoveryNode node;

        TestClusterNode(DiscoveryNode node, DeterministicTaskQueue deterministicTaskQueue) {
            this.node = node;
            when(clusterService.localNode()).thenReturn(node);
            when(clusterService.getClusterApplierService()).thenReturn(mock(ClusterApplierService.class));
            this.deterministicTaskQueue = deterministicTaskQueue;
            transportService = new MockTransportService(
                Settings.EMPTY,
                new MockTransport() {
                    @Override
                    protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                        throw new AssertionError("unexpected " + action);
                    }
                },
                deterministicTaskQueue.getThreadPool(),
                TransportService.NOOP_TRANSPORT_INTERCEPTOR, boundTransportAddress -> node,
                null, emptySet()
            );
            IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver();
            snapshotsService = new SnapshotsService(Settings.EMPTY, clusterService, indexNameExpressionResolver,
                repositoriesService, deterministicTaskQueue.getThreadPool());
            indicesService = mock(IndicesService.class);
            snapshotShardsService = new SnapshotShardsService(
                Settings.EMPTY, clusterService, snapshotsService, deterministicTaskQueue.getThreadPool(),
                transportService, indicesService, new ActionFilters(emptySet()), indexNameExpressionResolver);
        }

        public void start() {
            transportService.start();
            transportService.acceptIncomingRequests();
            snapshotsService.start();
            snapshotShardsService.start();
        }
    }
}
