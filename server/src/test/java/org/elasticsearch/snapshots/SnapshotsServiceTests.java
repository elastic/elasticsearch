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
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SnapshotsServiceTests extends ESTestCase {

    private AllocationService allocationService;
    private ClusterNode masterNode;
    private ClusterNode dataNode1;
    private ClusterNode dataNode2;

    @Before
    public void createServices() {
        masterNode = newMasterNode("local");
        dataNode1 = newDataNode("dataNode1");
        dataNode2 = newDataNode("dataNode2");
        allocationService = ESAllocationTestCase.createAllocationService(Settings.EMPTY);
    }

    /**
     * Starts 3 nodes (one master and 2 data nodes). Then creates a single index with a single shard
     * and one replica, adds the snapshot in progress for the primary shard's node, then removes
     * the primary shard allocation from the state and ensures that the snapshot completes.
     */
    public void testSnapshotWithOutOfSyncAllocationTable() throws Exception {
        // Set up fake repository
        String repoName = "repo";
        String snapshotName = "snapshot";
        final int shards = 1;
        final Repository repository = mock(Repository.class);
        when(repository.getRepositoryData()).thenReturn(RepositoryData.EMPTY);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            return new SnapshotInfo(
                (SnapshotId) args[0],
                ((List<IndexId>) args[1]).stream().map(IndexId::getName).collect(Collectors.toList()),
                ((List<IndexShard.ShardFailure>) args[5]).isEmpty() ? SnapshotState.SUCCESS : SnapshotState.FAILED
            );
        }).when(repository).finalizeSnapshot(any(), any(), anyLong(), any(), anyInt(), any(), anyLong(), anyBoolean());
        when(masterNode.repositoriesService.repository(repoName)).thenReturn(repository);
        when(dataNode1.repositoriesService.repository(repoName)).thenReturn(repository);
        when(dataNode2.repositoriesService.repository(repoName)).thenReturn(repository);

        CompletableFuture<Void> successfulSnapshotStart = new CompletableFuture<>();

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

        TestClusterState clusterState = new TestClusterState(
            allocateRouting(
                new ClusterState.Builder(ClusterName.DEFAULT)
                    .nodes(
                        DiscoveryNodes.builder()
                            .add(masterNode.node)
                            .add(dataNode1.node)
                            .add(dataNode2.node)
                            .localNodeId(masterNode.node.getId())
                            .masterNodeId(masterNode.node.getId())
                    )
                    .metaData(metaData)
                    .routingTable(RoutingTable.builder().addAsNew(metaData.index(index)).build())
                    .build()
            )
        );

        startServices();

        ClusterStateUpdateTask createSnapshotTask = expectOneUpdateTask(
            masterNode.clusterService,
            () -> masterNode.snapshotsService.createSnapshot(
                new SnapshotsService.SnapshotRequest(repoName, snapshotName, ""),
                new SnapshotsService.CreateSnapshotListener() {
                    @Override
                    public void onResponse() {
                        successfulSnapshotStart.complete(null);
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

        successfulSnapshotStart.get(0L, TimeUnit.SECONDS);

        String primaryNodeId = clusterState.currentState(masterNode.node.getId()).routingTable().allShards(index)
            .stream()
            .filter(ShardRouting::primary)
            .findFirst()
            .map(ShardRouting::currentNodeId)
            .orElseThrow(
                () -> new AssertionError("Expected to find primary allocation")
            );
        ClusterNode primaryNode = primaryNodeId.equals(dataNode1.node.getId()) ? dataNode1 : dataNode2;

        ClusterStateUpdateTask removeSnapshotTask;
        final boolean disconnectNode = randomBoolean();
        if (disconnectNode) {
            clusterState.disconnectNode(primaryNodeId);

            ClusterStateUpdateTask adjustSnapshotTask = expectOneUpdateTask(
                masterNode.clusterService,
                () -> clusterState.applyLatestChange(masterNode.node.getId(), masterNode.snapshotsService));

            // Notify the previous primary shard's SnapshotShardsService of the new Snapshot by reconnecting
            clusterState.connectNode(primaryNode.node, primaryNode.snapshotShardsService);
            assertTrue("SnapshotShardService did not enqueue a snapshot task.", primaryNode.deterministicTaskQueue.hasRunnableTasks());

            // Now fix the SnapshotsInProgress by running the cluster state update task on master
            clusterState.updateAndGet(adjustSnapshotTask);
            // Allow the state update task to run on the non-master node to avoid having to mock the master node action request handling.
            // This should be safe since we only capture the state update executor and task anyway and then manually run it.
            when(primaryNode.clusterService.state()).thenReturn(clusterState.currentState(masterNode.node.getId()));
            SetOnce<ClusterStateTaskExecutor<UpdateIndexShardSnapshotStatusRequest>> removeSnapshotTaskHolder = new SetOnce<>();
            SetOnce<UpdateIndexShardSnapshotStatusRequest> statusUpdateRequest = new SetOnce<>();
            doAnswer(invocation -> {
                Object[] arguments = invocation.getArguments();
                removeSnapshotTaskHolder.set((ClusterStateTaskExecutor<UpdateIndexShardSnapshotStatusRequest>) arguments[3]);
                statusUpdateRequest.set((UpdateIndexShardSnapshotStatusRequest) arguments[1]);
                return null;
            }).when(primaryNode.clusterService).submitStateUpdateTask(anyString(), any(), any(), any(), any());

            primaryNode.deterministicTaskQueue.runAllTasks();
            // The enqueued shard snapshot action must fail since we changed the routing table and the node isn't primary anymore.
            ClusterStateTaskExecutor<UpdateIndexShardSnapshotStatusRequest> removeSnapshotExecutor = removeSnapshotTaskHolder.get();
            assertNotNull("No cluster state update task executor enqueued even though a shard should have failed.", removeSnapshotExecutor);
            // Remove short-circuiting master action on primary shard's node
            when(primaryNode.clusterService.state()).thenReturn(clusterState.currentState(primaryNodeId));

            // Apply latest cluster state c
            clusterState.applyLatestChange(masterNode.node.getId(), masterNode.snapshotsService);

            assertNoWaitingTasks(dataNode1, dataNode2, masterNode);
            assertFalse(dataNode1.deterministicTaskQueue.hasRunnableTasks());
            assertFalse(dataNode2.deterministicTaskQueue.hasRunnableTasks());
            assertFalse(masterNode.deterministicTaskQueue.hasRunnableTasks());
            assertNotNull(statusUpdateRequest.get());

            // Run the cluster state update that was caused by the failing shard
            SnapshotsInProgress snapshotsInProgress = clusterState.updateState(
                currentClusterState -> {
                    try {
                        return removeSnapshotExecutor.execute(
                            currentClusterState, Collections.singletonList(statusUpdateRequest.get())).resultingState;
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                }
            ).custom(SnapshotsInProgress.TYPE);
            // The Snapshot should not yet have been removed from the state.
            assertThat(snapshotsInProgress.entries(), hasSize(shards));

            // The failing shard must result in a task for removing the SnapshotInProgress from the cluster state
            // that we capture and run.
        } else {
            clusterState.handleLatestChange(primaryNodeId, primaryNode.snapshotShardsService);
            assertHasTasks(primaryNode);
            IndexService indexService = mock(IndexService.class);
            ClusterState currentState = clusterState.currentState(primaryNodeId);
            IndexShard indexShard = mock(IndexShard.class);
            when(indexShard.acquireLastIndexCommit(anyBoolean())).thenReturn(
                new Engine.IndexCommitRef(null,
                    () -> {
                    })
            );
            when(indexShard.routingEntry()).thenReturn(currentState.routingTable().index(index).shard(0).primaryShard());
            doAnswer(invocation -> indexShard).when(indexService).getShardOrNull(anyInt());
            when(primaryNode.clusterService.state()).thenReturn(clusterState.currentState(masterNode.node.getId()));
            when(
                primaryNode.indicesService.indexServiceSafe(
                    currentState.metaData().index(index).getIndex()
                )
            ).thenReturn(indexService);
            SetOnce<ClusterStateTaskExecutor<UpdateIndexShardSnapshotStatusRequest>> removeSnapshotTaskHolder = new SetOnce<>();
            SetOnce<UpdateIndexShardSnapshotStatusRequest> statusUpdateRequest = new SetOnce<>();
            doAnswer(invocation -> {
                Object[] arguments = invocation.getArguments();
                removeSnapshotTaskHolder.set((ClusterStateTaskExecutor<UpdateIndexShardSnapshotStatusRequest>) arguments[3]);
                statusUpdateRequest.set((UpdateIndexShardSnapshotStatusRequest) arguments[1]);
                return null;
            }).when(primaryNode.clusterService).submitStateUpdateTask(anyString(), any(), any(), any(), any());
            primaryNode.deterministicTaskQueue.runAllTasks();
            // Run the cluster state update that was caused by the failing shard
            SnapshotsInProgress snapshotsInProgress = clusterState.updateState(
                currentClusterState -> {
                    try {
                        return removeSnapshotTaskHolder.get().execute(
                            currentClusterState, Collections.singletonList(statusUpdateRequest.get())).resultingState;
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                }
            ).custom(SnapshotsInProgress.TYPE);
            // The Snapshot should not yet have been removed from the state.
            assertThat(snapshotsInProgress.entries(), hasSize(shards));

            // The failing shard must result in a task for removing the SnapshotInProgress from the cluster state
            // that we capture and run.
        }
        assertHasTasks(primaryNode);
        removeSnapshotTask = expectOneUpdateTask(primaryNode.clusterService, primaryNode.deterministicTaskQueue::runAllTasks);
        assertNoSnapshotsInProgress(clusterState.updateAndGet(removeSnapshotTask));
    }

    private static void assertNoWaitingTasks(ClusterNode ... nodes) {
        for(ClusterNode node: nodes) {
            assertFalse(node.deterministicTaskQueue.hasRunnableTasks());
        }
    }

    private static void assertHasTasks(ClusterNode node) {
        assertTrue(node.deterministicTaskQueue.hasRunnableTasks());
    }

    private static void assertNoSnapshotsInProgress(ClusterState clusterState) {
        SnapshotsInProgress finalSnapshotsInProgress = clusterState.custom(SnapshotsInProgress.TYPE);

        assertThat(finalSnapshotsInProgress.entries(), empty());
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
        masterNode.start();
        dataNode1.start();
        dataNode2.start();
    }

    private static ClusterNode newMasterNode(String nodeName) {
        return newNode(nodeName, DiscoveryNode.Role.MASTER);
    }

    private static ClusterNode newDataNode(String nodeName) {
        return newNode(nodeName, DiscoveryNode.Role.DATA);
    }

    private static ClusterNode newNode(String nodeName, DiscoveryNode.Role role) {
        return new ClusterNode(
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
            current = ClusterState.builder(current)
                .nodes(DiscoveryNodes.builder(current.nodes()).add(node).build()).incrementVersion().build();
            clusterStateListener.clusterChanged(
                new ClusterChangedEvent("", currentState(node.getId()), initialState(node.getId()))
            );
        }

        public void applyLatestChange(String nodeId, ClusterStateApplier clusterStateApplier) {
            clusterStateApplier.applyClusterState(
                new ClusterChangedEvent("", currentState(nodeId), initialState(nodeId))
            );
        }

        public void handleLatestChange(String nodeId, ClusterStateListener clusterStateApplier) {
            clusterStateApplier.clusterChanged(
                new ClusterChangedEvent("", currentState(nodeId), initialState(nodeId))
            );
        }
    }

    private static final class ClusterNode {

        private final DeterministicTaskQueue deterministicTaskQueue;

        private final MockTransportService transportService;

        private final ClusterService clusterService = mock(ClusterService.class);

        private final RepositoriesService repositoriesService = mock(RepositoriesService.class);

        private final SnapshotsService snapshotsService;

        private final SnapshotShardsService snapshotShardsService;

        private final IndicesService indicesService;

        private final DiscoveryNode node;

        ClusterNode(DiscoveryNode node, DeterministicTaskQueue deterministicTaskQueue) {
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
