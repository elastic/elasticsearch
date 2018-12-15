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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
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

    private ClusterNode masterNode;
    private ClusterNode dataNode1;
    private ClusterNode dataNode2;

    @Before
    public void createServices() {
        masterNode = newMasterNode("local");
        dataNode1 = newDataNode("dataNode1");
        dataNode2 = newDataNode("dataNode2");
    }

    public void testSnapshotWithOutOfSyncAllocationTable() throws Exception {
        String repoName = "repo";
        String snapshotName = "snapshot";
        final Repository repository = mock(Repository.class);
        when(repository.getRepositoryData()).thenReturn(RepositoryData.EMPTY);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            return new SnapshotInfo(
                (SnapshotId) args[0],
                ((List<IndexId>) args[1]).stream().map(IndexId::getName).collect(Collectors.toList()),
                SnapshotState.SUCCESS
            );
        }).when(repository).finalizeSnapshot(any(), any(), anyLong(), any(), anyInt(), any(), anyLong(), anyBoolean());
        when(masterNode.repositoriesService.repository(repoName)).thenReturn(repository);
        when(dataNode1.repositoriesService.repository(repoName)).thenReturn(repository);
        when(dataNode2.repositoriesService.repository(repoName)).thenReturn(repository);

        CompletableFuture<Void> successfulSnapshotStart = new CompletableFuture<>();

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
        ).put(IndexMetaData.builder(index)
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
        ).build();

        TestClusterState clusterState = new TestClusterState(
            initializeRouting(
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

        BlockingQueue<ClusterStateUpdateTask> clusterStateUpdateTasks = new ArrayBlockingQueue<>(1);
        doAnswer(invocation -> {
            clusterStateUpdateTasks.add((ClusterStateUpdateTask) invocation.getArguments()[1]);
            return null;
        }).when(masterNode.clusterService).submitStateUpdateTask(any(), any());

        masterNode.snapshotsService.createSnapshot(
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
            });
        ClusterStateUpdateTask createSnapshotTask = clusterStateUpdateTasks.poll(0L, TimeUnit.MILLISECONDS);
        assertNotNull("Should have created create snapshot task.", createSnapshotTask);
        clusterState.runStateUpdateTask(createSnapshotTask);
        assertTrue(masterNode.deterministicTaskQueue.hasRunnableTasks());
        masterNode.deterministicTaskQueue.runAllTasks();
        ClusterStateUpdateTask beginSnapshotTask = clusterStateUpdateTasks.poll(0L, TimeUnit.MILLISECONDS);
        assertNotNull("Should have created begin snapshot task", beginSnapshotTask);
        clusterState.runStateUpdateTask(beginSnapshotTask);

        successfulSnapshotStart.get(0L, TimeUnit.SECONDS);

        String primaryNode = clusterState.currentState(masterNode.node.getId()).routingTable().allShards(index)
            .stream()
            .filter(ShardRouting::primary)
            .findFirst()
            .map(ShardRouting::currentNodeId)
            .orElseThrow(
                () -> new AssertionError("Expected to find primary allocation")
            );
        ClusterNode primary = primaryNode.equals(dataNode1.node.getId()) ? dataNode1 : dataNode2;

        // Notify the previous primary shard's SnapshotShardsService of the new Snapshot
        clusterState.notifyLastChange(primaryNode, primary.snapshotShardsService);
        assertTrue("Primary shard did not enqueue a snapshot task.", primary.deterministicTaskQueue.hasRunnableTasks());

        // Allow the state update task to run on the non-master node to avoid having to mock the master node action request handling.
        // This should be safe since we only capture the state update executor and task anyway and then manually run it.
        when(primary.clusterService.state()).thenReturn(clusterState.currentState(masterNode.node.getId()));
        SetOnce<ClusterStateTaskExecutor<UpdateIndexShardSnapshotStatusRequest>> taskExec = new SetOnce<>();
        SetOnce<UpdateIndexShardSnapshotStatusRequest> statusUpdateRequest = new SetOnce<>();
        doAnswer(invocation -> {
            Object[] arguments = invocation.getArguments();
            taskExec.set((ClusterStateTaskExecutor<UpdateIndexShardSnapshotStatusRequest>) arguments[3]);
            statusUpdateRequest.set((UpdateIndexShardSnapshotStatusRequest) arguments[1]);
            return null;
        }).when(primary.clusterService).submitStateUpdateTask(anyString(), any(), any(), any(), any());


        // The enqueued shard snapshot action must fail since we changed the routing table and the node isn't primary anymore.
        primary.deterministicTaskQueue.runAllTasks();
        // Remove short-circuiting master action on primary shard's node
        when(primary.clusterService.state()).thenReturn(clusterState.currentState(primaryNode));

        assertFalse(dataNode1.deterministicTaskQueue.hasRunnableTasks());
        assertFalse(dataNode2.deterministicTaskQueue.hasRunnableTasks());
        assertFalse(masterNode.deterministicTaskQueue.hasRunnableTasks());
        assertNotNull("No cluster state update task executor enqueued even though a shard should have failed.", taskExec.get());
        assertNotNull(statusUpdateRequest.get());

        // Run the cluster state update that was caused by the failing shard
        SnapshotsInProgress snapshotsInProgress = clusterState.updateState(
            currentClusterState -> {
                try {
                    ClusterStateTaskExecutor.ClusterTasksResult<UpdateIndexShardSnapshotStatusRequest> updateFailedSnapshotStateResult =
                        taskExec.get().execute(currentClusterState, Collections.singletonList(statusUpdateRequest.get()));
                    return updateFailedSnapshotStateResult.resultingState;
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        ).custom(SnapshotsInProgress.TYPE);
        // The Snapshot should not yet have been removed from the state.
        assertThat(snapshotsInProgress.entries(), hasSize(1));

        // The failing shard must result in a task for removing the SnapshotInProgress from the cluster state
        // that we capture and run.
        assertTrue(primary.deterministicTaskQueue.hasRunnableTasks());
        SetOnce<ClusterStateUpdateTask> removeSnapshotTaskHolder = new SetOnce<>();
        doAnswer(invocation -> {
            removeSnapshotTaskHolder.set((ClusterStateUpdateTask) invocation.getArguments()[1]);
            return null;
        }).when(primary.clusterService).submitStateUpdateTask(anyString(), any());
        primary.deterministicTaskQueue.runAllTasks();
        ClusterStateUpdateTask removeSnapshotTask = removeSnapshotTaskHolder.get();
        assertNotNull(removeSnapshotTask);
        snapshotsInProgress = clusterState.runStateUpdateTask(removeSnapshotTask).custom(SnapshotsInProgress.TYPE);
        assertThat(snapshotsInProgress.entries(), empty());
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

    private static ClusterState initializeRouting(ClusterState state) {
        AllocationService allocationService = ESAllocationTestCase.createAllocationService(Settings.EMPTY);
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
     * Holds the current cluster state and its predecessor.
     */
    private static final class TestClusterState {

        private ClusterState current;

        private ClusterState previous;

        TestClusterState(ClusterState initialState) {
            this.current = initialState;
        }

        public ClusterState currentState(String nodeId) {
            return ClusterState.builder(current).nodes(DiscoveryNodes.builder(current.nodes()).localNodeId(nodeId)).build();
        }

        public ClusterState previousState(String nodeId) {
            return ClusterState.builder(previous).nodes(DiscoveryNodes.builder(previous.nodes()).localNodeId(nodeId)).build();
        }

        public ClusterState runStateUpdateTask(ClusterStateUpdateTask stateUpdateTask) throws Exception {
            previous = currentState(current.nodes().getMasterNodeId());
            ClusterState currentClusterState = stateUpdateTask.execute(previous);
            stateUpdateTask.clusterStateProcessed("", previous, currentClusterState);
            current = currentClusterState;
            return currentClusterState;
        }

        public ClusterState updateState(Function<ClusterState, ClusterState> updater) {
            previous = currentState(current.nodes().getMasterNodeId());
            current = updater.apply(previous);
            return current;
        }

        /**
         * Invoke {@link ClusterStateListener#clusterChanged(ClusterChangedEvent)} for the given listener
         * on the given node.
         * @param nodeId NodeId that the ClusterStateListener resides on
         * @param clusterStateListener ClusterStateListener
         */
        public void notifyLastChange(String nodeId, ClusterStateListener clusterStateListener) {
            assertNotNull(previous);
            clusterStateListener.clusterChanged(
                new ClusterChangedEvent("", currentState(nodeId), previousState(nodeId))
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
            snapshotShardsService = new SnapshotShardsService(
                Settings.EMPTY, clusterService, snapshotsService, deterministicTaskQueue.getThreadPool(),
                transportService, mock(IndicesService.class), new ActionFilters(emptySet()), indexNameExpressionResolver);
        }

        public void start() {
            transportService.start();
            transportService.acceptIncomingRequests();
            snapshotsService.start();
            snapshotShardsService.start();
        }
    }

}
