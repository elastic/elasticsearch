/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.engine;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.FakeThreadPoolMasterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.INDEX_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.MASTER_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.SEARCH_ROLE;
import static org.hamcrest.Matchers.equalTo;

public class RefreshThrottlingServiceTests extends ESTestCase {

    /**
     * The following tests simulate nodes being added and removed from the cluster. The first node action added is the master.
     * Each subsequent node action adds or removes an index or search node with a specific memory.
     * At each node action, the resulting hardware multiplier on the master node's {@link RefreshThrottlingService}  is asserted to be
     * equal to the given masterMultiplier argument.
     */
    private record NodeAction(int index, boolean remove, boolean search, ByteSizeValue memory, double masterMultiplier) {}

    private void simulateNodeActions(List<NodeAction> nodeActions) {
        Map<Integer, DiscoveryNode> activeNodes = new HashMap<>();
        assert nodeActions.size() > 0 && nodeActions.get(0).index() == 0 && nodeActions.get(0).remove() == false;

        String nodeIdPrefix = "test";
        String masterNodeId = nodeIdPrefix + "0";
        final DiscoveryNode masterNode = DiscoveryNodeUtils.create(
            null,
            masterNodeId,
            ESTestCase.buildNewFakeTransportAddress(),
            Map.of(RefreshThrottlingService.MEMORY_NODE_ATTR, String.valueOf(nodeActions.get(0).memory().getBytes())),
            Set.of(MASTER_ROLE, nodeActions.get(0).search() ? SEARCH_ROLE : INDEX_ROLE)
        );
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(Node.NODE_NAME_SETTING.getKey(), masterNode.getName())
            .put("node.roles", nodeActions.get(0).search() ? SEARCH_ROLE.roleName() : INDEX_ROLE.roleName())
            // disable thread watchdog to avoid infinitely repeating task
            .put(ClusterApplierService.CLUSTER_APPLIER_THREAD_WATCHDOG_INTERVAL.getKey(), TimeValue.ZERO);
        final Settings settings = settingsBuilder.build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();
        final ThreadPool threadPool = deterministicTaskQueue.getThreadPool();

        final ClusterApplierService clusterApplierService = new ClusterApplierService(masterNodeId, settings, clusterSettings, threadPool) {
            @Override
            protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
                return deterministicTaskQueue.getPrioritizedEsThreadPoolExecutor();
            }
        };

        final MasterService masterService = new FakeThreadPoolMasterService(masterNodeId, threadPool, deterministicTaskQueue::scheduleNow);
        final ClusterService clusterService = new ClusterService(settings, clusterSettings, masterService, clusterApplierService);
        RefreshThrottlingService refreshThrottlingService = new RefreshThrottlingService(settings, clusterService);
        refreshThrottlingService.start();

        clusterService.setNodeConnectionsService(ClusterServiceUtils.createNoOpNodeConnectionsService());
        var initialState = ClusterState.builder(new ClusterName("cluster"))
            .nodes(DiscoveryNodes.builder().add(masterNode).localNodeId(masterNodeId).masterNodeId(masterNodeId))
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();

        clusterApplierService.setInitialState(initialState);
        masterService.setClusterStatePublisher(ClusterServiceUtils.createClusterStatePublisher(clusterService.getClusterApplierService()));
        masterService.setClusterStateSupplier(clusterApplierService::state);
        clusterService.start();
        assertThat(
            refreshThrottlingService.getRegularIndicesCreditManager().getMultiplier(),
            equalTo(nodeActions.get(0).masterMultiplier())
        );
        assertThat(
            refreshThrottlingService.getSystemIndicesCreditManager().getMultiplier(),
            equalTo(nodeActions.get(0).masterMultiplier())
        );

        Consumer<NodeAction> changeNode = (nodeAction -> {
            if (nodeAction.remove()) {
                DiscoveryNode nodeToRemove = activeNodes.get(nodeAction.index());
                assert nodeToRemove != null;
                clusterService.submitUnbatchedStateUpdateTask(masterNodeId, new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        var newState = ClusterState.builder(currentState)
                            .nodes(DiscoveryNodes.builder(currentState.nodes()).remove(nodeToRemove).build())
                            .build();
                        return newState;
                    }

                    @Override
                    public void onFailure(Exception e) {
                        throw new AssertionError(e);
                    }
                });
                activeNodes.remove(nodeAction.index());
            } else {
                final DiscoveryNode newNode = DiscoveryNodeUtils.create(
                    null,
                    nodeIdPrefix + nodeAction.index(),
                    ESTestCase.buildNewFakeTransportAddress(),
                    Map.of(RefreshThrottlingService.MEMORY_NODE_ATTR, String.valueOf(nodeAction.memory().getBytes())),
                    Set.of(nodeAction.search() ? SEARCH_ROLE : INDEX_ROLE)
                );
                clusterService.submitUnbatchedStateUpdateTask(masterNodeId, new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        var newState = ClusterState.builder(currentState)
                            .nodes(DiscoveryNodes.builder(currentState.nodes()).add(newNode).build())
                            .build();
                        return newState;
                    }

                    @Override
                    public void onFailure(Exception e) {
                        throw new AssertionError(e);
                    }
                });
                final DiscoveryNode previousDiscoveryNode = activeNodes.put(nodeAction.index(), newNode);
                assert previousDiscoveryNode == null;
            }
            deterministicTaskQueue.runAllTasks();
            assertThat(refreshThrottlingService.getRegularIndicesCreditManager().getMultiplier(), equalTo(nodeAction.masterMultiplier()));
            assertThat(refreshThrottlingService.getSystemIndicesCreditManager().getMultiplier(), equalTo(nodeAction.masterMultiplier()));
        });

        for (int i = 1; i < nodeActions.size(); i++) {
            changeNode.accept(nodeActions.get(i));
        }

        clusterService.close();
    }

    public void testMinimumMultiplier() {
        simulateNodeActions(
            List.of(
                new NodeAction(0, false, false, ByteSizeValue.ofMb(512), 1.0),
                new NodeAction(
                    1,
                    false,
                    true,
                    ByteSizeValue.ofMb(512),
                    1.0 // 0.5 / 0.5 * 1 / 4 = 0.25, but minimum multiplier should be 1
                )
            )
        );
    }

    public void testSearchMasterHasMultiplierOne() {
        simulateNodeActions(
            List.of(
                new NodeAction(0, false, true, ByteSizeValue.ofGb(64), 1.0),
                new NodeAction(1, false, false, ByteSizeValue.ofGb(128), 1.0)
            )
        );
    }

    public void testIndexMasterAndSearchNode() {
        simulateNodeActions(
            List.of(
                new NodeAction(0, false, false, ByteSizeValue.ofGb(64), 1.0),
                new NodeAction(
                    1,
                    false,
                    true,
                    ByteSizeValue.ofGb(128),
                    48.0 // 64 / 64 * 192 / 4
                )
            )
        );
    }

    public void testIndexMasterAndIndexNode() {
        simulateNodeActions(
            List.of(
                new NodeAction(0, false, false, ByteSizeValue.ofGb(64), 1.0),
                new NodeAction(
                    1,
                    false,
                    false,
                    ByteSizeValue.ofGb(128),
                    16.0 // 64 / 192 * 192 / 4
                )
            )
        );
    }

    public void testIndexMasterAndMultipleActions() {
        simulateNodeActions(
            List.of(
                new NodeAction(0, false, false, ByteSizeValue.ofGb(64), 1.0),
                new NodeAction(
                    1,
                    false,
                    true,
                    ByteSizeValue.ofGb(32),
                    24.0 // 64 / 64 * 96 / 4
                ),
                new NodeAction(2, false, false, ByteSizeValue.ofGb(32), (1.0 * 64 / 96 * 128 / 4)),
                new NodeAction(
                    3,
                    false,
                    true,
                    ByteSizeValue.ofGb(4),
                    22.0 // 64 / 96 * 132 / 4
                ),
                new NodeAction(
                    2,
                    true,
                    false,
                    ByteSizeValue.ZERO,
                    25.0 // 64 / 64 * 100 / 4
                )
            )
        );
    }
}
