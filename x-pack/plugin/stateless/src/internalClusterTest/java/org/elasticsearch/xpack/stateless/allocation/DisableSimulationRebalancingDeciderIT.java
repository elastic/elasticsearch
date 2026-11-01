/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.stateless.allocation.DisableSimulationRebalancingDecider.RebalancingEnabled;
import static org.elasticsearch.xpack.stateless.allocation.DisableSimulationRebalancingDecider.SIMULATION_REBALANCING_ENABLED_SETTING;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DisableSimulationRebalancingDeciderIT extends AbstractStatelessPluginIntegTestCase {

    /** Node ids in this set make {@link TestAllocationDecider#canRemain} return NO during simulation only. */
    private static final Set<String> CAN_REMAIN_NO_IN_SIMULATION = ConcurrentCollections.newConcurrentSet();
    /** Node ids in this set make {@link TestAllocationDecider#canRemain} return NOT_PREFERRED during simulation only. */
    private static final Set<String> CAN_REMAIN_NOT_PREFERRED_IN_SIMULATION = ConcurrentCollections.newConcurrentSet();
    /** Node ids in this set make {@link TestAllocationDecider#canAllocate} return THROTTLE during reconciliation only. */
    private static final Set<String> CAN_ALLOCATE_THROTTLE_IN_RECONCILIATION = ConcurrentCollections.newConcurrentSet();

    @Before
    public void clearDeciderState() {
        CAN_REMAIN_NO_IN_SIMULATION.clear();
        CAN_REMAIN_NOT_PREFERRED_IN_SIMULATION.clear();
        CAN_ALLOCATE_THROTTLE_IN_RECONCILIATION.clear();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestAllocationPlugin.class);
    }

    /**
     * Verifies that a shard is moved during reconciliation even when rebalancing is disabled in the simulation phase.
     * <p>
     * With {@link RebalancingEnabled#NEVER}, {@link DisableSimulationRebalancingDecider#canRebalance} returns NO during simulation but
     * YES during reconciliation. This test plants a decider that returns canRemain NO/NOT_PREFERRED during simulation
     * only, so the desired balance plans a move off the source node, and YES during reconciliation, so the
     * reconciler's necessary-moves phase will skip the shard. The shard should still be moved by the reconciler's
     * rebalance phase, which is gated on canRebalance and canAllocate but not canRemain.
     */
    public void testShardIsMovedDuringReconciliationWhenRebalancingIsDisabledInSimulation() {
        final var testHarness = setupClusterWithSingleShard();

        randomFrom(CAN_REMAIN_NO_IN_SIMULATION, CAN_REMAIN_NOT_PREFERRED_IN_SIMULATION).add(
            getNodeId(testHarness.initiallyAllocatedNode())
        );
        ClusterRerouteUtils.reroute(client());

        awaitClusterState(state -> {
            final var shard = state.routingTable(ProjectId.DEFAULT).index(testHarness.indexName()).shard(0).primaryShard();
            return shard.state() == ShardRoutingState.STARTED && shard.currentNodeId().equals(getNodeId(testHarness.otherNode()));
        });
    }

    /**
     * Verifies that THROTTLE returned from {@link AllocationDecider#canAllocate} is still respected during
     * reconciliation. The desired balance plans a move (canRemain=NO during simulation), but the reconciler's
     * rebalance phase finds no YES/NOT_PREFERRED target because the only candidate returns THROTTLE, so the move
     * is deferred until the throttle clears.
     */
    public void testThrottleFromCanAllocateBlocksMovement() {
        final var testHarness = setupClusterWithSingleShard();

        CAN_REMAIN_NO_IN_SIMULATION.add(getNodeId(testHarness.initiallyAllocatedNode()));
        CAN_ALLOCATE_THROTTLE_IN_RECONCILIATION.add(getNodeId(testHarness.otherNode()));
        ClusterRerouteUtils.reroute(client());

        // Throttle is respected: the shard remains on initiallyAllocatedNode.
        assertEquals(
            getNodeId(testHarness.initiallyAllocatedNode()),
            clusterService().state().routingTable(ProjectId.DEFAULT).index(testHarness.indexName()).shard(0).primaryShard().currentNodeId()
        );

        // Once the throttle clears, the move planned by simulation is executed.
        CAN_ALLOCATE_THROTTLE_IN_RECONCILIATION.clear();
        ClusterRerouteUtils.reroute(client());
        awaitClusterState(state -> {
            final var shard = state.routingTable(ProjectId.DEFAULT).index(testHarness.indexName()).shard(0).primaryShard();
            return shard.state() == ShardRoutingState.STARTED && shard.currentNodeId().equals(getNodeId(testHarness.otherNode()));
        });
    }

    private TestHarness setupClusterWithSingleShard() {
        final var rebalancingDisabled = Settings.builder()
            .put(SIMULATION_REBALANCING_ENABLED_SETTING.getKey(), RebalancingEnabled.NEVER)
            .build();
        final var indexNode1 = startMasterAndIndexNode(rebalancingDisabled);
        // Create the index, it'll be allocated to indexNode1
        final var indexName = randomIdentifier();
        createIndex(indexName, 1, 0);
        ensureGreen(indexName);
        final var indexNode2 = startIndexNode(rebalancingDisabled);
        ensureStableCluster(2);
        // Ensure the shard is still on our initial node
        assertThat(
            internalCluster().clusterService()
                .state()
                .getRoutingNodes()
                .node(getNodeId(indexNode1))
                .numberOfShardsWithState(ShardRoutingState.STARTED),
            equalTo(1)
        );
        return new TestHarness(indexNode1, indexNode2, indexName);
    }

    private record TestHarness(String initiallyAllocatedNode, String otherNode, String indexName) {}

    public static class TestAllocationPlugin extends Plugin implements ClusterPlugin {

        @Override
        public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
            return List.of(new TestAllocationDecider());
        }
    }

    private static class TestAllocationDecider extends AllocationDecider {

        @Override
        public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            if (allocation.isSimulating()) {
                if (CAN_REMAIN_NO_IN_SIMULATION.contains(node.nodeId())) {
                    return Decision.NO;
                }
                if (CAN_REMAIN_NOT_PREFERRED_IN_SIMULATION.contains(node.nodeId())) {
                    return Decision.NOT_PREFERRED;
                }
            }
            return Decision.YES;
        }

        @Override
        public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            if (allocation.isSimulating() == false && CAN_ALLOCATE_THROTTLE_IN_RECONCILIATION.contains(node.nodeId())) {
                return Decision.THROTTLE;
            }
            return Decision.YES;
        }
    }
}
