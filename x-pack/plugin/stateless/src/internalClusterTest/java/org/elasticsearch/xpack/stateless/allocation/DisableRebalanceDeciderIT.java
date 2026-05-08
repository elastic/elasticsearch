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
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
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

import static org.elasticsearch.xpack.stateless.allocation.DisableRebalanceDecider.Enablement;
import static org.elasticsearch.xpack.stateless.allocation.DisableRebalanceDecider.REBALANCING_ENABLED;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DisableRebalanceDeciderIT extends AbstractStatelessPluginIntegTestCase {

    /** Node ids in this set make {@link TestAllocationDecider#canRemain} return NO during simulation only. */
    private static final Set<String> CAN_REMAIN_NO_IN_SIMULATION = ConcurrentCollections.newConcurrentSet();
    /** Node ids in this set make {@link TestAllocationDecider#canRemain} return NOT_PREFERRED during simulation only. */
    private static final Set<String> CAN_REMAIN_NOT_PREFERRED_IN_SIMULATION = ConcurrentCollections.newConcurrentSet();
    /** Node ids in this set make {@link TestAllocationDecider#canRemain} return THROTTLE in both simulation and reconciliation. */
    private static final Set<String> CAN_REMAIN_THROTTLE_NODE_IDS = ConcurrentCollections.newConcurrentSet();
    /** Node ids in this set make {@link TestAllocationDecider#canAllocate} return THROTTLE during reconciliation only. */
    private static final Set<String> CAN_ALLOCATE_THROTTLE_IN_RECONCILIATION = ConcurrentCollections.newConcurrentSet();

    @Before
    public void clearDeciderState() {
        CAN_REMAIN_NO_IN_SIMULATION.clear();
        CAN_REMAIN_NOT_PREFERRED_IN_SIMULATION.clear();
        CAN_REMAIN_THROTTLE_NODE_IDS.clear();
        CAN_ALLOCATE_THROTTLE_IN_RECONCILIATION.clear();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestAllocationPlugin.class);
    }

    /**
     * Verifies that a shard is moved during reconciliation even when rebalancing is disabled in the simulation phase.
     * <p>
     * With {@link Enablement#NEVER}, {@link DisableRebalanceDecider#canRebalance} returns NO during simulation but
     * YES during reconciliation. This test plants a decider that returns canRemain NO/NOT_PREFERRED during simulation
     * only — so the desired balance plans a move off the source node — and YES during reconciliation, so the
     * reconciler's necessary-moves phase will skip the shard. The shard should still be moved by the reconciler's
     * balance phase, which is gated on canRebalance and canAllocate but not canRemain.
     */
    public void testShardIsMovedDuringReconciliationWhenRebalancingIsDisabledInSimulation() {
        final var indexNode1 = startMasterAndIndexNode();
        final var indexNode2 = startIndexNode();
        ensureStableCluster(2);
        applyTestClusterSettings();

        final var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.require._name", indexNode1).build());
        ensureGreen(indexName);
        // Lift the require-name pin so the decider can drive the desired balance.
        updateIndexSettings(Settings.builder().putNull("index.routing.allocation.require._name"), indexName);

        randomFrom(CAN_REMAIN_NO_IN_SIMULATION, CAN_REMAIN_NOT_PREFERRED_IN_SIMULATION).add(getNodeId(indexNode1));
        ClusterRerouteUtils.reroute(client());

        awaitClusterState(state -> {
            final var shard = state.routingTable(ProjectId.DEFAULT).index(indexName).shard(0).primaryShard();
            return shard.state() == ShardRoutingState.STARTED && shard.currentNodeId().equals(getNodeId(indexNode2));
        });
    }

    /**
     * Verifies that THROTTLE returned from {@link AllocationDecider#canAllocate} is still respected during
     * reconciliation. The desired balance plans a move (canRemain=NO during simulation), but the reconciler's
     * balance phase finds no YES/NOT_PREFERRED target because the only candidate returns THROTTLE, so the move
     * is deferred until the throttle clears.
     */
    public void testThrottleFromCanAllocateBlocksMovement() {
        final var indexNode1 = startMasterAndIndexNode();
        final var indexNode2 = startIndexNode();
        ensureStableCluster(2);
        applyTestClusterSettings();

        final var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.require._name", indexNode1).build());
        ensureGreen(indexName);
        updateIndexSettings(Settings.builder().putNull("index.routing.allocation.require._name"), indexName);

        CAN_REMAIN_NO_IN_SIMULATION.add(getNodeId(indexNode1));
        CAN_ALLOCATE_THROTTLE_IN_RECONCILIATION.add(getNodeId(indexNode2));
        ClusterRerouteUtils.reroute(client());

        // Throttle is respected: the shard remains on indexNode1.
        assertEquals(
            getNodeId(indexNode1),
            clusterService().state().routingTable(ProjectId.DEFAULT).index(indexName).shard(0).primaryShard().currentNodeId()
        );

        // Once the throttle clears, the move planned by simulation is executed.
        CAN_ALLOCATE_THROTTLE_IN_RECONCILIATION.clear();
        ClusterRerouteUtils.reroute(client());
        awaitClusterState(state -> {
            final var shard = state.routingTable(ProjectId.DEFAULT).index(indexName).shard(0).primaryShard();
            return shard.state() == ShardRoutingState.STARTED && shard.currentNodeId().equals(getNodeId(indexNode2));
        });
    }

    /**
     * Verifies that THROTTLE returned from {@link AllocationDecider#canRemain} is still respected. The simulator
     * treats canRemain=THROTTLE as "remain" so the desired balance is not changed, and no move is planned. Once the
     * throttle is replaced by NO, simulation plans the move and reconciliation executes it.
     */
    public void testThrottleFromCanRemainBlocksMovement() {
        final var indexNode1 = startMasterAndIndexNode();
        final var indexNode2 = startIndexNode();
        ensureStableCluster(2);
        applyTestClusterSettings();

        final var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.require._name", indexNode1).build());
        ensureGreen(indexName);
        updateIndexSettings(Settings.builder().putNull("index.routing.allocation.require._name"), indexName);

        CAN_REMAIN_THROTTLE_NODE_IDS.add(getNodeId(indexNode1));
        ClusterRerouteUtils.reroute(client());

        // Throttle is respected: the shard remains on indexNode1.
        assertEquals(
            getNodeId(indexNode1),
            clusterService().state().routingTable(ProjectId.DEFAULT).index(indexName).shard(0).primaryShard().currentNodeId()
        );

        // Replace THROTTLE with NO so simulation plans a move; reconciliation should then execute it.
        CAN_REMAIN_THROTTLE_NODE_IDS.clear();
        CAN_REMAIN_NO_IN_SIMULATION.add(getNodeId(indexNode1));
        ClusterRerouteUtils.reroute(client());
        awaitClusterState(state -> {
            final var shard = state.routingTable(ProjectId.DEFAULT).index(indexName).shard(0).primaryShard();
            return shard.state() == ShardRoutingState.STARTED && shard.currentNodeId().equals(getNodeId(indexNode2));
        });
    }

    /**
     * Disables rebalancing in the simulation phase via {@link DisableRebalanceDecider}, and overrides the stateless
     * default of {@code cluster.routing.allocation.enable_rebalance=REPLICAS} so the reconciler's balance phase can
     * actually move primary (index) shards — otherwise {@link EnableAllocationDecider} would return NO and mask the
     * behavior under test.
     */
    private void applyTestClusterSettings() {
        updateClusterSettings(
            Settings.builder()
                .put(REBALANCING_ENABLED.getKey(), Enablement.NEVER)
                .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.ALL)
        );
    }

    public static class TestAllocationPlugin extends Plugin implements ClusterPlugin {

        @Override
        public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
            return List.of(new TestAllocationDecider());
        }
    }

    private static class TestAllocationDecider extends AllocationDecider {

        @Override
        public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            if (CAN_REMAIN_THROTTLE_NODE_IDS.contains(node.nodeId())) {
                return Decision.THROTTLE;
            }
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
