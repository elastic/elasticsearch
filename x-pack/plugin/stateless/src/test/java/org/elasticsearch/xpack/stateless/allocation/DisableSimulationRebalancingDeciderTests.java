/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.TestRoutingAllocationFactory;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;

import java.util.HashSet;

import static org.elasticsearch.xpack.stateless.allocation.DisableSimulationRebalancingDecider.SIMULATION_REBALANCING_ENABLED_SETTING;
import static org.hamcrest.Matchers.equalTo;

public class DisableSimulationRebalancingDeciderTests extends ESAllocationTestCase {

    // During reconciliation (not simulating), rebalancing is always allowed regardless of setting
    public void testReconciliationAlwaysAllows() {
        var decider = createDecider(randomFrom(DisableSimulationRebalancingDecider.RebalancingEnabled.values()));
        assertCanRebalance(decider, reconciliationAllocation(), indexOnlyShard(), Decision.Type.YES);
        assertCanRebalance(decider, reconciliationAllocation(), searchOnlyShard(), Decision.Type.YES);
    }

    // During simulation, ALWAYS setting permits all rebalancing
    public void testSimulation_always_allowsBothTiers() {
        var decider = createDecider(DisableSimulationRebalancingDecider.RebalancingEnabled.ALWAYS);
        assertCanRebalance(decider, simulatingAllocation(), indexOnlyShard(), Decision.Type.YES);
        assertCanRebalance(decider, simulatingAllocation(), searchOnlyShard(), Decision.Type.YES);
    }

    // During simulation, SEARCH_TIER_ONLY permits search shards, blocks index shards
    public void testSimulation_searchTierOnly_allowsSearchBlocksIndex() {
        var decider = createDecider(DisableSimulationRebalancingDecider.RebalancingEnabled.SEARCH_TIER_ONLY);
        assertCanRebalance(decider, simulatingAllocation(), indexOnlyShard(), Decision.Type.NO);
        assertCanRebalance(decider, simulatingAllocation(), searchOnlyShard(), Decision.Type.YES);
    }

    // During simulation, NEVER blocks all rebalancing
    public void testSimulation_never_blocksAllTiers() {
        var decider = createDecider(DisableSimulationRebalancingDecider.RebalancingEnabled.NEVER);
        assertCanRebalance(decider, simulatingAllocation(), indexOnlyShard(), Decision.Type.NO);
        assertCanRebalance(decider, simulatingAllocation(), searchOnlyShard(), Decision.Type.NO);
    }

    // canRebalance(RoutingAllocation): during reconciliation, always allows regardless of setting
    public void testAllocationLevel_reconciliation_alwaysAllows() {
        var decider = createDecider(randomFrom(DisableSimulationRebalancingDecider.RebalancingEnabled.values()));
        assertThat(decider.canRebalance(reconciliationAllocation()).type(), equalTo(Decision.Type.YES));
    }

    // canRebalance(RoutingAllocation): during simulation, ALWAYS returns YES
    public void testAllocationLevel_simulation_always_allowsRebalancing() {
        var decider = createDecider(DisableSimulationRebalancingDecider.RebalancingEnabled.ALWAYS);
        assertThat(decider.canRebalance(simulatingAllocation()).type(), equalTo(Decision.Type.YES));
    }

    // canRebalance(RoutingAllocation): during simulation, SEARCH_TIER_ONLY returns YES (some rebalancing enabled)
    public void testAllocationLevel_simulation_searchTierOnly_allowsSomeRebalancing() {
        var decider = createDecider(DisableSimulationRebalancingDecider.RebalancingEnabled.SEARCH_TIER_ONLY);
        assertThat(decider.canRebalance(simulatingAllocation()).type(), equalTo(Decision.Type.YES));
    }

    // canRebalance(RoutingAllocation): during simulation, NEVER returns NO
    public void testAllocationLevel_simulation_never_blocksAllRebalancing() {
        var decider = createDecider(DisableSimulationRebalancingDecider.RebalancingEnabled.NEVER);
        assertThat(decider.canRebalance(simulatingAllocation()).type(), equalTo(Decision.Type.NO));
    }

    // Dynamic setting update takes effect immediately
    public void testDynamicSettingUpdate() {
        var settingsSet = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingsSet.add(SIMULATION_REBALANCING_ENABLED_SETTING);
        var clusterSettings = new ClusterSettings(Settings.EMPTY, settingsSet);
        var decider = new DisableSimulationRebalancingDecider(clusterSettings);

        // Initially SEARCH_ONLY (default)
        assertCanRebalance(decider, simulatingAllocation(), searchOnlyShard(), Decision.Type.YES);
        assertCanRebalance(decider, simulatingAllocation(), indexOnlyShard(), Decision.Type.NO);
        assertCanRebalance(decider, simulatingAllocation(), Decision.Type.YES);

        // Update to NEVER
        clusterSettings.applySettings(
            Settings.builder()
                .put(SIMULATION_REBALANCING_ENABLED_SETTING.getKey(), DisableSimulationRebalancingDecider.RebalancingEnabled.NEVER)
                .build()
        );
        assertCanRebalance(decider, simulatingAllocation(), randomFrom(indexOnlyShard(), searchOnlyShard()), Decision.Type.NO);
        assertCanRebalance(decider, simulatingAllocation(), Decision.Type.NO);

        // Update back to ALWAYS
        clusterSettings.applySettings(
            Settings.builder()
                .put(SIMULATION_REBALANCING_ENABLED_SETTING.getKey(), DisableSimulationRebalancingDecider.RebalancingEnabled.ALWAYS)
                .build()
        );
        assertCanRebalance(decider, simulatingAllocation(), randomFrom(indexOnlyShard(), searchOnlyShard()), Decision.Type.YES);
        assertCanRebalance(decider, simulatingAllocation(), Decision.Type.YES);
    }

    private static DisableSimulationRebalancingDecider createDecider(DisableSimulationRebalancingDecider.RebalancingEnabled setting) {
        var settings = Settings.builder().put(SIMULATION_REBALANCING_ENABLED_SETTING.getKey(), setting.name()).build();
        var settingsSet = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingsSet.add(SIMULATION_REBALANCING_ENABLED_SETTING);
        return new DisableSimulationRebalancingDecider(new ClusterSettings(settings, settingsSet));
    }

    private static RoutingAllocation reconciliationAllocation() {
        return TestRoutingAllocationFactory.forClusterState(ClusterState.EMPTY_STATE).mutable();
    }

    private static RoutingAllocation simulatingAllocation() {
        return TestRoutingAllocationFactory.forClusterState(ClusterState.EMPTY_STATE).mutable().mutableCloneForSimulation();
    }

    private static ShardRouting indexOnlyShard() {
        return TestShardRouting.newShardRouting(
            new ShardId("test", "_na_", 0),
            "node-1",
            true,
            ShardRoutingState.STARTED,
            ShardRouting.Role.INDEX_ONLY
        );
    }

    private static ShardRouting searchOnlyShard() {
        return TestShardRouting.newShardRouting(
            new ShardId("test", "_na_", 0),
            "node-1",
            false,
            ShardRoutingState.STARTED,
            ShardRouting.Role.SEARCH_ONLY
        );
    }

    private static void assertCanRebalance(
        DisableSimulationRebalancingDecider decider,
        RoutingAllocation allocation,
        Decision.Type expected
    ) {
        assertThat(decider.canRebalance(allocation).type(), equalTo(expected));
    }

    private static void assertCanRebalance(
        DisableSimulationRebalancingDecider decider,
        RoutingAllocation allocation,
        ShardRouting shard,
        Decision.Type expected
    ) {
        assertThat(decider.canRebalance(shard, allocation).type(), equalTo(expected));
    }
}
