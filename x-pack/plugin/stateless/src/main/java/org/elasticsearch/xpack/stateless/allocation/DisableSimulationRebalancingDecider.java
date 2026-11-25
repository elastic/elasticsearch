/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;

/**
 * Disables rebalancing for nominated tier(s) when simulating.
 * <p>
 * When the {@link org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceShardsAllocator} is in use, the simulation will
 * make moves based on a copy of the {@link org.elasticsearch.cluster.ClusterInfo} which is updated to simulate the effects of moves
 * it has already made. For this reason {@link AllocationDecider#canRemain} will sometimes return NO or NOT_PREFERRED for the simulated
 * ClusterInfo, but not for the real ClusterInfo used by the
 * {@link org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceReconciler}.
 * <p>
 * If we turn off rebalancing in both simulation and reconciliation phases, we'll leave some desired balance moves unmade, because the
 * reconciler will only move shards that return NO or NOT_PREFERRED from {@link AllocationDecider#canRemain}. These unmade moves will
 * accumulate and eventually result in the simulation operating on a very inaccurate view of the cluster. This Decider disables rebalancing
 * for the simulation phase only, it is always enabled for the reconciliation phase.
 */
public class DisableSimulationRebalancingDecider extends AllocationDecider {

    private static final String NAME = "disable_simulation_rebalancing";
    private static final Decision ALWAYS_REBALANCE = new Decision.Single(Decision.Type.YES, NAME, "All rebalancing is enabled");

    private static final Decision RECONCILIATION_BALANCING_ALLOWED = new Decision.Single(
        Decision.Type.YES,
        NAME,
        "Rebalancing is always allowed during reconciliation"
    );

    private static final Decision INDEX_REBALANCING_DISABLED = new Decision.Single(
        Decision.Type.NO,
        NAME,
        "Rebalancing of index shards is disabled"
    );

    private static final Decision SEARCH_REBALANCING_ENABLED = new Decision.Single(
        Decision.Type.YES,
        NAME,
        "Rebalancing of search shards is enabled"
    );

    private static final Decision NO_REBALANCE = new Decision.Single(Decision.Type.NO, NAME, "Rebalancing is disabled");

    public enum RebalancingEnabled {
        ALWAYS {
            @Override
            Decision canRebalance() {
                return ALWAYS_REBALANCE;
            }

            @Override
            Decision canRebalance(ShardRouting shardRouting) {
                return ALWAYS_REBALANCE;
            }
        },
        SEARCH_TIER_ONLY {
            @Override
            Decision canRebalance() {
                return SEARCH_REBALANCING_ENABLED;
            }

            @Override
            Decision canRebalance(ShardRouting shardRouting) {
                return shardRouting.role() == ShardRouting.Role.SEARCH_ONLY ? SEARCH_REBALANCING_ENABLED : INDEX_REBALANCING_DISABLED;
            }
        },
        NEVER {
            @Override
            Decision canRebalance() {
                return NO_REBALANCE;
            }

            @Override
            Decision canRebalance(ShardRouting shardRouting) {
                return NO_REBALANCE;
            }
        };

        abstract Decision canRebalance();

        abstract Decision canRebalance(ShardRouting shardRouting);
    }

    /**
     * This decider is only added when the stateless plugin is enabled, so it's OK to disable
     * balancing in the index tier by default.
     */
    public static final Setting<RebalancingEnabled> SIMULATION_REBALANCING_ENABLED_SETTING = Setting.enumSetting(
        RebalancingEnabled.class,
        "stateless.cluster.routing.allocation.balance.balancing_enabled",
        RebalancingEnabled.SEARCH_TIER_ONLY,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private volatile RebalancingEnabled rebalancingEnabled;

    public DisableSimulationRebalancingDecider(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(
            SIMULATION_REBALANCING_ENABLED_SETTING,
            rebalancingEnabled -> this.rebalancingEnabled = rebalancingEnabled
        );
    }

    @Override
    public Decision canRebalance(RoutingAllocation allocation) {
        return allocation.isSimulating() ? rebalancingEnabled.canRebalance() : RECONCILIATION_BALANCING_ALLOWED;
    }

    @Override
    public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        return allocation.isSimulating() ? rebalancingEnabled.canRebalance(shardRouting) : RECONCILIATION_BALANCING_ALLOWED;
    }
}
