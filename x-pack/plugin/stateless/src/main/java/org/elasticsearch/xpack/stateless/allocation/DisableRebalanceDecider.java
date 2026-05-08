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
 * Disables rebalancing for nominated tier(s) in the simulation phase. Rebalancing is always allowed during reconciliation
 * to prevent drift between the desired balance and the actual shard allocation.
 */
public class DisableRebalanceDecider extends AllocationDecider {

    public static final String NAME = "stateless-rebalance-disabler";
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

    private static final Decision INDEX_REBALANCING_ENABLED = new Decision.Single(
        Decision.Type.YES,
        NAME,
        "Rebalancing of index shards is enabled"
    );

    private static final Decision SEARCH_REBALANCING_DISABLED = new Decision.Single(
        Decision.Type.NO,
        NAME,
        "Rebalancing of search shards is disabled"
    );

    private static final Decision SEARCH_REBALANCING_ENABLED = new Decision.Single(
        Decision.Type.YES,
        NAME,
        "Rebalancing of search shards is enabled"
    );

    private static final Decision NO_REBALANCE = new Decision.Single(Decision.Type.NO, NAME, "Rebalancing is disabled");

    public enum Enablement {
        ALWAYS {
            @Override
            Decision canRebalance(ShardRouting shardRouting) {
                return ALWAYS_REBALANCE;
            }
        },
        INDEXING_TIER {
            @Override
            Decision canRebalance(ShardRouting shardRouting) {
                return shardRouting.role() == ShardRouting.Role.INDEX_ONLY ? INDEX_REBALANCING_ENABLED : SEARCH_REBALANCING_DISABLED;
            }
        },
        SEARCH_TIER {
            @Override
            Decision canRebalance(ShardRouting shardRouting) {
                return shardRouting.role() == ShardRouting.Role.SEARCH_ONLY ? SEARCH_REBALANCING_ENABLED : INDEX_REBALANCING_DISABLED;
            }
        },
        NEVER {
            @Override
            Decision canRebalance(ShardRouting shardRouting) {
                return NO_REBALANCE;
            }
        };

        abstract Decision canRebalance(ShardRouting shardRouting);
    }

    public static final Setting<Enablement> REBALANCING_ENABLED = Setting.enumSetting(
        Enablement.class,
        "stateless.cluster.routing.allocation.balance.balancing_enabled",
        Enablement.ALWAYS,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private volatile Enablement enablement;

    public DisableRebalanceDecider(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(REBALANCING_ENABLED, enablement -> this.enablement = enablement);
    }

    @Override
    public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        return allocation.isSimulating() == false ? RECONCILIATION_BALANCING_ALLOWED : enablement.canRebalance(shardRouting);
    }
}
