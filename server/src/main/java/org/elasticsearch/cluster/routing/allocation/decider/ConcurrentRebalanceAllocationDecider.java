/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;

/**
 * Similar to the {@link ClusterRebalanceAllocationDecider} this
 * {@link AllocationDecider} controls the number of currently in-progress
 * re-balance (shard relocation) operations and restricts node allocations
 * if the configured threshold is reached. Frozen and non-frozen shards are
 * considered separately. The default number of concurrent rebalance operations
 * is set to {@code 2} for non-frozen shards. For frozen shards, the default is
 * the same setting as non-frozen shards, until set explicitly.
 * <p>
 * Re-balance operations can be controlled in real-time via the cluster update API using
 * {@code cluster.routing.allocation.cluster_concurrent_rebalance} and
 * {@code cluster.routing.allocation.cluster_concurrent_frozen_rebalance}.
 * Iff either setting is set to {@code -1} the number of concurrent re-balance operations
 * within the setting's category (frozen or non-frozen) are unlimited.
 */
public class ConcurrentRebalanceAllocationDecider extends AllocationDecider {

    private static final Logger logger = LogManager.getLogger(ConcurrentRebalanceAllocationDecider.class);

    public static final String NAME = "concurrent_rebalance";

    public static final Setting<Integer> CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING = Setting.intSetting(
        "cluster.routing.allocation.cluster_concurrent_rebalance",
        2,
        -1,
        Property.Dynamic,
        Property.NodeScope
    );
    private volatile int clusterConcurrentRebalance;

    /**
     * Same as cluster_concurrent_rebalance, but applies separately to frozen tier shards
     *
     * Defaults to the same value as normal concurrent rebalance, if unspecified
     */
    public static final Setting<Integer> CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_FROZEN_REBALANCE_SETTING = Setting.intSetting(
        "cluster.routing.allocation.cluster_concurrent_frozen_rebalance",
        CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING,
        -1,
        Property.Dynamic,
        Property.NodeScope
    );
    private volatile int clusterConcurrentFrozenRebalance;

    public ConcurrentRebalanceAllocationDecider(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(
            CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING,
            this::setClusterConcurrentRebalance
        );
        clusterSettings.initializeAndWatch(
            CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_FROZEN_REBALANCE_SETTING,
            this::setClusterConcurrentFrozenRebalance
        );
        logger.debug(
            "using [cluster_concurrent_rebalance] with [concurrent_rebalance={}, concurrent_frozen_rebalance={}]",
            clusterConcurrentRebalance,
            clusterConcurrentFrozenRebalance
        );
    }

    private void setClusterConcurrentRebalance(int concurrentRebalance) {
        clusterConcurrentRebalance = concurrentRebalance;
    }

    private void setClusterConcurrentFrozenRebalance(int concurrentFrozenRebalance) {
        clusterConcurrentFrozenRebalance = concurrentFrozenRebalance;
    }

    @Override
    public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        int relocatingFrozenShards = allocation.routingNodes().getRelocatingFrozenShardCount();
        if (allocation.routingNodes().isDedicatedFrozenNode(shardRouting.currentNodeId())) {
            if (clusterConcurrentFrozenRebalance == -1) {
                return allocation.decision(Decision.YES, NAME, "unlimited concurrent frozen rebalances are allowed");
            }
            if (relocatingFrozenShards >= clusterConcurrentFrozenRebalance) {
                return allocation.decision(
                    Decision.THROTTLE,
                    NAME,
                    "reached the limit of concurrently rebalancing frozen shards [%d], cluster setting [%s=%d]",
                    relocatingFrozenShards,
                    CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_FROZEN_REBALANCE_SETTING.getKey(),
                    clusterConcurrentFrozenRebalance
                );
            }
            return allocation.decision(
                Decision.YES,
                NAME,
                "below threshold [%d] for concurrent frozen rebalances, current frozen rebalance shard count [%d]",
                clusterConcurrentFrozenRebalance,
                relocatingFrozenShards
            );
        } else {
            int relocatingShards = allocation.routingNodes().getRelocatingShardCount() - relocatingFrozenShards;
            if (clusterConcurrentRebalance == -1) {
                return allocation.decision(Decision.YES, NAME, "unlimited concurrent rebalances are allowed");
            }
            if (relocatingShards >= clusterConcurrentRebalance) {
                return allocation.decision(
                    Decision.THROTTLE,
                    NAME,
                    "reached the limit of concurrently rebalancing shards [%d], cluster setting [%s=%d]",
                    relocatingShards,
                    CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING.getKey(),
                    clusterConcurrentRebalance
                );
            }
            return allocation.decision(
                Decision.YES,
                NAME,
                "below threshold [%d] for concurrent rebalances, current rebalance shard count [%d]",
                clusterConcurrentRebalance,
                relocatingShards
            );
        }
    }

    /**
     * We allow a limited number of concurrent shard relocations, per the cluster setting
     * {@link #CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING}.
     * Returns a {@link Decision#THROTTLE} decision if the limit is exceeded, otherwise returns {@link Decision#YES}.
     */
    @Override
    public Decision canRebalance(RoutingAllocation allocation) {
        int relocatingFrozenShards = allocation.routingNodes().getRelocatingFrozenShardCount();
        int relocatingShards = allocation.routingNodes().getRelocatingShardCount();
        if (allocation.isSimulating() && relocatingShards >= 2) {
            // This branch should no longer run after https://github.com/elastic/elasticsearch/pull/134786
            assert false : "allocation simulation should have returned earlier and not hit throttling";
            // BalancedShardAllocator is prone to perform unnecessary moves when cluster_concurrent_rebalance is set to high values (>2).
            // (See https://github.com/elastic/elasticsearch/issues/87279)
            // Above allocator is used in DesiredBalanceComputer. Since we do not move actual shard data during calculation
            // it is possible to artificially set above setting to 2 to avoid unnecessary moves in desired balance.
            // Separately: keep overall limit in simulation to two including frozen shards
            return allocation.decision(Decision.THROTTLE, NAME, "allocation should move one shard at the time when simulating");
        }

        // separate into frozen/non-frozen counts
        relocatingShards = relocatingShards - relocatingFrozenShards;

        // either frozen or non-frozen having some allowance before their limit means the allocator has room to rebalance
        if (clusterConcurrentRebalance == -1 || relocatingShards < clusterConcurrentRebalance) {
            return allocation.decision(
                Decision.YES,
                NAME,
                "below threshold [%s=%d] for concurrent rebalances, current rebalance shard count [%d]",
                CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING.getKey(),
                clusterConcurrentRebalance,
                relocatingShards
            );
        }
        if (clusterConcurrentFrozenRebalance == -1 || relocatingFrozenShards < clusterConcurrentFrozenRebalance) {
            return allocation.decision(
                Decision.YES,
                NAME,
                "below threshold [%s=%d] for concurrent frozen rebalances, current frozen rebalance shard count [%d]",
                CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_FROZEN_REBALANCE_SETTING.getKey(),
                clusterConcurrentFrozenRebalance,
                relocatingFrozenShards
            );
        }
        return allocation.decision(
            Decision.THROTTLE,
            NAME,
            "reached the limit of concurrently rebalancing shards [%d] for concurrent rebalances, cluster setting [%s=%d], "
                + "and [%d] for concurrent frozen rebalances, frozen cluster setting [%s=%d]",
            relocatingShards,
            CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING.getKey(),
            clusterConcurrentRebalance,
            relocatingFrozenShards,
            CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_FROZEN_REBALANCE_SETTING.getKey(),
            clusterConcurrentFrozenRebalance
        );
    }
}
