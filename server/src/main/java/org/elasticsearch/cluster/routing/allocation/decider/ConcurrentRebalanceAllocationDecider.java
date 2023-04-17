/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
 * re-balance (relocation) operations and restricts node allocations if the
 * configured threshold is reached. The default number of concurrent rebalance
 * operations is set to {@code 2}
 * <p>
 * Re-balance operations can be controlled in real-time via the cluster update API using
 * {@code cluster.routing.allocation.cluster_concurrent_rebalance}. Iff this
 * setting is set to {@code -1} the number of concurrent re-balance operations
 * are unlimited.
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

    public ConcurrentRebalanceAllocationDecider(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(
            CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING,
            this::setClusterConcurrentRebalance
        );
        logger.debug("using [cluster_concurrent_rebalance] with [{}]", clusterConcurrentRebalance);
    }

    private void setClusterConcurrentRebalance(int concurrentRebalance) {
        clusterConcurrentRebalance = concurrentRebalance;
    }

    @Override
    public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        return canRebalance(allocation);
    }

    @Override
    public Decision canRebalance(RoutingAllocation allocation) {
        int relocatingShards = allocation.routingNodes().getRelocatingShardCount();
        if (allocation.isSimulating() && relocatingShards >= 2) {
            // BalancedShardAllocator is prone to perform unnecessary moves when cluster_concurrent_rebalance is set to high values (>2).
            // (See https://github.com/elastic/elasticsearch/issues/87279)
            // Above allocator is used in DesiredBalanceComputer. Since we do not move actual shard data during calculation
            // it is possible to artificially set above setting to 2 to avoid unnecessary moves in desired balance.
            return allocation.decision(Decision.THROTTLE, NAME, "allocation should move one shard at the time when simulating");
        }
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
