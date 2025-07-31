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
import org.elasticsearch.cluster.NodeUsageStatsForThreadPools.ThreadPoolUsageStats;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintSettings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * Decides whether shards can be allocated to cluster nodes, or can remain on cluster nodes, based on the target node's current write thread
 * pool usage stats and any candidate shard's write load estimate.
 */
public class WriteLoadConstraintDecider extends AllocationDecider {
    private static final Logger logger = LogManager.getLogger(WriteLoadConstraintDecider.class);

    public static final String NAME = "write_load";

    private final WriteLoadConstraintSettings writeLoadConstraintSettings;

    public WriteLoadConstraintDecider(ClusterSettings clusterSettings) {
        this.writeLoadConstraintSettings = new WriteLoadConstraintSettings(clusterSettings);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (writeLoadConstraintSettings.getWriteLoadConstraintEnabled() != WriteLoadConstraintSettings.WriteLoadDeciderStatus.ENABLED) {
            return Decision.YES;
        }

        // Check whether the shard being relocated has any write load estimate. If it does not, then this decider has no opinion.
        var allShardWriteLoads = allocation.clusterInfo().getShardWriteLoads();
        var shardWriteLoad = allShardWriteLoads.get(shardRouting.shardId());
        if (shardWriteLoad == null || shardWriteLoad == 0) {
            return Decision.YES;
        }

        var allNodeUsageStats = allocation.clusterInfo().getNodeUsageStatsForThreadPools();
        var nodeUsageStatsForThreadPools = allNodeUsageStats.get(node.nodeId());
        if (nodeUsageStatsForThreadPools == null) {
            // No node-level thread pool usage stats were reported for this node. Let's assume this is OK and that the simulator will handle
            // setting a node-level write load for this node after this shard is assigned.
            return Decision.YES;
        }

        // NOMERGE: should create a utility class (eventually, maybe duplicate code for this class, for now, if simulator does have bug)
        // to calculate the change node's usage change with assignment of the new shard
        assert nodeUsageStatsForThreadPools.threadPoolUsageStatsMap().isEmpty() == false;
        assert nodeUsageStatsForThreadPools.threadPoolUsageStatsMap().get(ThreadPool.Names.WRITE) != null;
        var nodeWriteThreadPoolStats = nodeUsageStatsForThreadPools.threadPoolUsageStatsMap().get(ThreadPool.Names.WRITE);
        var nodeWriteThreadPoolLoadThreshold = writeLoadConstraintSettings.getWriteThreadPoolHighUtilizationThresholdSetting();
        if (nodeWriteThreadPoolStats.averageThreadPoolUtilization() >= nodeWriteThreadPoolLoadThreshold) {
            // The node's write thread pool usage stats already show high utilization above the threshold for accepting new shards.
            logger.debug(
                "The high utilization threshold of {} has already been reached on node {}. Cannot allocate shard {} to node {} "
                    + "without risking increased write latencies.",
                nodeWriteThreadPoolLoadThreshold,
                node.nodeId(),
                shardRouting.shardId(),
                node.nodeId()
            );
            return Decision.NO;
        }

        if (nodeWriteThreadPoolStats.averageThreadPoolUtilization() + calculateShardMovementChange(nodeWriteThreadPoolStats, shardWriteLoad) >= nodeWriteThreadPoolLoadThreshold) {
            // The node's write thread pool usage would be raised above the high utilization threshold. This could lead to a hot spot on
            // this node and is undesirable.
            logger.debug(
                "The high utilization threshold of {} would be exceeded on node {} if shard {} with estimated write load {} were "
                    + "assigned to it. Cannot allocate shard {} to node {} without risking increased write latencies.",
                nodeWriteThreadPoolLoadThreshold,
                node.nodeId(),
                shardRouting.shardId(),
                shardWriteLoad,
                shardRouting.shardId(),
                node.nodeId()
            );
            return Decision.NO;
        }

        return Decision.YES;
    }

    @Override
    public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (writeLoadConstraintSettings.getWriteLoadConstraintEnabled() != WriteLoadConstraintSettings.WriteLoadDeciderStatus.ENABLED) {
            return Decision.YES;
        }

        return Decision.YES;
    }

    /**
     * Calculates the change to the node's write thread pool utilization percentage if the shard is added to the node.
     * Returns the percent thread pool utilization change.
     */
    private float calculateShardMovementChange(ThreadPoolUsageStats nodeWriteThreadPoolStats, double shardWriteLoad) {
        assert shardWriteLoad > 0;
        // NOMERGE: move this into an utility class, should be commonly accessible with the simulator.
        // TODO: implement..
        return 0;
    }
}
