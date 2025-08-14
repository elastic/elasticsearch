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
import org.elasticsearch.cluster.routing.ShardMovementWriteLoadSimulator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintSettings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.Strings;
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
        if (writeLoadConstraintSettings.getWriteLoadConstraintEnabled().disabled()) {
            return Decision.single(Decision.Type.YES, NAME, "Decider is disabled");
        }

        // Check whether the shard being relocated has any write load estimate. If it does not, then this decider has no opinion.
        var allShardWriteLoads = allocation.clusterInfo().getShardWriteLoads();
        var shardWriteLoad = allShardWriteLoads.get(shardRouting.shardId());
        if (shardWriteLoad == null || shardWriteLoad == 0) {
            return Decision.single(Decision.Type.YES, NAME, "Shard has no estimated write load. Decider takes no action.");
        }

        var allNodeUsageStats = allocation.clusterInfo().getNodeUsageStatsForThreadPools();
        var nodeUsageStatsForThreadPools = allNodeUsageStats.get(node.nodeId());
        if (nodeUsageStatsForThreadPools == null) {
            // No node-level thread pool usage stats were reported for this node. Let's assume this is OK and that the simulator will handle
            // setting a node-level write load for this node after this shard is assigned.
            return Decision.single(Decision.Type.YES, NAME, "The node has no write load estimate. Decider takes no action.");
        }

        assert nodeUsageStatsForThreadPools.threadPoolUsageStatsMap().isEmpty() == false;
        assert nodeUsageStatsForThreadPools.threadPoolUsageStatsMap().get(ThreadPool.Names.WRITE) != null;
        var nodeWriteThreadPoolStats = nodeUsageStatsForThreadPools.threadPoolUsageStatsMap().get(ThreadPool.Names.WRITE);
        var nodeWriteThreadPoolLoadThreshold = writeLoadConstraintSettings.getWriteThreadPoolHighUtilizationThresholdSetting();
        if (nodeWriteThreadPoolStats.averageThreadPoolUtilization() >= nodeWriteThreadPoolLoadThreshold) {
            // The node's write thread pool usage stats already show high utilization above the threshold for accepting new shards.
            String explain = Strings.format(
                "Node [%s] with write thread pool utilization [%.2f] already exceeds the high utilization threshold of [%f]. Cannot "
                    + "allocate shard [%s] to node without risking increased write latencies.",
                node.nodeId(),
                nodeWriteThreadPoolStats.averageThreadPoolUtilization(),
                nodeWriteThreadPoolLoadThreshold,
                shardRouting.shardId()
            );
            logger.debug(explain);
            return Decision.single(Decision.Type.NO, NAME, explain);
        }

        if (calculateShardMovementChange(nodeWriteThreadPoolStats, shardWriteLoad) >= nodeWriteThreadPoolLoadThreshold) {
            // The node's write thread pool usage would be raised above the high utilization threshold with assignment of the new shard.
            // This could lead to a hot spot on this node and is undesirable.
            String explain = Strings.format(
                "The high utilization threshold of [%f] would be exceeded on node [%s] with utilization [%.2f] if shard [%s] with "
                    + "estimated additional utilisation [%.5f] (write load [%.5f] / threads [%d]) were assigned to it. Cannot allocate "
                    + "shard to node without risking increased write latencies.",
                nodeWriteThreadPoolLoadThreshold,
                node.nodeId(),
                nodeWriteThreadPoolStats.averageThreadPoolUtilization(),
                shardRouting.shardId(),
                shardWriteLoad / nodeWriteThreadPoolStats.totalThreadPoolThreads(),
                shardWriteLoad,
                nodeWriteThreadPoolStats.totalThreadPoolThreads()
            );
            logger.debug(explain);
            return Decision.single(Decision.Type.NO, NAME, explain);
        }

        return Decision.YES;
    }

    @Override
    public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (writeLoadConstraintSettings.getWriteLoadConstraintEnabled().notFullyEnabled()) {
            return Decision.single(Decision.Type.YES, NAME, "canRemain() is not enabled");
        }

        // TODO: implement

        return Decision.single(Decision.Type.YES, NAME, "canRemain() is not yet implemented");
    }

    /**
     * Calculates the change to the node's write thread pool utilization percentage if the shard is added to the node.
     * Returns the percent thread pool utilization change.
     */
    private float calculateShardMovementChange(ThreadPoolUsageStats nodeWriteThreadPoolStats, double shardWriteLoad) {
        assert shardWriteLoad > 0;
        return ShardMovementWriteLoadSimulator.updateNodeUtilizationWithShardMovements(
            nodeWriteThreadPoolStats.averageThreadPoolUtilization(),
            (float) shardWriteLoad,
            nodeWriteThreadPoolStats.totalThreadPoolThreads()
        );
    }
}
