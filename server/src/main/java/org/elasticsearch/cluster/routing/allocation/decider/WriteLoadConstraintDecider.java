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
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.NodeUsageStatsForThreadPools;
import org.elasticsearch.cluster.NodeUsageStatsForThreadPools.ThreadPoolUsageStats;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardMovementWriteLoadSimulator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintSettings;
import org.elasticsearch.common.FrequencyCappedAction;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Comparator;
import java.util.Map;

/**
 * Decides whether shards can be allocated to cluster nodes, or can remain on cluster nodes, based on the target node's current write thread
 * pool usage stats and any candidate shard's write load estimate.
 */
public class WriteLoadConstraintDecider extends AllocationDecider {
    private static final Logger logger = LogManager.getLogger(WriteLoadConstraintDecider.class);

    public static final String NAME = "write_load";

    private final FrequencyCappedAction logCanRemainMessage;
    private final FrequencyCappedAction logCanAllocateMessage;
    private final WriteLoadConstraintSettings writeLoadConstraintSettings;

    public WriteLoadConstraintDecider(ClusterSettings clusterSettings) {
        this.writeLoadConstraintSettings = new WriteLoadConstraintSettings(clusterSettings);
        logCanRemainMessage = new FrequencyCappedAction(System::currentTimeMillis, TimeValue.ZERO);
        logCanAllocateMessage = new FrequencyCappedAction(System::currentTimeMillis, TimeValue.ZERO);
        clusterSettings.initializeAndWatch(WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_MINIMUM_LOGGING_INTERVAL, timeValue -> {
            logCanRemainMessage.setMinInterval(timeValue);
            logCanAllocateMessage.setMinInterval(timeValue);
        });
    }

    /**
     * @return Whether a node is currently hotspotting, given the threshold criteria for queue latency and utilization
     */
    public static boolean nodeIsHotspotting(
        NodeUsageStatsForThreadPools nodeUsageStatsForThreadPools,
        TimeValue hotspotQueueLatencyThreshold,
        double hotspotUtilizationThreshold
    ) {
        assert nodeUsageStatsForThreadPools.threadPoolUsageStatsMap().isEmpty() == false;
        assert nodeUsageStatsForThreadPools.threadPoolUsageStatsMap().get(ThreadPool.Names.WRITE) != null;
        var nodeWriteThreadPoolStats = nodeUsageStatsForThreadPools.threadPoolUsageStatsMap().get(ThreadPool.Names.WRITE);
        return nodeWriteThreadPoolStats.maxThreadPoolQueueLatencyMillis() >= hotspotQueueLatencyThreshold.millis()
            && nodeWriteThreadPoolStats.averageThreadPoolUtilization() >= hotspotUtilizationThreshold;
    }

    /**
     * Returns true when a shard's write load is below or equal to the minimum threshold, meaning the shard cannot
     * meaningfully relieve a hotspot by being moved. Returns false (not negligible) when the threshold
     * is -1.0 (disabled).
     */
    public static boolean isShardWriteLoadContributionNegligible(double minThreshold, double shardWriteLoad) {
        if (minThreshold < 0.0) {
            return false;
        }
        return shardWriteLoad <= minThreshold;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (writeLoadConstraintSettings.getWriteLoadConstraintEnabled().disabled()) {
            return allocation.decision(Decision.YES, NAME, "Decider is disabled");
        }

        var allNodeUsageStats = allocation.clusterInfo().getNodeUsageStatsForThreadPools();
        var nodeUsageStatsForThreadPools = allNodeUsageStats.get(node.nodeId());
        if (nodeUsageStatsForThreadPools == null) {
            // No node-level thread pool usage stats were reported for this node. Let's assume this is OK and that the simulator will handle
            // setting a node-level write load for this node after this shard is assigned.
            return allocation.decision(Decision.YES, NAME, "The node has no write load estimate. Decider takes no action.");
        }

        assert nodeUsageStatsForThreadPools.threadPoolUsageStatsMap().isEmpty() == false;
        assert nodeUsageStatsForThreadPools.threadPoolUsageStatsMap().get(ThreadPool.Names.WRITE) != null;
        var nodeWriteThreadPoolStats = nodeUsageStatsForThreadPools.threadPoolUsageStatsMap().get(ThreadPool.Names.WRITE);
        var nodeWriteThreadPoolLoadAllocationThreshold = writeLoadConstraintSettings.getAllocationUtilizationThreshold();
        if (nodeWriteThreadPoolStats.averageThreadPoolUtilization() >= nodeWriteThreadPoolLoadAllocationThreshold) {
            // The node's write thread pool usage stats already show high utilization above the threshold for accepting new shards.
            if (logger.isDebugEnabled() || allocation.debugDecision()) {
                final String explain = Strings.format(
                    "Node [%s] with write thread pool utilization [%.2f] already exceeds the high utilization threshold of [%f]. Cannot "
                        + "allocate shard [%s] to node without risking increased write latencies.",
                    node.getShortNodeDescription(),
                    nodeWriteThreadPoolStats.averageThreadPoolUtilization(),
                    nodeWriteThreadPoolLoadAllocationThreshold,
                    shardRouting.shardId()
                );
                if (logger.isDebugEnabled()) {
                    logCanAllocateMessage.maybeExecute(() -> logger.debug(explain));
                }
                return allocation.decision(Decision.NOT_PREFERRED, NAME, explain);
            } else {
                return Decision.NOT_PREFERRED;
            }
        } else if (allocation.clusterInfo().nodeIsWriteLoadHotspotting(node.nodeId())) {
            return allocation.decision(
                Decision.NOT_PREFERRED,
                NAME,
                "Node [%s] is currently hot-spotting or in a waiting period, and does not prefer shards moved onto it",
                node.nodeId()
            );
        }

        var allShardWriteLoads = allocation.clusterInfo().getShardWriteLoads();
        var shardWriteLoad = allShardWriteLoads.getOrDefault(shardRouting.shardId(), 0.0);
        var newWriteThreadPoolUtilization = calculateShardMovementChange(nodeWriteThreadPoolStats, shardWriteLoad);
        if (newWriteThreadPoolUtilization >= nodeWriteThreadPoolLoadAllocationThreshold) {
            // The node's write thread pool usage would be raised above the high utilization threshold with assignment of the new shard.
            // This could lead to a hot spot on this node and is undesirable.
            if (logger.isDebugEnabled() || allocation.debugDecision()) {
                final String explain = Strings.format(
                    "The high utilization threshold of [%f] would be exceeded on node [%s] with utilization [%.2f] if shard [%s] with "
                        + "estimated additional utilisation [%.5f] (write load [%.5f] / threads [%d]) were assigned to it. Cannot allocate "
                        + "shard to node without risking increased write latencies.",
                    nodeWriteThreadPoolLoadAllocationThreshold,
                    node.getShortNodeDescription(),
                    nodeWriteThreadPoolStats.averageThreadPoolUtilization(),
                    shardRouting.shardId(),
                    shardWriteLoad / nodeWriteThreadPoolStats.totalThreadPoolThreads(),
                    shardWriteLoad,
                    nodeWriteThreadPoolStats.totalThreadPoolThreads()
                );
                if (logger.isDebugEnabled()) {
                    logCanAllocateMessage.maybeExecute(() -> logger.debug(explain));
                }
                return allocation.decision(Decision.NOT_PREFERRED, NAME, explain);
            } else {
                return Decision.NOT_PREFERRED;
            }
        }

        return allocation.decision(
            Decision.YES,
            NAME,
            "Shard [%s] in index [%s] can be assigned to node [%s]. The node's utilization would become [%s]",
            shardRouting.shardId(),
            shardRouting.index(),
            node.getShortNodeDescription(),
            newWriteThreadPoolUtilization
        );
    }

    @Override
    public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (writeLoadConstraintSettings.getWriteLoadConstraintEnabled().notFullyEnabled()) {
            return allocation.decision(Decision.YES, NAME, "canRemain() is not enabled");
        }

        var allNodeUsageStats = allocation.clusterInfo().getNodeUsageStatsForThreadPools();
        var nodeUsageStatsForThreadPools = allNodeUsageStats.get(node.nodeId());
        if (nodeUsageStatsForThreadPools == null) {
            // No node-level thread pool usage stats were reported for this node. Let's assume this is OK and that the simulator will handle
            // setting a node-level write load for this node after this shard is assigned.
            return allocation.decision(Decision.YES, NAME, "The node has no write load estimate. Decider takes no action.");
        }

        var nodeWriteThreadPoolStats = nodeUsageStatsForThreadPools.threadPoolUsageStatsMap().get(ThreadPool.Names.WRITE);
        var nodeWriteThreadPoolQueueLatencyThreshold = writeLoadConstraintSettings.getQueueLatencyThreshold();
        var nodeWriteThreadPoolUtilizationThreshold = writeLoadConstraintSettings.getHotspotUtilizationThreshold();

        // First, check if the node is hot-spotting. If not, then this decider will not consider the shard for movement.
        final boolean nodeIsHotspotting = nodeIsHotspotting(
            nodeUsageStatsForThreadPools,
            nodeWriteThreadPoolQueueLatencyThreshold,
            nodeWriteThreadPoolUtilizationThreshold
        );
        if (nodeIsHotspotting == false) {
            return allocation.decision(
                Decision.YES,
                NAME,
                """
                    Node [%s]'s queue latency of [%d] does not exceed the latency threshold of [%s], or the thread pool utilization of \
                    [%f] does not exceed the utilization threshold of [%s]""",
                node.getShortNodeDescription(),
                nodeWriteThreadPoolStats.maxThreadPoolQueueLatencyMillis(),
                nodeWriteThreadPoolQueueLatencyThreshold.toHumanReadableString(2),
                nodeWriteThreadPoolStats.averageThreadPoolUtilization(),
                writeLoadConstraintSettings.getHotspotUtilizationThresholdString()
            );
        }

        // When a node is hot-spotting, but its write-load is too focused on a single shard, then trying to correct
        // it with a shard move is useless: the node that receives the shard will hotspot instead, and an important
        // shard will be unavailable briefly when it moves.
        //
        // The maxShardWriteLoadProportion is computed only for hot-spotting nodes, and cached within the routing allocation so it
        // is only computed once per balancing round.
        final double maxShardWriteLoadThreshold = writeLoadConstraintSettings.getHotspotMaxShardWriteLoadProportionThreshold();

        // check that the threshold comparison is enabled (not 0.0) before computing the maxShardWriteLoadProportion
        double maxShardWriteLoadProportionCalculated = Double.NaN;
        if (maxShardWriteLoadThreshold != 0.0) {
            maxShardWriteLoadProportionCalculated = allocation.maxShardWriteLoadProportionForNode(node);
            if (maxShardWriteLoadProportionCalculated >= maxShardWriteLoadThreshold) {
                return allocation.decision(
                    Decision.YES,
                    NAME,
                    """
                        Node [%s] is hot-spotting due to a single shard executing [%.2f] percent of the writes. But since this is above \
                        the single shard write load threshold ([%s]), moving shards away from this node is not expected to resolve \
                        the hot-spot.""",
                    node.getShortNodeDescription(),
                    maxShardWriteLoadProportionCalculated * 100,
                    writeLoadConstraintSettings.getHotspotMaxShardWriteLoadProportionThresholdString()
                );
            }
        }

        // We know the node is hot-spotting, we know the load is not concentrated on a single shard, but there is no
        // point moving a shard with almost no write-load to try and remedy a hot-spot. Cluster nodes often host many shards
        // with little-to-no write-load, for example; shards of system indices, or shards of data stream indices other than
        // the current write-index. It makes no sense to shuffle these around when a node is hot-spotting.
        final double minShardWriteLoadThreshold = writeLoadConstraintSettings.getHotspotMinShardWriteLoadThreshold();
        final double shardWriteLoad = getShardWriteLoad(allocation, shardRouting);
        if (isShardWriteLoadContributionNegligible(minShardWriteLoadThreshold, shardWriteLoad)) {
            final var threadPoolUsageStats = nodeUsageStatsForThreadPools.threadPoolUsageStatsMap().get(ThreadPool.Names.WRITE);
            final double totalNodeWriteLoad = threadPoolUsageStats.averageThreadPoolUtilization() * threadPoolUsageStats
                .totalThreadPoolThreads();
            return allocation.decision(
                Decision.YES,
                NAME,
                """
                    Node [%s] is hot-spotting, but shard [%s] has write load [%.5f], which is at or below the minimum threshold [%.5f].
                    The total node write-load is [%.5f]; this shard contributes only [%.2f%%] of the total, so moving it would
                    do little to resolve the hot-spot.""",
                node.getShortNodeDescription(),
                shardRouting.shardId(),
                shardWriteLoad,
                minShardWriteLoadThreshold,
                totalNodeWriteLoad,
                (shardWriteLoad / totalNodeWriteLoad) * 100
            );
        }

        // If we got this far, we are hot-spotting and this shard is a reasonable candidate for movement. Return NOT_PREFERRED
        if (logger.isDebugEnabled() || allocation.debugDecision()) {
            final String explain = Strings.format(
                """
                    Node [%s] has a queue latency of [%d] millis that exceeds the queue latency threshold of [%s] and a thread \
                    pool utilization of [%f] that exceeds the utilization threshold of [%s]. This node is hot-spotting. Shard \
                    write load [%.5f]. %s. Should move shard(s) away""",
                node.getShortNodeDescription(),
                nodeWriteThreadPoolStats.maxThreadPoolQueueLatencyMillis(),
                nodeWriteThreadPoolQueueLatencyThreshold.toHumanReadableString(2),
                nodeWriteThreadPoolStats.averageThreadPoolUtilization(),
                writeLoadConstraintSettings.getHotspotUtilizationThresholdString(),
                shardWriteLoad,
                Double.isNaN(maxShardWriteLoadProportionCalculated)
                    ? "Max shard write-load proportion is disabled"
                    : Strings.format(
                        "The max shard write-load proportion on this node is %.1f%%, below the single-hot-shard threshold of %s",
                        maxShardWriteLoadProportionCalculated * 100,
                        writeLoadConstraintSettings.getHotspotMaxShardWriteLoadProportionThresholdString()
                    )
            );
            if (logger.isDebugEnabled()) {
                logCanRemainMessage.maybeExecute(() -> logger.debug(explain));
            }
            return allocation.decision(Decision.NOT_PREFERRED, NAME, explain);
        } else {
            return Decision.NOT_PREFERRED;
        }
    }

    /**
     * Returns the write-load move order for a hotspotting node, or {@code null} when this decider does not want shards moved off it.
     */
    @Override
    @Nullable
    public Comparator<ShardRouting> shardMoveOrder(RoutingNode node, RoutingAllocation allocation) {
        // If this decider is not fully enabled then we have nothing to return
        if (writeLoadConstraintSettings.getWriteLoadConstraintEnabled().notFullyEnabled()) {
            return null;
        }
        var nodeUsageStats = allocation.clusterInfo().getNodeUsageStatsForThreadPools().get(node.nodeId());
        // Without thread-pool stats we cannot tell whether this node is hot.
        if (nodeUsageStats == null) {
            return null;
        }
        // A node that is within both hotspot thresholds does not need a shard moved off it.
        if (nodeIsHotspotting(
            nodeUsageStats,
            writeLoadConstraintSettings.getQueueLatencyThreshold(),
            writeLoadConstraintSettings.getHotspotUtilizationThreshold()
        ) == false) {
            return null;
        }
        // One shard already accounts for too much of the write load. Moving it would hotspot the destination instead,
        // and moving anything else would not relieve this node. A threshold of 0 disables this check.
        double maxShardWriteLoadThreshold = writeLoadConstraintSettings.getHotspotMaxShardWriteLoadProportionThreshold();
        if (maxShardWriteLoadThreshold != 0.0 && allocation.maxShardWriteLoadProportionForNode(node) >= maxShardWriteLoadThreshold) {
            return null;
        }
        // Prefer shards near half of this node's maximum write load.
        // Shards with negligible write loads will remain on this node since canRemain will return YES
        return new PrioritiseByShardWriteLoadComparator(allocation.clusterInfo(), node);
    }

    /**
     * Get the write-load for the specified shard
     *
     * @param allocation The RoutingAllocation instance
     * @param shardRouting The shard whose write-load is being requested
     * @return The write-load for the specified shard, or 0.0 if no write-load has been reported
     */
    private double getShardWriteLoad(RoutingAllocation allocation, ShardRouting shardRouting) {
        return allocation.clusterInfo().getShardWriteLoads().getOrDefault(shardRouting.shardId(), 0.0);
    }

    /**
     * Calculates the change to the node's write thread pool utilization percentage if the shard is added to the node.
     * Returns the percent thread pool utilization change.
     */
    private float calculateShardMovementChange(ThreadPoolUsageStats nodeWriteThreadPoolStats, double shardWriteLoad) {
        return ShardMovementWriteLoadSimulator.updateNodeUtilizationWithShardMovements(
            nodeWriteThreadPoolStats.averageThreadPoolUtilization(),
            (float) shardWriteLoad,
            nodeWriteThreadPoolStats.totalThreadPoolThreads()
        );
    }

    /**
     * Sorts shards by desirability to move, in the sort order:
     * <ol>
     *     <li>Shards with write-load in the range <i>{@link #threshold}</i> &rarr; {@link #maxWriteLoadOnNode} (exclusive)</li>
     *     <li>Shards with write-load in the range <i>{@link #threshold}</i> &rarr; 0</li>
     *     <li>Shards with write-load == {@link #maxWriteLoadOnNode}</li>
     *     <li>Shards with missing write-load</li>
     * </ol>
     */
    public static class PrioritiseByShardWriteLoadComparator implements Comparator<ShardRouting> {

        /**
         * This is the threshold over which we consider shards to have a "high" write load represented
         * as a ratio of the maximum write-load present on the node.
         * <p>
         * We prefer to move shards that have a write-load close to <b>this value</b> x {@link #maxWriteLoadOnNode}.
         */
        public static final double THRESHOLD_RATIO = 0.5;
        private static final double MISSING_WRITE_LOAD = -1;
        private final Map<ShardId, Double> shardWriteLoads;
        private final double maxWriteLoadOnNode;
        private final double threshold;
        private final String nodeId;

        public PrioritiseByShardWriteLoadComparator(ClusterInfo clusterInfo, RoutingNode routingNode) {
            shardWriteLoads = clusterInfo.getShardWriteLoads();
            double maxWriteLoadOnNode = MISSING_WRITE_LOAD;
            for (ShardRouting shardRouting : routingNode) {
                maxWriteLoadOnNode = Math.max(maxWriteLoadOnNode, shardWriteLoads.getOrDefault(shardRouting.shardId(), MISSING_WRITE_LOAD));
            }
            this.maxWriteLoadOnNode = maxWriteLoadOnNode;
            threshold = maxWriteLoadOnNode * THRESHOLD_RATIO;
            nodeId = routingNode.nodeId();
        }

        @Override
        public int compare(ShardRouting lhs, ShardRouting rhs) {
            assert nodeId.equals(lhs.currentNodeId()) && nodeId.equals(rhs.currentNodeId())
                : this.getClass().getSimpleName()
                    + " is node-specific. comparator="
                    + nodeId
                    + ", lhs="
                    + lhs.currentNodeId()
                    + ", rhs="
                    + rhs.currentNodeId();

            // If we have no shard write-load data, shortcut
            if (maxWriteLoadOnNode == MISSING_WRITE_LOAD) {
                return 0;
            }

            final double lhsWriteLoad = shardWriteLoads.getOrDefault(lhs.shardId(), MISSING_WRITE_LOAD);
            final double rhsWriteLoad = shardWriteLoads.getOrDefault(rhs.shardId(), MISSING_WRITE_LOAD);

            // prefer any known write-load over any unknown write-load
            final var rhsIsMissing = rhsWriteLoad == MISSING_WRITE_LOAD;
            final var lhsIsMissing = lhsWriteLoad == MISSING_WRITE_LOAD;
            if (rhsIsMissing && lhsIsMissing) {
                return 0;
            }
            if (rhsIsMissing ^ lhsIsMissing) {
                return lhsIsMissing ? 1 : -1;
            }

            if (lhsWriteLoad < maxWriteLoadOnNode && rhsWriteLoad < maxWriteLoadOnNode) {
                final var lhsOverThreshold = lhsWriteLoad >= threshold;
                final var rhsOverThreshold = rhsWriteLoad >= threshold;
                if (lhsOverThreshold && rhsOverThreshold) {
                    // Both values between threshold and maximum, prefer lowest
                    return Double.compare(lhsWriteLoad, rhsWriteLoad);
                } else if (lhsOverThreshold) {
                    // lhs between threshold and maximum, rhs below threshold, prefer lhs
                    return -1;
                } else if (rhsOverThreshold) {
                    // lhs below threshold, rhs between threshold and maximum, prefer rhs
                    return 1;
                }
                // Both values below the threshold, prefer highest
                return Double.compare(rhsWriteLoad, lhsWriteLoad);
            }

            // prefer the non-max write load if there is one
            return Double.compare(lhsWriteLoad, rhsWriteLoad);
        }
    }

}
