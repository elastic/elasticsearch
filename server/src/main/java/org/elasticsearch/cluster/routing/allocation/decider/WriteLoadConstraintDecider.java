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
import org.elasticsearch.cluster.NodeUsageStatsForThreadPools;
import org.elasticsearch.cluster.NodeUsageStatsForThreadPools.ThreadPoolUsageStats;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardMovementWriteLoadSimulator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintSettings;
import org.elasticsearch.common.FrequencyCappedAction;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Map;
import java.util.function.DoubleSupplier;

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

    public static double maxShardWriteLoadProportion(List<ShardId> assignedShardIds, Map<ShardId, Double> shardWriteLoads) {
        double totalWriteLoad = 0.0;
        double maxShardWriteLoad = 0.0;
        for (ShardId shardId : assignedShardIds) {
            double shardWriteLoad = shardWriteLoads.getOrDefault(shardId, 0.0);
            totalWriteLoad += shardWriteLoad;
            if (shardWriteLoad > maxShardWriteLoad) {
                maxShardWriteLoad = shardWriteLoad;
            }
        }

        if (totalWriteLoad > 0.0) {
            return maxShardWriteLoad / totalWriteLoad;
        } else {
            // no shards or some issue -- return 0.0
            return 0.0;
        }
    }

    public static boolean maxShardWriteLoadProportionIsHigh(double maxShardWriteLoadProportion, double maxShardWriteLoadThreshold) {
        if (maxShardWriteLoadThreshold == 0.0) {
            return false;
        }
        return maxShardWriteLoadProportion >= maxShardWriteLoadThreshold;
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

        final boolean nodeIsHotspotting = nodeIsHotspotting(
            nodeUsageStatsForThreadPools,
            nodeWriteThreadPoolQueueLatencyThreshold,
            nodeWriteThreadPoolUtilizationThreshold
        );

        if (nodeIsHotspotting) {
            // When a node is hot-spotting, but its write-load is too focused on a single shard, then trying to correct
            // it with a shard move is useless: the node that receives the shard will hotspot instead, and an important
            // shard will be unavailable briefly when it moves.
            //
            // The maxShardWriteLoadProportion is computed only for hot-spotting nodes, and cached within cluster info so it
            // is only computed once per balancing round.
            final double maxShardWriteLoadThreshold = writeLoadConstraintSettings.getHotspotMaxShardWriteLoadProportionThreshold();
            final DoubleSupplier maxShardWriteLoadProportion = () -> allocation.clusterInfo()
                .nodeMaxShardWriteLoadProportion(
                    node.nodeId(),
                    // compute cache entry if absent
                    () -> {
                        final var shardIds = node.shardsWithState(ShardRoutingState.STARTED).map(ShardRouting::shardId).toList();
                        return maxShardWriteLoadProportion(shardIds, allocation.clusterInfo().getShardWriteLoads());
                    }
                );

            // check that the threshold comparison is enabled (not 0.0) before computing the maxShardWriteLoadProportion
            final double maxShardWriteLoadProportionCalculated = maxShardWriteLoadThreshold == 0.0
                ? Double.NaN
                : maxShardWriteLoadProportion.getAsDouble();
            if (maxShardWriteLoadThreshold == 0.0
                || maxShardWriteLoadProportionIsHigh(maxShardWriteLoadProportionCalculated, maxShardWriteLoadThreshold) == false) {
                if (logger.isDebugEnabled() || allocation.debugDecision()) {
                    final Double shardWriteLoad = getShardWriteLoad(allocation, shardRouting);
                    final String explain = Strings.format(
                        """
                            Node [%s] has a queue latency of [%d] millis that exceeds the queue latency threshold of [%s] and a thread \
                            pool utilization of [%f] that exceeds the utilization threshold of [%s]. This node is hot-spotting. Shard \
                            write load [%s]. %s. Should move shard(s) away""",
                        node.getShortNodeDescription(),
                        nodeWriteThreadPoolStats.maxThreadPoolQueueLatencyMillis(),
                        nodeWriteThreadPoolQueueLatencyThreshold.toHumanReadableString(2),
                        nodeWriteThreadPoolStats.averageThreadPoolUtilization(),
                        writeLoadConstraintSettings.getHotspotUtilizationThresholdString(),
                        shardWriteLoad == null ? "unknown" : shardWriteLoad,
                        maxShardWriteLoadThreshold == 0.0
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
            } else {
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

        return allocation.decision(
            Decision.YES,
            NAME,
            """
                Node [%s]'s queue latency of [%d] does not exceed the latency threshold of [%s], or the thread pool utilization of [%f] \
                does not exceed the utilization threshold of [%s]""",
            node.getShortNodeDescription(),
            nodeWriteThreadPoolStats.maxThreadPoolQueueLatencyMillis(),
            nodeWriteThreadPoolQueueLatencyThreshold.toHumanReadableString(2),
            nodeWriteThreadPoolStats.averageThreadPoolUtilization(),
            writeLoadConstraintSettings.getHotspotUtilizationThresholdString()
        );
    }

    @Nullable
    private Double getShardWriteLoad(RoutingAllocation allocation, ShardRouting shardRouting) {
        return allocation.clusterInfo().getShardWriteLoads().get(shardRouting.shardId());
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

}
