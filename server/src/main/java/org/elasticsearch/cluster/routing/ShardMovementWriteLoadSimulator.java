/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import com.carrotsearch.hppc.ObjectDoubleHashMap;
import com.carrotsearch.hppc.ObjectDoubleMap;

import org.elasticsearch.cluster.NodeUsageStatsForThreadPools;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Simulates the impact to each node's write-load in response to the movement of individual
 * shards around the cluster.
 */
public class ShardMovementWriteLoadSimulator {

    private final Map<String, NodeUsageStatsForThreadPools> originalNodeUsageStatsForThreadPools;
    private final ObjectDoubleMap<String> simulatedNodeWriteLoadDeltas;
    private final Map<ShardId, Double> writeLoadsPerShard;
    // The set to track whether a node has seen a shard move away from it
    private final Set<String> nodesWithMovedAwayShard;

    public ShardMovementWriteLoadSimulator(RoutingAllocation routingAllocation) {
        this.originalNodeUsageStatsForThreadPools = routingAllocation.clusterInfo().getNodeUsageStatsForThreadPools();
        this.writeLoadsPerShard = routingAllocation.clusterInfo().getShardWriteLoads();
        this.simulatedNodeWriteLoadDeltas = new ObjectDoubleHashMap<>();
        this.nodesWithMovedAwayShard = new HashSet<>();
    }

    public void simulateShardStarted(ShardRouting shardRouting) {
        final Double writeLoadForShard = writeLoadsPerShard.get(shardRouting.shardId());
        if (writeLoadForShard != null) {
            if (shardRouting.relocatingNodeId() != null) {
                assert shardRouting.state() == ShardRoutingState.INITIALIZING
                    : "This should only be happening on the destination node (the source node will have status RELOCATING)";
                // This is a shard being relocated
                simulatedNodeWriteLoadDeltas.addTo(shardRouting.relocatingNodeId(), -1 * writeLoadForShard);
                simulatedNodeWriteLoadDeltas.addTo(shardRouting.currentNodeId(), writeLoadForShard);
            } else {
                // This is a new shard starting, it's unlikely we'll have a write-load value for a new
                // shard, but we may be able to estimate if the new shard is created as part of a datastream
                // rollover. See https://elasticco.atlassian.net/browse/ES-12469
                simulatedNodeWriteLoadDeltas.addTo(shardRouting.currentNodeId(), writeLoadForShard);
            }
        }
        // Always record the moving shard regardless whether its write load is adjusted
        if (shardRouting.relocatingNodeId() != null) {
            nodesWithMovedAwayShard.add(shardRouting.relocatingNodeId());
        }
    }

    /**
     * Apply the simulated shard movements to the original thread pool usage stats for each node.
     */
    public Map<String, NodeUsageStatsForThreadPools> simulatedNodeUsageStatsForThreadPools() {
        final Map<String, NodeUsageStatsForThreadPools> adjustedNodeUsageStatsForThreadPools = Maps.newMapWithExpectedSize(
            originalNodeUsageStatsForThreadPools.size()
        );
        for (Map.Entry<String, NodeUsageStatsForThreadPools> entry : originalNodeUsageStatsForThreadPools.entrySet()) {
            if (simulatedNodeWriteLoadDeltas.containsKey(entry.getKey()) || nodesWithMovedAwayShard.contains(entry.getKey())) {
                var adjustedValue = new NodeUsageStatsForThreadPools(
                    entry.getKey(),
                    Maps.copyMapWithAddedOrReplacedEntry(
                        entry.getValue().threadPoolUsageStatsMap(),
                        ThreadPool.Names.WRITE,
                        replaceWritePoolStats(
                            entry.getValue(),
                            simulatedNodeWriteLoadDeltas.getOrDefault(entry.getKey(), 0.0),
                            nodesWithMovedAwayShard.contains(entry.getKey())
                        )
                    )
                );
                adjustedNodeUsageStatsForThreadPools.put(entry.getKey(), adjustedValue);
            } else {
                adjustedNodeUsageStatsForThreadPools.put(entry.getKey(), entry.getValue());
            }
        }
        return Collections.unmodifiableMap(adjustedNodeUsageStatsForThreadPools);
    }

    private static NodeUsageStatsForThreadPools.ThreadPoolUsageStats replaceWritePoolStats(
        NodeUsageStatsForThreadPools value,
        double writeLoadDelta,
        boolean hasSeenMovedAwayShard
    ) {
        final NodeUsageStatsForThreadPools.ThreadPoolUsageStats writeThreadPoolStats = value.threadPoolUsageStatsMap()
            .get(ThreadPool.Names.WRITE);
        return new NodeUsageStatsForThreadPools.ThreadPoolUsageStats(
            writeThreadPoolStats.totalThreadPoolThreads(),
            updateNodeUtilizationWithShardMovements(
                writeThreadPoolStats.averageThreadPoolUtilization(),
                (float) writeLoadDelta,
                writeThreadPoolStats.totalThreadPoolThreads()
            ),
            adjustThreadPoolQueueLatencyWithShardMovements(writeThreadPoolStats.maxThreadPoolQueueLatencyMillis(), hasSeenMovedAwayShard)
        );
    }

    /**
     * The {@code nodeUtilization} is the average utilization per thread for some duration of time. The {@code shardWriteLoadDelta} is the
     * sum of shards' total execution time. Dividing the shards total execution time by the number of threads provides the average
     * utilization of each write thread for those shards. The change in shard load can then be added to the node utilization.
     *
     * @param nodeUtilization The current node-level write load percent utilization.
     * @param shardWriteLoadDelta The change in shard(s) execution time across all threads. This can be positive or negative depending on
     *                            whether shards were moved onto the node or off of the node.
     * @param numberOfWriteThreads The number of threads available in the node's write thread pool.
     * @return The new node-level write load percent utilization after adding the shard write load delta.
     */
    public static float updateNodeUtilizationWithShardMovements(
        float nodeUtilization,
        float shardWriteLoadDelta,
        int numberOfWriteThreads
    ) {
        float newNodeUtilization = nodeUtilization + calculateUtilizationForWriteLoad(shardWriteLoadDelta, numberOfWriteThreads);
        return (float) Math.max(newNodeUtilization, 0.0);
    }

    /**
     * Calculate what percentage utilization increase would result from adding some amount of write-load
     *
     * @param totalShardWriteLoad The write-load being added/removed
     * @param numberOfThreads The number of threads in the node-being-added-to's write thread pool
     * @return The change in percentage utilization
     */
    public static float calculateUtilizationForWriteLoad(float totalShardWriteLoad, int numberOfThreads) {
        return totalShardWriteLoad / numberOfThreads;
    }

    /**
     * Adjust the max thread pool queue latency by accounting for whether shard has moved away from the node.
     * @param maxThreadPoolQueueLatencyMillis The current max thread pool queue latency.
     * @param hasSeenMovedAwayShard Whether the node has seen a shard move away from it.
     * @return The new adjusted max thread pool queue latency.
     */
    public static long adjustThreadPoolQueueLatencyWithShardMovements(long maxThreadPoolQueueLatencyMillis, boolean hasSeenMovedAwayShard) {
        // Intentionally keep it simple by reducing queue latency to zero if we move any shard off the node.
        // This means the node is considered as no longer hot-spotting (with respect to queue latency) once a shard moves away.
        // The next ClusterInfo will come in up-to 30 seconds later, and we will see the actual impact of the
        // shard movement and repeat the process. We keep the queue latency unchanged if shard moves onto the node.
        return hasSeenMovedAwayShard ? 0L : maxThreadPoolQueueLatencyMillis;
    }
}
