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
import java.util.Map;

/**
 * Simulates the impact to each node's write-load in response to the movement of individual
 * shards around the cluster.
 */
public class ShardMovementWriteLoadSimulator {

    private final Map<String, NodeUsageStatsForThreadPools> originalNodeUsageStatsForThreadPools;
    private final ObjectDoubleMap<String> simulatedNodeWriteLoadDeltas;
    private final Map<ShardId, Double> writeLoadsPerShard;

    public ShardMovementWriteLoadSimulator(RoutingAllocation routingAllocation) {
        this.originalNodeUsageStatsForThreadPools = routingAllocation.clusterInfo().getNodeUsageStatsForThreadPools();
        this.writeLoadsPerShard = routingAllocation.clusterInfo().getShardWriteLoads();
        this.simulatedNodeWriteLoadDeltas = new ObjectDoubleHashMap<>();
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
    }

    /**
     * Apply the simulated shard movements to the original thread pool usage stats for each node.
     */
    public Map<String, NodeUsageStatsForThreadPools> simulatedNodeUsageStatsForThreadPools() {
        final Map<String, NodeUsageStatsForThreadPools> adjustedNodeUsageStatsForThreadPools = Maps.newMapWithExpectedSize(
            originalNodeUsageStatsForThreadPools.size()
        );
        for (Map.Entry<String, NodeUsageStatsForThreadPools> entry : originalNodeUsageStatsForThreadPools.entrySet()) {
            if (simulatedNodeWriteLoadDeltas.containsKey(entry.getKey())) {
                var adjustedValue = new NodeUsageStatsForThreadPools(
                    entry.getKey(),
                    Maps.copyMapWithAddedOrReplacedEntry(
                        entry.getValue().threadPoolUsageStatsMap(),
                        ThreadPool.Names.WRITE,
                        replaceWritePoolStats(entry.getValue(), simulatedNodeWriteLoadDeltas.get(entry.getKey()))
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
        double writeLoadDelta
    ) {
        final NodeUsageStatsForThreadPools.ThreadPoolUsageStats writeThreadPoolStats = value.threadPoolUsageStatsMap()
            .get(ThreadPool.Names.WRITE);
        return new NodeUsageStatsForThreadPools.ThreadPoolUsageStats(
            writeThreadPoolStats.totalThreadPoolThreads(),
            (float) Math.max(
                (writeThreadPoolStats.averageThreadPoolUtilization() + (writeLoadDelta / writeThreadPoolStats.totalThreadPoolThreads())),
                0.0
            ),
            writeThreadPoolStats.maxThreadPoolQueueLatencyMillis()
        );
    }
}
