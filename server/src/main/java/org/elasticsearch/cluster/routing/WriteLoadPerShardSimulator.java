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

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Simulates the impact to each node's write-load in response to the movement of individual
 * shards around the cluster.
 */
public class WriteLoadPerShardSimulator {

    private final ObjectDoubleMap<String> simulatedWriteLoadDeltas;
    private final RoutingAllocation routingAllocation;
    private final Map<ShardId, Double> writeLoadsPerShard;

    public WriteLoadPerShardSimulator(RoutingAllocation routingAllocation) {
        this.routingAllocation = routingAllocation;
        this.simulatedWriteLoadDeltas = new ObjectDoubleHashMap<>();
        writeLoadsPerShard = routingAllocation.clusterInfo().getShardWriteLoads();
    }

    public void simulateShardStarted(ShardRouting shardRouting) {
        final Double writeLoadForShard = writeLoadsPerShard.get(shardRouting.shardId());
        if (writeLoadForShard != null) {
            if (shardRouting.relocatingNodeId() != null) {
                // relocating
                simulatedWriteLoadDeltas.addTo(shardRouting.relocatingNodeId(), -1 * writeLoadForShard);
                simulatedWriteLoadDeltas.addTo(shardRouting.currentNodeId(), writeLoadForShard);
            } else {
                // not sure how this would come about, perhaps when allocating a replica after a delay?
                simulatedWriteLoadDeltas.addTo(shardRouting.currentNodeId(), writeLoadForShard);
            }
        }
    }

    public Map<String, NodeUsageStatsForThreadPools> nodeUsageStatsForThreadPools() {
        return routingAllocation.clusterInfo()
            .getNodeUsageStatsForThreadPools()
            .entrySet()
            .stream()
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> {
                if (simulatedWriteLoadDeltas.containsKey(e.getKey())) {
                    return new NodeUsageStatsForThreadPools(
                        e.getKey(),
                        Maps.copyMapWithAddedOrReplacedEntry(
                            e.getValue().threadPoolUsageStatsMap(),
                            "write",
                            replaceWritePoolStats(e.getValue(), simulatedWriteLoadDeltas.get(e.getKey()))
                        )
                    );
                }
                return e.getValue();
            }));
    }

    private NodeUsageStatsForThreadPools.ThreadPoolUsageStats replaceWritePoolStats(
        NodeUsageStatsForThreadPools value,
        double writeLoadDelta
    ) {
        final NodeUsageStatsForThreadPools.ThreadPoolUsageStats writeThreadPoolStats = value.threadPoolUsageStatsMap()
            .get(ThreadPool.Names.WRITE);
        return new NodeUsageStatsForThreadPools.ThreadPoolUsageStats(
            writeThreadPoolStats.totalThreadPoolThreads(),
            (float) (writeThreadPoolStats.averageThreadPoolUtilization() + (writeLoadDelta / writeThreadPoolStats
                .totalThreadPoolThreads())),
            writeThreadPoolStats.averageThreadPoolQueueLatencyMillis()
        );
    }
}
