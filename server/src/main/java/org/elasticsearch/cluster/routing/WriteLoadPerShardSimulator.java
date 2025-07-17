/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import com.carrotsearch.hppc.ObjectFloatHashMap;
import com.carrotsearch.hppc.ObjectFloatMap;

import org.elasticsearch.cluster.NodeUsageStatsForThreadPools;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class WriteLoadPerShardSimulator {

    private final ObjectFloatMap<String> writeLoadDeltas;
    private final RoutingAllocation routingAllocation;
    private final ObjectFloatMap<ShardId> writeLoadsPerShard;

    public WriteLoadPerShardSimulator(RoutingAllocation routingAllocation) {
        this.routingAllocation = routingAllocation;
        this.writeLoadDeltas = new ObjectFloatHashMap<>();
        writeLoadsPerShard = estimateWriteLoadsPerShard(routingAllocation);
    }

    public void simulateShardStarted(ShardRouting shardRouting) {
        float writeLoadForShard = writeLoadsPerShard.get(shardRouting.shardId());
        if (writeLoadForShard > 0.0) {
            if (shardRouting.relocatingNodeId() != null) {
                // relocating
                writeLoadDeltas.addTo(shardRouting.relocatingNodeId(), -1 * writeLoadForShard);
                writeLoadDeltas.addTo(shardRouting.currentNodeId(), writeLoadForShard);
            } else {
                // not sure how this would come about, perhaps when allocating a replica after a delay?
                writeLoadDeltas.addTo(shardRouting.currentNodeId(), writeLoadForShard);
            }
        }
    }

    public Map<String, NodeUsageStatsForThreadPools> nodeUsageStatsForThreadPools() {
        return routingAllocation.clusterInfo()
            .getNodeUsageStatsForThreadPools()
            .entrySet()
            .stream()
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> {
                if (writeLoadDeltas.containsKey(e.getKey())) {
                    return new NodeUsageStatsForThreadPools(
                        e.getKey(),
                        Maps.copyMapWithAddedOrReplacedEntry(
                            e.getValue().threadPoolUsageStatsMap(),
                            "write",
                            replaceWritePoolStats(e.getValue(), writeLoadDeltas.get(e.getKey()))
                        )
                    );
                }
                return e.getValue();
            }));
    }

    private NodeUsageStatsForThreadPools.ThreadPoolUsageStats replaceWritePoolStats(
        NodeUsageStatsForThreadPools value,
        float writeLoadDelta
    ) {
        final NodeUsageStatsForThreadPools.ThreadPoolUsageStats writeThreadPoolStats = value.threadPoolUsageStatsMap()
            .get(ThreadPool.Names.WRITE);
        return new NodeUsageStatsForThreadPools.ThreadPoolUsageStats(
            writeThreadPoolStats.totalThreadPoolThreads(),
            writeThreadPoolStats.averageThreadPoolUtilization() + (writeLoadDelta / writeThreadPoolStats.totalThreadPoolThreads()),
            writeThreadPoolStats.averageThreadPoolQueueLatencyMillis()
        );
    }

    // Everything below this line can probably go once we are publishing shard-write-load estimates to the master

    private static ObjectFloatMap<ShardId> estimateWriteLoadsPerShard(RoutingAllocation allocation) {
        final Map<ShardId, Average> writeLoadPerShard = new HashMap<>();
        final Set<String> writeIndexNames = getWriteIndexNames(allocation);
        final Map<String, NodeUsageStatsForThreadPools> nodeUsageStatsForThreadPools = allocation.clusterInfo()
            .getNodeUsageStatsForThreadPools();
        for (final Map.Entry<String, NodeUsageStatsForThreadPools> usageStatsForThreadPoolsEntry : nodeUsageStatsForThreadPools
            .entrySet()) {
            final NodeUsageStatsForThreadPools value = usageStatsForThreadPoolsEntry.getValue();
            final NodeUsageStatsForThreadPools.ThreadPoolUsageStats writeThreadPoolStats = value.threadPoolUsageStatsMap()
                .get(ThreadPool.Names.WRITE);
            if (writeThreadPoolStats == null) {
                // No stats from this node yet
                continue;
            }
            float writeUtilisation = writeThreadPoolStats.averageThreadPoolUtilization() * writeThreadPoolStats.totalThreadPoolThreads();

            final String nodeId = usageStatsForThreadPoolsEntry.getKey();
            final RoutingNode node = allocation.routingNodes().node(nodeId);
            final Set<ShardId> writeShardsOnNode = new HashSet<>();
            for (final ShardRouting shardRouting : node) {
                if (shardRouting.role() != ShardRouting.Role.SEARCH_ONLY && writeIndexNames.contains(shardRouting.index().getName())) {
                    writeShardsOnNode.add(shardRouting.shardId());
                }
            }
            writeShardsOnNode.forEach(
                shardId -> writeLoadPerShard.computeIfAbsent(shardId, k -> new Average()).add(writeUtilisation / writeShardsOnNode.size())
            );
        }
        final ObjectFloatMap<ShardId> writeLoads = new ObjectFloatHashMap<>(writeLoadPerShard.size());
        writeLoadPerShard.forEach((shardId, average) -> writeLoads.put(shardId, average.get()));
        return writeLoads;
    }

    private static Set<String> getWriteIndexNames(RoutingAllocation allocation) {
        return allocation.metadata()
            .projects()
            .values()
            .stream()
            .map(ProjectMetadata::getIndicesLookup)
            .flatMap(il -> il.values().stream())
            .map(IndexAbstraction::getWriteIndex)
            .filter(Objects::nonNull)
            .map(Index::getName)
            .collect(Collectors.toUnmodifiableSet());
    }

    private static final class Average {
        int count;
        float sum;

        public void add(float value) {
            count++;
            sum += value;
        }

        public float get() {
            return sum / count;
        }
    }
}
