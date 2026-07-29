/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.ObjectLongMap;

import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Accumulates heap usage deltas that result from shard movements, then applies them to the initial
 * heap estimates to produce a simulated result. Deltas are accumulated first and estimates updated
 * with clamping applied at the end so that we don't produce negative estimates. This minimises the
 * error introduced by the clamping.
 */
class NodeHeapMemoryShardMovementSimulator {
    private final ObjectLongMap<String> totalUsageDeltaByNode;
    private final ObjectLongMap<String> hostedShardUsageDeltaByNode;
    private final Map<String, NodeHeapMetrics> initialNodeHeapMetrics;
    private final Map<ShardId, ShardAndIndexHeapUsage> estimatedShardHeapUsages;
    private final ShardAndIndexHeapUsage defaultShardHeapUsageForShardsWithoutMetrics;
    private final RoutingNodes routingNodes;

    NodeHeapMemoryShardMovementSimulator(
        Map<String, NodeHeapMetrics> initialNodeHeapMetrics,
        Map<ShardId, ShardAndIndexHeapUsage> estimatedShardHeapUsages,
        ShardAndIndexHeapUsage defaultShardHeapUsageForShardsWithoutMetrics,
        RoutingNodes routingNodes
    ) {
        this.initialNodeHeapMetrics = initialNodeHeapMetrics;
        this.estimatedShardHeapUsages = estimatedShardHeapUsages;
        this.defaultShardHeapUsageForShardsWithoutMetrics = defaultShardHeapUsageForShardsWithoutMetrics;
        this.routingNodes = routingNodes;
        this.totalUsageDeltaByNode = new ObjectLongHashMap<>();
        this.hostedShardUsageDeltaByNode = new ObjectLongHashMap<>();
    }

    void simulateShardStarted(ShardRouting shard, boolean includeIndexUsage) {
        // Started on, or relocate to, the current node assignment.
        modifyHeapUsage(routingNodes.node(shard.currentNodeId()), shard.shardId(), Modification.ADD, includeIndexUsage);

        if (shard.relocatingNodeId() != null) {
            // Shard relocation from another node, so remove the stats from the previous node.
            modifyHeapUsage(routingNodes.node(shard.relocatingNodeId()), shard.shardId(), Modification.REMOVE, includeIndexUsage);
        }
    }

    void simulateAddIndexToNode(String nodeId, Index index) {
        // Don't simulate shard movement for nodes that we have no initial estimate for, we need the initial estimate to apply the deltas.
        if (initialNodeHeapMetrics.containsKey(nodeId) == false) {
            return;
        }
        // Use any shard ID since index stats are the same.
        var shardAndIndexHeap = estimatedShardHeapUsages.getOrDefault(new ShardId(index, 0), defaultShardHeapUsageForShardsWithoutMetrics);
        totalUsageDeltaByNode.addTo(nodeId, shardAndIndexHeap.indexHeapUsageBytes());
    }

    void simulateRemoveIndexFromNode(String nodeId, Index index) {
        // Don't simulate shard movement for nodes that we have no initial estimate for, we need the initial estimate to apply the deltas.
        if (initialNodeHeapMetrics.containsKey(nodeId) == false) {
            return;
        }
        // Use any shard ID since index stats are the same.
        var shardAndIndexHeap = estimatedShardHeapUsages.getOrDefault(new ShardId(index, 0), defaultShardHeapUsageForShardsWithoutMetrics);
        totalUsageDeltaByNode.addTo(nodeId, -1 * shardAndIndexHeap.indexHeapUsageBytes());
    }

    private enum Modification {
        ADD,
        REMOVE
    }

    private void modifyHeapUsage(@Nullable RoutingNode routingNode, ShardId shardId, Modification modification, boolean includeIndexUsage) {
        // Don't simulate shard movement for nodes that we have no initial estimate for, we need the initial estimate to apply the deltas.
        if (routingNode == null || initialNodeHeapMetrics.containsKey(routingNode.nodeId()) == false) {
            return;
        }
        var shardAndIndexHeap = estimatedShardHeapUsages.getOrDefault(shardId, defaultShardHeapUsageForShardsWithoutMetrics);
        var numberOfShardsForIndex = routingNode.numberOfOwningShardsForIndex(shardId.getIndex());
        long indexUsageDelta = 0;
        long shardUsageDelta = 0;
        switch (modification) {
            case ADD -> {
                if (includeIndexUsage && numberOfShardsForIndex == 1) {
                    // This node's index only has the initializing shard, which is now being added in simulation. This is the node's
                    // first shard for the index, and the index-level heap usage overhead must be added.
                    indexUsageDelta = shardAndIndexHeap.indexHeapUsageBytes();
                }
                shardUsageDelta = shardAndIndexHeap.shardHeapUsageBytes();
            }
            case REMOVE -> {
                if (includeIndexUsage && numberOfShardsForIndex == 0) {
                    // This node only had one shard of the index, which is now being relocated away in simulation. The index-level heap
                    // usage overhead must be subtracted, since the node will no longer have the index.
                    indexUsageDelta = -1 * shardAndIndexHeap.indexHeapUsageBytes();
                }
                shardUsageDelta = -1 * shardAndIndexHeap.shardHeapUsageBytes();
            }
        }

        // Update the deltas for the node
        totalUsageDeltaByNode.addTo(routingNode.nodeId(), indexUsageDelta + shardUsageDelta);
        hostedShardUsageDeltaByNode.addTo(routingNode.nodeId(), shardUsageDelta);
    }

    /**
     * Apply the deltas to the initial estimates, clamping the results to 0 to avoid producing negative estimates.
     */
    Map<String, NodeHeapMetrics> getSimulatedHeapMetrics() {
        // If there was no shard movement, just return the unchanged metrics
        if (totalUsageDeltaByNode.isEmpty()) {
            return initialNodeHeapMetrics;
        }
        return initialNodeHeapMetrics.entrySet().stream().collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, entry -> {
            if (totalUsageDeltaByNode.containsKey(entry.getKey())) {
                NodeHeapMetrics initialMetrics = entry.getValue();
                final var adjustedTotalUsage = Math.max(
                    0,
                    Math.addExact(initialMetrics.nodeHeapEstimates().totalHeapUsage(), totalUsageDeltaByNode.get(entry.getKey()))
                );
                final var adjustedHostedShardsUsage = Math.max(
                    0,
                    Math.addExact(
                        initialMetrics.nodeHeapEstimates().hostedShardsHeapUsage(),
                        hostedShardUsageDeltaByNode.get(entry.getKey())
                    )
                );
                return new NodeHeapMetrics(
                    initialMetrics.nodeId(),
                    initialMetrics.totalBytes(),
                    new NodeHeapEstimates(adjustedTotalUsage, adjustedHostedShardsUsage)
                );
            }
            return entry.getValue();
        }));
    }
}
