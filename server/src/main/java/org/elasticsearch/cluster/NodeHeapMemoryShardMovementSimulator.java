/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.shard.ShardId;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Tracks simulated shard placement and recomputes node heap estimates from per-shard heap estimates.
 * {@link ClusterInfo} is assembled asynchronously, so its node-level heap estimates and shard placement
 * metadata may reflect different cluster states. Recomputing the shard-derived portion from a single
 * placement view avoids applying movement deltas to a potentially inconsistent node-level snapshot.
 */
class NodeHeapMemoryShardMovementSimulator {
    private final Map<String, NodeHeapMetrics> initialNodeHeapMetrics;
    private final Map<ShardId, ShardAndIndexHeapUsage> estimatedShardHeapUsages;
    private final ShardAndIndexHeapUsage defaultShardHeapUsageForShardsWithoutMetrics;
    private final Map<String, Set<ShardId>> simulatedShardIdsByNode;
    private final Map<String, Long> nonShardHeapUsageByNode;
    private boolean shardPlacementModified;

    NodeHeapMemoryShardMovementSimulator(
        Map<String, NodeHeapMetrics> initialNodeHeapMetrics,
        Map<ShardId, ShardAndIndexHeapUsage> estimatedShardHeapUsages,
        ShardAndIndexHeapUsage defaultShardHeapUsageForShardsWithoutMetrics,
        RoutingNodes routingNodes
    ) {
        this.initialNodeHeapMetrics = initialNodeHeapMetrics;
        this.estimatedShardHeapUsages = estimatedShardHeapUsages;
        this.defaultShardHeapUsageForShardsWithoutMetrics = defaultShardHeapUsageForShardsWithoutMetrics;
        this.simulatedShardIdsByNode = new HashMap<>();
        this.nonShardHeapUsageByNode = new HashMap<>();
        initializeSimulatedPlacement(routingNodes);
    }

    void simulateShardStarted(ShardRouting shard) {
        addShardToNode(shard.currentNodeId(), shard.shardId());
        if (shard.relocatingNodeId() != null) {
            removeShardFromNode(shard.relocatingNodeId(), shard.shardId());
        }
    }

    /// Compute the non-shard heap usage for each node based on the initial placement.
    /// @param routingNodes
    private void initializeSimulatedPlacement(RoutingNodes routingNodes) {
        initialNodeHeapMetrics.keySet().forEach(nodeId -> simulatedShardIdsByNode.put(nodeId, new HashSet<>()));

        // populate active shard IDs for each node based on the initial placement
        for (var routingNode : routingNodes) {
            final var shardIds = simulatedShardIdsByNode.get(routingNode.nodeId());
            // routingNode may be a node that is not in the initialNodeHeapMetrics,
            // e.g. a node that has been added to the cluster since the last ClusterInfo was computed.
            if (shardIds == null) {
                continue;
            }
            for (var shardRouting : routingNode) {
                if (shardRouting.active()) {
                    shardIds.add(shardRouting.shardId());
                }
            }
        }

        // compute the non-shard heap usage for each node based on the initial placement
        initialNodeHeapMetrics.forEach((nodeId, nodeHeapMetrics) -> {
            final long initialPlacementHeapUsage = computeHeapUsageForPlacement(simulatedShardIdsByNode.getOrDefault(nodeId, Set.of()))
                .totalHeapUsage();
            nonShardHeapUsageByNode.put(
                nodeId,
                Math.max(0, nodeHeapMetrics.nodeHeapEstimates().totalHeapUsage() - initialPlacementHeapUsage)
            );
        });
    }

    private void addShardToNode(String nodeId, ShardId shardId) {
        final var shardIds = simulatedShardIdsByNode.get(nodeId);
        // If we have no initial heap metrics for this node, we don't track shard additions
        if (shardIds != null && shardIds.add(shardId)) {
            shardPlacementModified = true;
        }
    }

    private void removeShardFromNode(String nodeId, ShardId shardId) {
        final var shardIds = simulatedShardIdsByNode.get(nodeId);
        // If we have no initial heap metrics for this node, we don't track shard removals
        if (shardIds != null && shardIds.remove(shardId)) {
            shardPlacementModified = true;
        }
    }

    Map<String, NodeHeapMetrics> getSimulatedHeapMetrics() {
        if (shardPlacementModified == false) {
            return initialNodeHeapMetrics;
        }
        final Map<String, NodeHeapMetrics> simulatedNodeHeapMetrics = new HashMap<>(initialNodeHeapMetrics.size());
        for (var nodeId : initialNodeHeapMetrics.keySet()) {
            final NodeHeapMetrics initialMetrics = initialNodeHeapMetrics.get(nodeId);
            final var placementHeapUsage = computeHeapUsageForPlacement(simulatedShardIdsByNode.getOrDefault(nodeId, Set.of()));
            simulatedNodeHeapMetrics.put(
                nodeId,
                new NodeHeapMetrics(
                    initialMetrics.nodeId(),
                    initialMetrics.totalBytes(),
                    new NodeHeapEstimates(
                        Math.addExact(nonShardHeapUsageByNode.get(nodeId), placementHeapUsage.totalHeapUsage()),
                        placementHeapUsage.hostedShardsHeapUsage()
                    )
                )
            );
        }
        return Collections.unmodifiableMap(simulatedNodeHeapMetrics);
    }

    private NodeHeapEstimates computeHeapUsageForPlacement(Set<ShardId> shardIds) {
        long hostedShardsHeapUsage = 0L;
        long indexHeapUsage = 0L;
        final Set<String> seenIndices = new HashSet<>();
        for (var shardId : shardIds) {
            final var shardAndIndexHeapUsage = getShardAndIndexHeapUsage(shardId);
            hostedShardsHeapUsage = Math.addExact(hostedShardsHeapUsage, shardAndIndexHeapUsage.shardHeapUsageBytes());
            if (seenIndices.add(shardId.getIndexName())) {
                indexHeapUsage = Math.addExact(indexHeapUsage, shardAndIndexHeapUsage.indexHeapUsageBytes());
            }
        }
        return new NodeHeapEstimates(Math.addExact(hostedShardsHeapUsage, indexHeapUsage), hostedShardsHeapUsage);
    }

    private ShardAndIndexHeapUsage getShardAndIndexHeapUsage(ShardId shardId) {
        return estimatedShardHeapUsages.getOrDefault(shardId, defaultShardHeapUsageForShardsWithoutMetrics);
    }
}
