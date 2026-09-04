/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.index.shard.ShardId;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Calculates node heap estimates from routing nodes, shard heap inputs, and explicit non-shard heap estimates.
 */
public final class NodeHeapUsageCalculator {

    private NodeHeapUsageCalculator() {}

    /**
     * Calculates heap usage for each selected node from the active shards in {@code clusterState}.
     * <p>
     * The stateless service reports shard-level heap inputs independent of the current routing. This method joins those inputs with the
     * current routing view, counts index-level heap once per index per node, and applies the largest node-local postings value to every
     * node's total to preserve the existing conservative total-heap behavior. The node predicate lets callers select the node roles they
     * publish estimates for without deriving a separate node-id set from the same cluster state.
     */
    public static NodeHeapEstimatesAndMaxPostingsHeapUsage calculateForRoutingNodes(
        ClusterState clusterState,
        Predicate<DiscoveryNode> shouldCalculateForNode,
        long nonShardHeapUsage,
        ShardHeapUsageEstimates shardHeapUsageEstimates
    ) {
        return calculateForShardAllocationMap(
            activeShardIdsByNode(clusterState, shouldCalculateForNode),
            nonShardHeapUsage,
            shardHeapUsageEstimates
        );
    }

    private static Map<String, Set<ShardId>> activeShardIdsByNode(
        ClusterState clusterState,
        Predicate<DiscoveryNode> shouldCalculateForNode
    ) {
        final Map<String, Set<ShardId>> shardIdsByNode = new HashMap<>();
        for (var routingNode : clusterState.getRoutingNodes()) {
            final var discoveryNode = clusterState.nodes().get(routingNode.nodeId());
            assert discoveryNode != null : "routing nodes are from the cluster state so DiscoveryNodes should be consistent";
            if (shouldCalculateForNode.test(discoveryNode) == false) {
                continue;
            }
            final var shardIds = new HashSet<ShardId>();
            for (var shardRouting : routingNode) {
                if (shardRouting.active()) {
                    shardIds.add(shardRouting.shardId());
                }
            }
            shardIdsByNode.put(routingNode.nodeId(), shardIds);
        }
        return shardIdsByNode;
    }

    /**
     * Calculates heap usage for each supplied node from a shard allocation map.
     * <p>
     * Use this when the caller has already built the node-to-shards view, for example when simulating a future allocation instead of using
     * the current cluster state's routing. The same non-shard heap estimate is applied to every node.
     */
    public static NodeHeapEstimatesAndMaxPostingsHeapUsage calculateForShardAllocationMap(
        Map<String, Set<ShardId>> shardAllocationMap,
        long nonShardHeapUsage,
        ShardHeapUsageEstimates shardHeapUsageEstimates
    ) {
        final Map<String, NodeHeapUsageComponents> nodeHeapUsageComponentsByNode = new HashMap<>(shardAllocationMap.size());
        long maxPostingsHeapUsage = 0L;
        for (var entry : shardAllocationMap.entrySet()) {
            final var nodeHeapUsageComponents = computeNodeHeapUsageComponents(entry.getValue(), shardHeapUsageEstimates);
            nodeHeapUsageComponentsByNode.put(entry.getKey(), nodeHeapUsageComponents);
            maxPostingsHeapUsage = Math.max(maxPostingsHeapUsage, nodeHeapUsageComponents.postingsHeapUsage);
        }

        final Map<String, NodeHeapEstimates> nodeHeapEstimates = new HashMap<>(shardAllocationMap.size());
        for (var entry : nodeHeapUsageComponentsByNode.entrySet()) {
            final var nodeHeapUsageComponents = entry.getValue();
            nodeHeapEstimates.put(
                entry.getKey(),
                new NodeHeapEstimates(
                    Math.addExact(Math.addExact(nonShardHeapUsage, nodeHeapUsageComponents.shardAndIndexHeapUsage), maxPostingsHeapUsage),
                    Math.addExact(nodeHeapUsageComponents.shardHeapUsage, nodeHeapUsageComponents.postingsHeapUsage),
                    nonShardHeapUsage
                )
            );
        }
        return new NodeHeapEstimatesAndMaxPostingsHeapUsage(Collections.unmodifiableMap(nodeHeapEstimates), maxPostingsHeapUsage);
    }

    /**
     * Calculates heap usage for one node from its allocated shard IDs.
     */
    public static NodeHeapEstimates calculateForSingleNode(
        Set<ShardId> shardIds,
        long nonShardHeapUsage,
        ShardHeapUsageEstimates shardHeapUsageEstimates
    ) {
        final var nodeHeapUsageComponents = computeNodeHeapUsageComponents(shardIds, shardHeapUsageEstimates);
        return new NodeHeapEstimates(
            Math.addExact(
                Math.addExact(nonShardHeapUsage, nodeHeapUsageComponents.shardAndIndexHeapUsage),
                nodeHeapUsageComponents.postingsHeapUsage
            ),
            Math.addExact(nodeHeapUsageComponents.shardHeapUsage, nodeHeapUsageComponents.postingsHeapUsage),
            nonShardHeapUsage
        );
    }

    /**
     * Computes the heap usage components for a node given its shard IDs and individual shard heap usage estimates.
     * <p>
     * Shard heap is counted per shard, index heap is counted once per index hosted on the node, and postings heap is accumulated
     * separately so node totals can use the maximum node-local postings value.
     */
    private static NodeHeapUsageComponents computeNodeHeapUsageComponents(
        Set<ShardId> shardIds,
        ShardHeapUsageEstimates shardHeapUsageEstimates
    ) {
        long shardHeapUsage = 0L;
        long indexHeapUsage = 0L;
        long postingsHeapUsage = 0L;
        final Set<String> seenIndices = new HashSet<>();
        for (var shardId : shardIds) {
            final var shardAndIndexHeapUsage = shardHeapUsageEstimates.perShard()
                .getOrDefault(shardId, shardHeapUsageEstimates.defaultForShardsWithoutMetrics());
            shardHeapUsage = Math.addExact(shardHeapUsage, shardAndIndexHeapUsage.shardHeapUsageBytes());
            postingsHeapUsage = Math.addExact(postingsHeapUsage, shardAndIndexHeapUsage.shardPostingsHeapUsageBytes());
            if (seenIndices.add(shardId.getIndexName())) {
                indexHeapUsage = Math.addExact(indexHeapUsage, shardAndIndexHeapUsage.indexHeapUsageBytes());
            }
        }
        return new NodeHeapUsageComponents(Math.addExact(shardHeapUsage, indexHeapUsage), shardHeapUsage, postingsHeapUsage);
    }

    private record NodeHeapUsageComponents(long shardAndIndexHeapUsage, long shardHeapUsage, long postingsHeapUsage) {}

    /**
     * The estimated node heap usages and the max hosted postings heap usage included in every node total.
     */
    public record NodeHeapEstimatesAndMaxPostingsHeapUsage(Map<String, NodeHeapEstimates> nodeHeapEstimates, long maxPostingsHeapUsage) {
        public NodeHeapEstimatesAndMaxPostingsHeapUsage {
            nodeHeapEstimates = Map.copyOf(nodeHeapEstimates);
            assert maxPostingsHeapUsage >= 0;
        }
    }
}
