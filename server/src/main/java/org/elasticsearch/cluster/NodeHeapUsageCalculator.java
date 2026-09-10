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
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RoutingNode;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Calculates node heap estimates from routing nodes, shard heap inputs, and explicit non-shard heap estimates.
 */
public final class NodeHeapUsageCalculator {

    private NodeHeapUsageCalculator() {}

    /**
     * Calculates heap usage for each stateless index or search routing node from the active shards in {@code clusterState}.
     * <p>
     * The stateless service reports shard-level heap inputs independent of the current routing. This method joins those inputs with the
     * current routing table, counts index-level heap once per index per node, includes node-local postings in hosted-shards usage, and
     * applies the largest node-local postings value to every index node's total to preserve the existing conservative total-heap behavior.
     * Search nodes receive hosted-shards and non-shard components, but their total heap is intentionally left unmodeled as {@code 0}.
     */
    public static NodeHeapEstimatesAndMaxPostingsHeapUsage calculateForRoutingNodes(
        ClusterState clusterState,
        long nonShardHeapUsage,
        ShardHeapUsageEstimates shardHeapUsageEstimates
    ) {
        final Map<DiscoveryNode, NodeHeapUsageComponents> nodeHeapUsageComponentsByNode = new HashMap<>(
            clusterState.getRoutingNodes().size()
        );
        long maxPostingsHeapUsage = 0L;
        for (var routingNode : clusterState.getRoutingNodes()) {
            final var discoveryNode = routingNode.node();
            if (isIndexingNode(discoveryNode) == false && isSearchNode(discoveryNode) == false) {
                continue;
            }
            final var nodeHeapUsageComponents = computeNodeHeapUsageComponents(routingNode, shardHeapUsageEstimates);
            nodeHeapUsageComponentsByNode.put(discoveryNode, nodeHeapUsageComponents);
            if (isIndexingNode(discoveryNode)) {
                maxPostingsHeapUsage = Math.max(maxPostingsHeapUsage, nodeHeapUsageComponents.postingsHeapUsage);
            }
        }

        long finalMaxPostingsHeapUsage = maxPostingsHeapUsage;
        final Map<String, NodeHeapEstimates> nodeHeapEstimates = nodeHeapUsageComponentsByNode.entrySet()
            .stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    entry -> entry.getKey().getId(),
                    entry -> nodeHeapEstimate(entry.getKey(), entry.getValue(), nonShardHeapUsage, finalMaxPostingsHeapUsage)
                )
            );

        return new NodeHeapEstimatesAndMaxPostingsHeapUsage(nodeHeapEstimates, finalMaxPostingsHeapUsage);
    }

    /**
     * Calculates heap usage for a single routing node.
     * <p>
     * This is used by local callers that need the same per-node component math as {@link #calculateForRoutingNodes} without computing a
     * cluster-wide max postings value. Indexing nodes include their local postings in total heap; search nodes leave total heap unmodeled.
     */
    public static NodeHeapEstimates calculateForRoutingNode(
        RoutingNode routingNode,
        long nonShardHeapUsage,
        ShardHeapUsageEstimates shardHeapUsageEstimates
    ) {
        final var discoveryNode = routingNode.node();
        final var nodeHeapUsageComponents = computeNodeHeapUsageComponents(routingNode, shardHeapUsageEstimates);
        return nodeHeapEstimate(discoveryNode, nodeHeapUsageComponents, nonShardHeapUsage, nodeHeapUsageComponents.postingsHeapUsage);
    }

    private static boolean isIndexingNode(DiscoveryNode discoveryNode) {
        return discoveryNode.getRoles().contains(DiscoveryNodeRole.INDEX_ROLE);
    }

    private static boolean isSearchNode(DiscoveryNode discoveryNode) {
        return discoveryNode.getRoles().contains(DiscoveryNodeRole.SEARCH_ROLE);
    }

    private static NodeHeapEstimates nodeHeapEstimate(
        DiscoveryNode discoveryNode,
        NodeHeapUsageComponents nodeHeapUsageComponents,
        long nonShardHeapUsage,
        long postingsHeapUsageForTotal
    ) {
        return new NodeHeapEstimates(
            isIndexingNode(discoveryNode)
                ? Math.addExact(Math.addExact(nonShardHeapUsage, nodeHeapUsageComponents.shardAndIndexHeapUsage), postingsHeapUsageForTotal)
                : 0L,
            Math.addExact(nodeHeapUsageComponents.shardAndIndexHeapUsage, nodeHeapUsageComponents.postingsHeapUsage),
            nonShardHeapUsage
        );
    }

    private static NodeHeapUsageComponents computeNodeHeapUsageComponents(
        RoutingNode routingNode,
        ShardHeapUsageEstimates shardHeapUsageEstimates
    ) {
        long shardHeapUsage = 0L;
        long indexHeapUsage = 0L;
        long postingsHeapUsage = 0L;
        final Set<String> seenIndices = new HashSet<>();
        for (var shardRouting : routingNode) {
            if (shardRouting.active() == false) {
                continue;
            }
            final var shardId = shardRouting.shardId();
            final var shardAndIndexHeapUsage = shardHeapUsageEstimates.perShard()
                .getOrDefault(shardId, shardHeapUsageEstimates.defaultForShardsWithoutMetrics());
            shardHeapUsage = Math.addExact(shardHeapUsage, shardAndIndexHeapUsage.shardHeapUsageBytes());
            postingsHeapUsage = Math.addExact(postingsHeapUsage, shardAndIndexHeapUsage.shardPostingsHeapUsageBytes());
            if (seenIndices.add(shardId.getIndexName())) {
                indexHeapUsage = Math.addExact(indexHeapUsage, shardAndIndexHeapUsage.indexHeapUsageBytes());
            }
        }
        return new NodeHeapUsageComponents(Math.addExact(shardHeapUsage, indexHeapUsage), postingsHeapUsage);
    }

    private record NodeHeapUsageComponents(long shardAndIndexHeapUsage, long postingsHeapUsage) {}

    /**
     * The estimated node heap usages and the max hosted postings heap usage included in index-node totals.
     */
    public record NodeHeapEstimatesAndMaxPostingsHeapUsage(Map<String, NodeHeapEstimates> nodeHeapEstimates, long maxPostingsHeapUsage) {
        public NodeHeapEstimatesAndMaxPostingsHeapUsage {
            nodeHeapEstimates = Map.copyOf(nodeHeapEstimates);
            assert maxPostingsHeapUsage >= 0;
        }
    }
}
