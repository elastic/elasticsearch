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
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Estimates how much heap each node is using from the shards currently placed on it,
 * plus a fixed overhead that stays on the node when those shards move.
 */
public final class NodeHeapUsageCalculator {

    private NodeHeapUsageCalculator() {}

    /**
     * Derives each node's heap estimate from the routing table on {@code clusterState}.
     *
     * @see #calculateForRoutingNodes(RoutingNodes, long, ShardHeapUsageEstimates)
     */
    public static RoutingNodeHeapCalculation calculateForRoutingNodes(
        ClusterState clusterState,
        long nonShardHeapUsage,
        ShardHeapUsageEstimates shardHeapUsageEstimates
    ) {
        return calculateForRoutingNodes(clusterState.getRoutingNodes(), nonShardHeapUsage, shardHeapUsageEstimates);
    }

    /**
     * Derives each node's heap estimate from the routing table.
     * <p>
     * A node's heap usage is the sum of the heap used by the shards the routing table currently places on that node, plus a fixed
     * per-node overhead that stays put when shards move.
     * <p>
     * For every indexing and search node, the active shards on that node are summed. Index overhead is counted once per index on the node,
     * because every shard of an index shares the same mapping infrastructure.
     * {@code nonShardHeapUsage} is the overhead that does not follow shard moves, and it is added only on indexing nodes.
     * <p>
     * Indexing node heap totals all use the largest postings sum found on any indexing node, so each of those nodes is charged at least as
     * much as the heaviest one. Search node postings are left out of that maximum since search nodes hold replicas of the same shards
     * and would otherwise raise every indexing node's total. Search nodes themselves publish hosted-shards heap only, with their total
     * and non-shard values intentionally left unmodelled as {@code 0}. Shards that are still initializing are skipped. Callers that
     * simulate a recovery completing, such as desired-balance computation, start the shard on {@code routingNodes} before reading the
     * result.
     *
     * @param routingNodes             routing table that says which shards sit on which nodes. This may be the mutable copy used while
     *                                 simulating allocation, which is not the routing table stored on the cluster state
     * @param nonShardHeapUsage        per-node overhead that does not move with shard placement, applicable to indexing nodes only
     * @param shardHeapUsageEstimates  shard and index heap for each shard, plus the default for a shard with no metrics yet
     * @return node estimates keyed by node id
     */
    public static RoutingNodeHeapCalculation calculateForRoutingNodes(
        RoutingNodes routingNodes,
        long nonShardHeapUsage,
        ShardHeapUsageEstimates shardHeapUsageEstimates
    ) {
        assert nonShardHeapUsage >= 0;
        final Map<DiscoveryNode, NodeHeapUsageComponents> nodeHeapUsageComponentsByNode = Maps.newHashMapWithExpectedSize(
            routingNodes.size()
        );
        long maxIndexingNodePostingsHeapUsage = 0L;
        for (RoutingNode routingNode : routingNodes) {
            final DiscoveryNode discoveryNode = routingNode.node();
            if (isIndexingNode(discoveryNode) == false && isSearchNode(discoveryNode) == false) {
                continue;
            }
            final NodeHeapUsageComponents nodeHeapUsageComponents = componentsForActiveShards(routingNode, shardHeapUsageEstimates);
            nodeHeapUsageComponentsByNode.put(discoveryNode, nodeHeapUsageComponents);
            if (isIndexingNode(discoveryNode)) {
                maxIndexingNodePostingsHeapUsage = Math.max(maxIndexingNodePostingsHeapUsage, nodeHeapUsageComponents.postingsHeapUsage);
            }
        }

        final long postingsHeapUsageForTotal = maxIndexingNodePostingsHeapUsage;
        final Map<String, NodeHeapEstimates> nodeHeapEstimates = nodeHeapUsageComponentsByNode.entrySet()
            .stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    entry -> entry.getKey().getId(),
                    entry -> nodeHeapEstimate(entry.getKey(), entry.getValue(), nonShardHeapUsage, postingsHeapUsageForTotal)
                )
            );

        return new RoutingNodeHeapCalculation(nodeHeapEstimates, postingsHeapUsageForTotal);
    }

    /**
     * Calculates heap usage for one indexing node
     * <p>
     * Used by local callers, such as the recovery gate, whose source of truth is the shard metrics collected on the node. Every shard in
     * that snapshot is included, regardless of routing state, and the total uses this node's own postings because there is no cluster-wide
     * maximum for a one-node calculation.
     */
    public static NodeHeapEstimates calculateForShardEstimates(long nonShardHeapUsage, ShardHeapUsageEstimates shardHeapUsageEstimates) {
        assert nonShardHeapUsage >= 0;
        final NodeHeapUsageComponents nodeHeapUsageComponents = componentsForShards(shardHeapUsageEstimates);
        final long hostedShardsHeapUsage = Math.addExact(
            nodeHeapUsageComponents.shardAndIndexHeapUsage,
            nodeHeapUsageComponents.postingsHeapUsage
        );
        return new NodeHeapEstimates(Math.addExact(nonShardHeapUsage, hostedShardsHeapUsage), hostedShardsHeapUsage, nonShardHeapUsage);
    }

    private static boolean isIndexingNode(DiscoveryNode discoveryNode) {
        return discoveryNode.getRoles().contains(DiscoveryNodeRole.INDEX_ROLE);
    }

    private static boolean isSearchNode(DiscoveryNode discoveryNode) {
        return discoveryNode.getRoles().contains(DiscoveryNodeRole.SEARCH_ROLE);
    }

    /**
     * Produces a {@link NodeHeapEstimates} object for a given node.
     * If this is an indexing node, shard and index heap, non-shard heap, and {@code postingsHeapUsageForTotal} are included.
     * If this is a search node, only shard heap is returned
     */
    private static NodeHeapEstimates nodeHeapEstimate(
        DiscoveryNode discoveryNode,
        NodeHeapUsageComponents nodeHeapUsageComponents,
        long nonShardHeapUsage,
        long postingsHeapUsageForTotal
    ) {
        final boolean indexingNode = isIndexingNode(discoveryNode);
        final long hostedShardsHeapUsage = Math.addExact(
            nodeHeapUsageComponents.shardAndIndexHeapUsage,
            nodeHeapUsageComponents.postingsHeapUsage
        );
        if (indexingNode == false) {
            return new NodeHeapEstimates(0L, hostedShardsHeapUsage, 0L);
        }
        final long totalHeapUsage = Math.addExact(
            Math.addExact(nonShardHeapUsage, nodeHeapUsageComponents.shardAndIndexHeapUsage),
            postingsHeapUsageForTotal
        );
        return new NodeHeapEstimates(totalHeapUsage, hostedShardsHeapUsage, nonShardHeapUsage);
    }

    /** Shard and index heap for the active shards on one routing node. Index overhead is counted once per index. */
    private static NodeHeapUsageComponents componentsForActiveShards(
        RoutingNode routingNode,
        ShardHeapUsageEstimates shardHeapUsageEstimates
    ) {
        final NodeHeapUsageComponentsAccumulator accumulator = new NodeHeapUsageComponentsAccumulator(shardHeapUsageEstimates);
        for (var shardRouting : routingNode) {
            // Only active shards contribute. A relocating shard is still active on its source until the caller starts it on the target.
            if (shardRouting.active() == false) {
                continue;
            }
            accumulator.add(shardRouting.shardId());
        }
        return accumulator.result();
    }

    /** Shard and index heap for every shard in the estimates, including ones that are still initializing. */
    private static NodeHeapUsageComponents componentsForShards(ShardHeapUsageEstimates shardHeapUsageEstimates) {
        final NodeHeapUsageComponentsAccumulator accumulator = new NodeHeapUsageComponentsAccumulator(shardHeapUsageEstimates);
        shardHeapUsageEstimates.perShard().keySet().forEach(accumulator::add);
        return accumulator.result();
    }

    private record NodeHeapUsageComponents(long shardAndIndexHeapUsage, long postingsHeapUsage) {}

    private static class NodeHeapUsageComponentsAccumulator {
        private final ShardHeapUsageEstimates shardHeapUsageEstimates;
        private final Set<Index> seenIndices = new HashSet<>();
        private long shardHeapUsageExcludingPostings;
        private long indexHeapUsage;
        private long postingsHeapUsage;

        private NodeHeapUsageComponentsAccumulator(ShardHeapUsageEstimates shardHeapUsageEstimates) {
            this.shardHeapUsageEstimates = shardHeapUsageEstimates;
        }

        private void add(ShardId shardId) {
            final ShardAndIndexHeapUsage shardAndIndexHeapUsage = shardHeapUsageEstimates.getOrDefault(shardId);
            shardHeapUsageExcludingPostings = Math.addExact(
                shardHeapUsageExcludingPostings,
                shardAndIndexHeapUsage.shardHeapUsageBytesExcludingPostings()
            );
            postingsHeapUsage = Math.addExact(postingsHeapUsage, shardAndIndexHeapUsage.postingsHeapUsageBytes());
            if (seenIndices.add(shardId.getIndex())) {
                indexHeapUsage = Math.addExact(indexHeapUsage, shardAndIndexHeapUsage.indexHeapUsageBytes());
            }
        }

        private NodeHeapUsageComponents result() {
            return new NodeHeapUsageComponents(Math.addExact(shardHeapUsageExcludingPostings, indexHeapUsage), postingsHeapUsage);
        }
    }

    /**
     * Estimated node heap usages, plus the max index-node postings heap included in those nodes' totals.
     */
    public record RoutingNodeHeapCalculation(Map<String, NodeHeapEstimates> nodeHeapEstimates, long maxPostingsHeapUsage) {
        public RoutingNodeHeapCalculation {
            assert maxPostingsHeapUsage >= 0;
        }
    }
}
