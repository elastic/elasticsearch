/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.util.CopyOnFirstWriteMap;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.shard.ShardId;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.cluster.ClusterInfo.shardIdentifierFromRouting;
import static org.elasticsearch.cluster.routing.ExpectedShardSizeEstimator.getExpectedShardSize;
import static org.elasticsearch.cluster.routing.ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE;

public class ClusterInfoSimulator {

    private static final Logger logger = LogManager.getLogger(ClusterInfoSimulator.class);

    private final RoutingAllocation allocation;

    private final ClusterInfo polled;
    private final CopyOnFirstWriteMap<String, Long> shardSizes;
    private final Map<String, NodeHeapMetrics> initialNodeHeapMetrics;
    private final PlacementAspects.UsedBytes usedBytes;
    private final PlacementAspects.CacheCommitment cacheCommitment;
    private final PlacementAspects.WriteLoad writeLoad;
    private final Map<ShardId, ShardAndIndexHeapUsage> estimatedShardHeapUsages;
    private final ShardAndIndexHeapUsage defaultShardHeapUsageForShardsWithoutMetrics;
    private final RoutingDerivedPlacementModel placementModel;

    public ClusterInfoSimulator(RoutingAllocation allocation) {
        this.allocation = allocation;
        this.polled = allocation.clusterInfo();
        this.shardSizes = new CopyOnFirstWriteMap<>(polled.shardSizes);
        this.initialNodeHeapMetrics = polled.getNodeHeapMetrics();
        this.usedBytes = new PlacementAspects.UsedBytes(
            allocation,
            (shardId, primary) -> shardSizes.get(shardIdentifierFromRouting(shardId, primary))
        );
        this.cacheCommitment = new PlacementAspects.CacheCommitment(polled);
        this.writeLoad = new PlacementAspects.WriteLoad(polled);
        this.estimatedShardHeapUsages = polled.getEstimatedShardHeapUsages();
        this.defaultShardHeapUsageForShardsWithoutMetrics = polled.getDefaultShardHeapUsageForShardsWithoutMetrics();
        this.placementModel = new RoutingDerivedPlacementModel(allocation, usedBytes, cacheCommitment, writeLoad);
    }

    /**
     * Records that the computation starts an initializing shard: a relocation target (its relocating node is the source) or a new copy.
     * The simulated disk usage, write load and cache commitments are all derived from the shards the routing table places on each node
     * (see {@link RoutingDerivedPlacementModel}), so every shard start must be reported before it is started in the routing table, but
     * shards that moved before the computation began need no special handling. Node heap is recomputed from the routing table when the
     * simulated cluster info is read.
     */
    public void simulateShardStarted(ShardRouting shard) {
        assert shard.initializing() : "expected an initializing shard, but got: " + shard;

        placementModel.shardStarted(shard);

        if (shard.relocatingNodeId() == null) {
            // new shard: its size is now known to the simulation
            var project = allocation.metadata().projectFor(shard.index());
            var size = getExpectedShardSize(
                shard,
                shard.getExpectedShardSize(),
                (shardId, primary) -> shardSizes.get(shardIdentifierFromRouting(shardId, primary)),
                allocation.snapshotShardSizeInfo(),
                project,
                allocation.routingTable(project.id())
            );
            if (size != UNAVAILABLE_EXPECTED_SHARD_SIZE) {
                shardSizes.put(shardIdentifierFromRouting(shard), project.getIndexSafe(shard.index()).ignoreDiskWatermarks() ? 0 : size);
            }
        }
    }

    /**
     * Node heap for the shards the current routing table places on each node
     */
    private Map<String, NodeHeapMetrics> computeNodeHeapMetrics() {
        if (initialNodeHeapMetrics.isEmpty()) {
            return initialNodeHeapMetrics;
        }
        final Map<String, NodeHeapEstimates> calculated = NodeHeapUsageCalculator.calculateForRoutingNodes(
            allocation.routingNodes(),
            nonShardHeapUsage(),
            new ShardHeapUsageEstimates(estimatedShardHeapUsages, defaultShardHeapUsageForShardsWithoutMetrics)
        ).nodeHeapEstimates();
        final Map<String, NodeHeapMetrics> updatedMetrics = Maps.newHashMapWithExpectedSize(initialNodeHeapMetrics.size());
        for (var entry : initialNodeHeapMetrics.entrySet()) {
            final NodeHeapEstimates recalculated = calculated.get(entry.getKey());
            if (recalculated == null) {
                updatedMetrics.put(entry.getKey(), entry.getValue());
            } else {
                final NodeHeapMetrics initial = entry.getValue();
                updatedMetrics.put(entry.getKey(), new NodeHeapMetrics(initial.nodeId(), initial.totalBytes(), recalculated));
            }
        }
        return Collections.unmodifiableMap(updatedMetrics);
    }

    /**
     * Returns the nonShardHeapUsage (a fixed overhead that does not move with shard placements). This value is 0 on all search nodes,
     * and the same on any indexing node.
     */
    private long nonShardHeapUsage() {
        long nonShardHeapUsage = 0L;
        for (NodeHeapMetrics metrics : initialNodeHeapMetrics.values()) {
            final var routingNode = allocation.routingNodes().node(metrics.nodeId());
            if (routingNode == null || routingNode.node() == null) {
                continue;
            }
            if (routingNode.node().getRoles().contains(DiscoveryNodeRole.INDEX_ROLE)) {
                nonShardHeapUsage = metrics.nodeHeapEstimates().nonShardHeapUsage();
                break;
            }
        }

        // Assertions only run in tests
        assert indexingNodesHaveIdenticalNonShardHeapUsage(nonShardHeapUsage);
        assert searchNodesHaveZeroNonShardHeapUsage();

        return nonShardHeapUsage;
    }

    /**
     * {@link #nonShardHeapUsage()} assumes every indexing node reports the same {@code nonShardHeapUsage},
     * and takes the first indexing node's value, so if that stops being true we need to decide which value to use.
     */
    private boolean indexingNodesHaveIdenticalNonShardHeapUsage(long nonShardHeapUsage) {
        for (NodeHeapMetrics metrics : initialNodeHeapMetrics.values()) {
            final var routingNode = allocation.routingNodes().node(metrics.nodeId());
            if (routingNode == null || routingNode.node() == null) {
                continue;
            }
            if (routingNode.node().getRoles().contains(DiscoveryNodeRole.INDEX_ROLE) == false) {
                continue;
            }
            final long nodeNonShardHeapUsage = metrics.nodeHeapEstimates().nonShardHeapUsage();
            assert nodeNonShardHeapUsage == nonShardHeapUsage
                : "indexing node ["
                    + metrics.nodeId()
                    + "] has nonShardHeapUsage ["
                    + nodeNonShardHeapUsage
                    + "] but another indexing node has ["
                    + nonShardHeapUsage
                    + "]";
        }
        return true;
    }

    /**
     * Search nodes should all return zero heap usage
     */
    private boolean searchNodesHaveZeroNonShardHeapUsage() {
        for (NodeHeapMetrics metrics : initialNodeHeapMetrics.values()) {
            final var routingNode = allocation.routingNodes().node(metrics.nodeId());
            if (routingNode == null || routingNode.node() == null) {
                continue;
            }
            if (routingNode.node().getRoles().contains(DiscoveryNodeRole.SEARCH_ROLE) == false) {
                continue;
            }
            final long nodeNonShardHeapUsage = metrics.nodeHeapEstimates().nonShardHeapUsage();
            assert nodeNonShardHeapUsage == 0L
                : "search node [" + metrics.nodeId() + "] has nonShardHeapUsage [" + nodeNonShardHeapUsage + "]";
        }
        return true;
    }

    public ClusterInfo getClusterInfo() {
        return allocation.clusterInfo()
            .updateWith(
                usedBytes.adjust(polled.getNodeLeastAvailableDiskUsages()),
                usedBytes.simulated(),
                shardSizes.toImmutableMap(),
                Map.of(),
                computeNodeHeapMetrics(),
                polled.getEstimatedShardHeapUsages(),
                writeLoad.simulated(),
                polled.getShardCacheRequirements(),
                cacheCommitment.simulated()
            );
    }

}
