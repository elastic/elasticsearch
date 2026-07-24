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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardMovementWriteLoadSimulator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.util.CopyOnFirstWriteMap;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.ClusterInfo.shardIdentifierFromRouting;
import static org.elasticsearch.cluster.routing.ExpectedShardSizeEstimator.getExpectedShardSize;
import static org.elasticsearch.cluster.routing.ExpectedShardSizeEstimator.shouldReserveSpaceForInitializingShard;
import static org.elasticsearch.cluster.routing.ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE;
import static org.elasticsearch.cluster.routing.UnassignedInfo.Reason.REINITIALIZED;

public class ClusterInfoSimulator {

    private static final Logger logger = LogManager.getLogger(ClusterInfoSimulator.class);

    private final RoutingAllocation allocation;

    private final Map<String, DiskUsage> leastAvailableSpaceUsage;
    private final Map<String, DiskUsage> mostAvailableSpaceUsage;
    private final CopyOnFirstWriteMap<String, Long> shardSizes;
    private final Map<ShardId, ShardAndIndexHeapUsage> estimatedShardHeapUsages;
    private final ShardAndIndexHeapUsage defaultShardHeapUsageForShardsWithoutMetrics;
    private final ShardMovementWriteLoadSimulator shardMovementWriteLoadSimulator;
    private final ShardMoveNodeCacheCommitmentSimulator shardMoveNodeCacheCommitmentSimulator;
    private final NodeHeapMemoryShardMovementSimulator nodeHeapMemoryShardMovementSimulator;

    public ClusterInfoSimulator(RoutingAllocation allocation) {
        this.allocation = allocation;
        this.leastAvailableSpaceUsage = getAdjustedDiskSpace(allocation, allocation.clusterInfo().getNodeLeastAvailableDiskUsages());
        this.mostAvailableSpaceUsage = getAdjustedDiskSpace(allocation, allocation.clusterInfo().getNodeMostAvailableDiskUsages());
        this.shardSizes = new CopyOnFirstWriteMap<>(allocation.clusterInfo().shardSizes);
        this.estimatedShardHeapUsages = allocation.clusterInfo().getEstimatedShardHeapUsages();
        this.defaultShardHeapUsageForShardsWithoutMetrics = allocation.clusterInfo().getDefaultShardHeapUsageForShardsWithoutMetrics();
        this.shardMovementWriteLoadSimulator = new ShardMovementWriteLoadSimulator(allocation);
        this.shardMoveNodeCacheCommitmentSimulator = new ShardMoveNodeCacheCommitmentSimulator(allocation.clusterInfo());
        this.nodeHeapMemoryShardMovementSimulator = new NodeHeapMemoryShardMovementSimulator(allocation.clusterInfo().getNodeHeapMetrics());
    }

    /**
     * Cluster info contains a reserved space that is necessary to finish initializing shards (that are currently in progress).
     * for all initializing shards sum(expected size) = reserved space + already used space
     * This deducts already used space from disk usage as when shard start is simulated it is going to add entire expected shard size.
     */
    private static Map<String, DiskUsage> getAdjustedDiskSpace(RoutingAllocation allocation, Map<String, DiskUsage> diskUsage) {
        var diskUsageCopy = new HashMap<>(diskUsage);
        for (var entry : diskUsageCopy.entrySet()) {
            var nodeId = entry.getKey();
            var usage = entry.getValue();

            var reserved = allocation.clusterInfo().getReservedSpace(nodeId, usage.path());
            if (reserved.total() == 0) {
                continue;
            }
            var node = allocation.routingNodes().node(nodeId);
            if (node == null) {
                continue;
            }

            long adjustment = 0;
            for (ShardId shardId : reserved.shardIds()) {
                var shard = node.getByShardId(shardId);
                if (shard != null) {
                    var expectedSize = getExpectedShardSize(shard, 0, allocation);
                    adjustment += expectedSize;
                }
            }
            adjustment -= reserved.total();

            entry.setValue(updateWithFreeBytes(usage, adjustment));
        }
        return diskUsageCopy;
    }

    public void simulateShardStarted(ShardRouting shard) {
        simulateShardStarted(shard, true);
    }

    /**
     * This method updates disk usage to reflect shard relocations and new replica initialization.
     * In case of a single data path both mostAvailableSpaceUsage and leastAvailableSpaceUsage are update to reflect the change.
     * In case of multiple data path only mostAvailableSpaceUsage as it is used in calculation in
     * {@link org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider} for allocating new shards.
     * This assumes the worst case (all shards are placed on a single most used disk) and prevents node overflow.
     * Balance is later recalculated with a refreshed cluster info containing actual shards placement.
     *
     * A relocating shard will have the current node ID set for the new node, and the relocating ID set for the previous node.
     * A new shard will have the current ID set for the new node, and relocating ID will be null.
     */
    public void simulateShardStarted(ShardRouting shard, boolean includeIndexUsage) {
        assert shard.initializing() : "expected an initializing shard, but got: " + shard;

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
            if (shard.relocatingNodeId() != null) {
                // relocation
                modifyDiskUsage(shard.relocatingNodeId(), size);
                modifyDiskUsage(shard.currentNodeId(), -size);
            } else {
                // new shard
                if (shouldReserveSpaceForInitializingShard(shard, allocation.metadata())) {
                    modifyDiskUsage(shard.currentNodeId(), -size);
                }
                shardSizes.put(shardIdentifierFromRouting(shard), project.getIndexSafe(shard.index()).ignoreDiskWatermarks() ? 0 : size);
            }
        }

        nodeHeapMemoryShardMovementSimulator.simulateShardStarted(shard, includeIndexUsage);
        shardMoveNodeCacheCommitmentSimulator.simulateShardStarted(shard);
        shardMovementWriteLoadSimulator.simulateShardStarted(shard);
    }

    public void simulateAddIndexToNode(String nodeId, Index index) {
        nodeHeapMemoryShardMovementSimulator.simulateAddIndexToNode(nodeId, index);
    }

    public void simulateRemoveIndexFromNode(String nodeId, Index index) {
        nodeHeapMemoryShardMovementSimulator.simulateRemoveIndexFromNode(nodeId, index);
    }

    // Visible for testing
    public Map<String, NodeHeapMetrics> computeNodeHeapMetrics() {
        return nodeHeapMemoryShardMovementSimulator.getSimulatedHeapMetrics();
    }

    /**
     * This method simulates starting an already started shard with an optional {@code sourceNodeId} in case of a relocation.
     * @param startedShard The shard to simulate. Must be started already.
     * @param sourceNodeId The source node ID if the shard started as a result of relocation. {@code null} otherwise.
     */
    public void simulateAlreadyStartedShard(ShardRouting startedShard, @Nullable String sourceNodeId) {
        assert startedShard.started() || startedShard.relocating() : "expected an already started shard, but got: " + startedShard;
        if (logger.isDebugEnabled()) {
            logger.debug(
                "simulated started shard {} on node [{}] as a {}",
                startedShard.shardId(),
                startedShard.currentNodeId(),
                sourceNodeId != null ? "relocating shard from node [" + sourceNodeId + "]" : "new shard"
            );
        }
        final long expectedShardSize = startedShard.getExpectedShardSize();
        if (sourceNodeId != null) {
            final var relocatingShard = startedShard.moveToUnassigned(new UnassignedInfo(REINITIALIZED, "simulation"))
                .initialize(sourceNodeId, null, expectedShardSize)
                .moveToStarted(expectedShardSize)
                .relocate(startedShard.currentNodeId(), expectedShardSize)
                .getTargetRelocatingShard();
            simulateShardStarted(relocatingShard, false);
        } else {
            final var initializingShard = startedShard.moveToUnassigned(new UnassignedInfo(REINITIALIZED, "simulation"))
                .initialize(startedShard.currentNodeId(), null, expectedShardSize);
            simulateShardStarted(initializingShard, false);
        }
    }

    private void modifyDiskUsage(String nodeId, long freeDelta) {
        if (freeDelta == 0) {
            return;
        }
        var diskUsage = mostAvailableSpaceUsage.get(nodeId);
        if (diskUsage == null) {
            return;
        }
        var path = diskUsage.path();
        updateDiskUsage(leastAvailableSpaceUsage, nodeId, path, freeDelta);
        updateDiskUsage(mostAvailableSpaceUsage, nodeId, path, freeDelta);
    }

    private void updateDiskUsage(Map<String, DiskUsage> availableSpaceUsage, String nodeId, String path, long freeDelta) {
        var usage = availableSpaceUsage.get(nodeId);
        if (usage != null && Objects.equals(usage.path(), path)) {
            // ensure new value is within bounds
            availableSpaceUsage.put(nodeId, updateWithFreeBytes(usage, freeDelta));
        }
    }

    private static DiskUsage updateWithFreeBytes(DiskUsage usage, long delta) {
        // free bytes might go out of range in case when multiple data path are used
        // we might not know exact disk used to allocate a shard and conservatively update
        // most used disk on a target node and least used disk on a source node
        var freeBytes = withinRange(0, usage.totalBytes(), usage.freeBytes() + delta);
        return usage.copyWithFreeBytes(freeBytes);
    }

    private static long withinRange(long min, long max, long value) {
        return Math.max(min, Math.min(max, value));
    }

    public ClusterInfo getClusterInfo() {
        return allocation.clusterInfo()
            .updateWith(
                leastAvailableSpaceUsage,
                mostAvailableSpaceUsage,
                shardSizes.toImmutableMap(),
                Map.of(),
                nodeHeapMemoryShardMovementSimulator.getSimulatedHeapMetrics(),
                estimatedShardHeapUsages,
                shardMovementWriteLoadSimulator.simulatedNodeUsageStatsForThreadPools(),
                shardMoveNodeCacheCommitmentSimulator.getShardCacheRequirements(),
                shardMoveNodeCacheCommitmentSimulator.getSimulatedNodeCacheSizeAndCommitments()
            );
    }

    /**
     * This class accumulates the heap usage deltas that result from shard movements
     * then applies them to the initial heap estimates to produce a simulated result.
     * The deltas are accumulated then the estimates updated with clamping applied
     * so that we don't produce negative estimates. This prevents nonsense estimates
     * while minimizing the error introduced by the clamping.
     */
    private class NodeHeapMemoryShardMovementSimulator {
        private final ObjectLongMap<String> totalUsageDeltaByNode;
        private final ObjectLongMap<String> hostedShardUsageDeltaByNode;
        private final Map<String, NodeHeapMetrics> initialNodeHeapMetrics;

        NodeHeapMemoryShardMovementSimulator(Map<String, NodeHeapMetrics> initialNodeHeapMetrics) {
            this.initialNodeHeapMetrics = initialNodeHeapMetrics;
            totalUsageDeltaByNode = new ObjectLongHashMap<>();
            hostedShardUsageDeltaByNode = new ObjectLongHashMap<>();
        }

        public void simulateShardStarted(ShardRouting shard, boolean includeIndexUsage) {
            // Started on, or relocate to, the current node assignment.
            modifyHeapUsage(allocation.routingNodes().node(shard.currentNodeId()), shard.shardId(), Modification.ADD, includeIndexUsage);

            if (shard.relocatingNodeId() != null) {
                // Shard relocation from another node, so remove the stats from the previous node.
                modifyHeapUsage(
                    allocation.routingNodes().node(shard.relocatingNodeId()),
                    shard.shardId(),
                    Modification.REMOVE,
                    includeIndexUsage
                );
            }
        }

        public void simulateAddIndexToNode(String nodeId, Index index) {
            // Don't simulate shard movement for nodes that we have no initial estimate for, we need the initial estimate to apply the
            // deltas
            if (initialNodeHeapMetrics.containsKey(nodeId) == false) {
                return;
            }
            // Use any shard ID since index stats are the same.
            var shardAndIndexHeap = estimatedShardHeapUsages.getOrDefault(
                new ShardId(index, 0),
                defaultShardHeapUsageForShardsWithoutMetrics
            );
            totalUsageDeltaByNode.addTo(nodeId, shardAndIndexHeap.indexHeapUsageBytes());
        }

        public void simulateRemoveIndexFromNode(String nodeId, Index index) {
            // Don't simulate shard movement for nodes that we have no initial estimate for, we need the initial estimate to apply the
            // deltas
            if (initialNodeHeapMetrics.containsKey(nodeId) == false) {
                return;
            }
            // Use any shard ID since index stats are the same.
            var shardAndIndexHeap = estimatedShardHeapUsages.getOrDefault(
                new ShardId(index, 0),
                defaultShardHeapUsageForShardsWithoutMetrics
            );
            totalUsageDeltaByNode.addTo(nodeId, -1 * shardAndIndexHeap.indexHeapUsageBytes());
        }

        private enum Modification {
            ADD,
            REMOVE
        }

        private void modifyHeapUsage(
            @Nullable RoutingNode routingNode,
            ShardId shardId,
            Modification modification,
            boolean includeIndexUsage
        ) {
            // Don't simulate shard movement for nodes that we have no initial estimate for, we need the initial estimate to apply the
            // deltas
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
                        // first
                        // shard for the index, and the index-level heap usage overhead must be added.
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
         * Apply the deltas to the initial estimates, clamping the results to 0 to avoid producing negative estimates
         */
        public Map<String, NodeHeapMetrics> getSimulatedHeapMetrics() {
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
}
