/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import com.carrotsearch.hppc.ObjectDoubleHashMap;
import com.carrotsearch.hppc.ObjectDoubleMap;
import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.ObjectLongMap;

import org.elasticsearch.cluster.routing.ExpectedShardSizeEstimator;
import org.elasticsearch.cluster.routing.ShardMovementWriteLoadSimulator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.cluster.BoostedAndUnboostedCacheRequirements.NO_BOOSTED_OR_UNBOOSTED_CACHE_REQUIREMENT;
import static org.elasticsearch.cluster.ClusterInfo.shardIdentifierFromRouting;
import static org.elasticsearch.cluster.routing.ExpectedShardSizeEstimator.getExpectedShardSize;
import static org.elasticsearch.cluster.routing.ExpectedShardSizeEstimator.shouldReserveSpaceForInitializingShard;
import static org.elasticsearch.cluster.routing.ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE;

/** PROTOTYPE. The {@link PlacementAspect}s for the placement-dependent parts of {@link ClusterInfo} that are simulated today. */
public final class PlacementAspects {

    private PlacementAspects() {}

    /**
     * For a fact keyed by {@link ShardId} alone, so that a copy needs no resolution: a placement event is just
     * {@code apply(node, shardId, +-1)}.
     */
    private abstract static class ShardIdKeyedAspect<R> implements PlacementAspect<R> {

        abstract void apply(String nodeId, ShardId shardId, int sign);

        /** A shard that was on the node at poll time is no longer there, or one starts moving off it. */
        void shardDeparted(String nodeId) {}

        @Override
        public void shardPlaced(ShardRouting shard) {
            apply(shard.currentNodeId(), shard.shardId(), +1);
        }

        @Override
        public void shardPlacedAtPoll(String nodeId, ShardId shardId, @Nullable ShardRouting currentOnNode) {
            if (currentOnNode == null) {
                shardDeparted(nodeId);
            }
            apply(nodeId, shardId, -1);
        }

        @Override
        public void shardStarted(ShardRouting target) {
            if (target.relocatingNodeId() != null) {
                apply(target.relocatingNodeId(), target.shardId(), -1);
                shardDeparted(target.relocatingNodeId());
            }
            apply(target.currentNodeId(), target.shardId(), +1);
        }
    }

    /** Free disk space: {@code f} is the shard size, which {@link ClusterInfo} keys by copy ({@code shardId[p|r]}). */
    public static final class UsedBytes implements PlacementAspect<Map<String, DiskUsage>> {

        private final RoutingAllocation allocation;
        private final ClusterInfo polled;
        private final ExpectedShardSizeEstimator.ShardSizeProvider shardSizes;
        private final ObjectLongMap<String> usedBytesDelta = new ObjectLongHashMap<>();

        public UsedBytes(RoutingAllocation allocation) {
            this(allocation, allocation.clusterInfo());
        }

        /**
         * @param shardSizes the sizes used to size a shard when it starts, which a simulation may extend with the sizes of new shards
         */
        public UsedBytes(RoutingAllocation allocation, ExpectedShardSizeEstimator.ShardSizeProvider shardSizes) {
            this.allocation = allocation;
            this.polled = allocation.clusterInfo();
            this.shardSizes = shardSizes;
            // Space reserved for recoveries in progress at poll time: their partial bytes are already in "used". The poll-time sum and
            // the start of the shard both size it the same way, so they cancel, and the remaining bytes are the reserved space.
            for (var usage : polled.getNodeMostAvailableDiskUsages().values()) {
                usedBytesDelta.addTo(usage.nodeId(), polled.getReservedSpace(usage.nodeId(), usage.path()).total());
            }
        }

        @Override
        public void shardPlaced(ShardRouting shard) {
            usedBytesDelta.addTo(shard.currentNodeId(), size(shard.shardId(), shard.primary()));
        }

        @Override
        public void shardPlacedAtPoll(String nodeId, ShardId shardId, @Nullable ShardRouting currentOnNode) {
            if (currentOnNode != null && currentOnNode.initializing()) {
                // Sized exactly as it will be when it starts, so the two cancel and leave the reserved space (see the constructor). A
                // shard whose start adds nothing, because it is already on disk, has nothing to cancel.
                if (chargedWhenStarted(currentOnNode)) {
                    usedBytesDelta.addTo(nodeId, -Math.max(0, expectedSize(currentOnNode)));
                }
                return;
            }
            usedBytesDelta.addTo(nodeId, -size(shardId, pollTimeCopyIsPrimary(shardId, currentOnNode)));
        }

        private long expectedSize(ShardRouting shard) {
            final var project = allocation.metadata().projectFor(shard.index());
            return getExpectedShardSize(
                shard,
                shard.getExpectedShardSize(),
                shardSizes,
                allocation.snapshotShardSizeInfo(),
                project,
                allocation.routingTable(project.id())
            );
        }

        @Override
        public void shardStarted(ShardRouting target) {
            final long size = expectedSize(target);
            if (size == UNAVAILABLE_EXPECTED_SHARD_SIZE) {
                return;
            }
            if (target.relocatingNodeId() != null) {
                usedBytesDelta.addTo(target.relocatingNodeId(), -size);
            }
            if (chargedWhenStarted(target)) {
                usedBytesDelta.addTo(target.currentNodeId(), size);
            }
        }

        /** Whether starting the initializing shard adds its size to its node: a relocation target, or a recovery that downloads it. */
        private boolean chargedWhenStarted(ShardRouting initializing) {
            return initializing.relocatingNodeId() != null || shouldReserveSpaceForInitializingShard(initializing, allocation.metadata());
        }

        @Override
        public Map<String, DiskUsage> simulated() {
            return adjust(polled.getNodeMostAvailableDiskUsages());
        }

        /**
         * Applies the delta to a per-node disk usage map. With several data paths the most-available path is the one assumed to take
         * every simulated shard, as {@code ClusterInfoSimulator} always did, so other paths are left as polled.
         */
        public Map<String, DiskUsage> adjust(Map<String, DiskUsage> polledUsage) {
            Map<String, DiskUsage> adjusted = null;
            for (var entry : polledUsage.entrySet()) {
                final long delta = usedBytesDelta.get(entry.getKey());
                final DiskUsage usage = entry.getValue();
                final DiskUsage most = polled.getNodeMostAvailableDiskUsages().get(entry.getKey());
                if (delta == 0 || most == null || Objects.equals(most.path(), usage.path()) == false) {
                    continue;
                }
                if (adjusted == null) {
                    adjusted = new HashMap<>(polledUsage);
                }
                final long free = Math.max(0, Math.min(usage.totalBytes(), usage.freeBytes() - delta));
                adjusted.put(entry.getKey(), usage.copyWithFreeBytes(free));
            }
            return adjusted == null ? polledUsage : adjusted;
        }

        /**
         * Sized like {@link ExpectedShardSizeEstimator#getExpectedShardSize} sizes a shard that starts, a replica without a size of its
         * own taking its primary's, so that the same shard cancels between the poll-time sum and its start. Unknown sizes count as 0.
         */
        private long size(ShardId shardId, boolean primary) {
            Long size = polled.shardSizes.get(shardIdentifierFromRouting(shardId, primary));
            if (size == null && primary == false) {
                size = polled.shardSizes.get(shardIdentifierFromRouting(shardId, true));
            }
            return size == null ? 0L : size;
        }

        /**
         * ClusterInfo records which node held a shard but not which copy, only the sizes keyed by copy. This is the one ambiguity in
         * the model: for a copy that has left its node, take the copy kind from the sizes if only one exists, otherwise assume primary.
         */
        private boolean pollTimeCopyIsPrimary(ShardId shardId, @Nullable ShardRouting currentOnNode) {
            if (currentOnNode != null) {
                return currentOnNode.primary();
            }
            final boolean hasPrimarySize = polled.shardSizes.containsKey(shardIdentifierFromRouting(shardId, true));
            final boolean hasReplicaSize = polled.shardSizes.containsKey(shardIdentifierFromRouting(shardId, false));
            return hasReplicaSize == false || hasPrimarySize;
        }
    }

    /** Boosted and unboosted cache commitment: {@code f} is the shard's cache requirement. */
    public static final class CacheCommitment extends ShardIdKeyedAspect<Map<String, NodeCacheSizeAndCommitments>> {

        private final ClusterInfo polled;
        private final ObjectLongMap<String> boostedDelta = new ObjectLongHashMap<>();
        private final ObjectLongMap<String> unboostedDelta = new ObjectLongHashMap<>();

        public CacheCommitment(ClusterInfo polled) {
            this.polled = polled;
        }

        @Override
        void apply(String nodeId, ShardId shardId, int sign) {
            final var requirement = polled.getShardCacheRequirements().get(shardId);
            if (requirement == null) {
                return;
            }
            if (requirement.boostedCacheRequirementInBytes() != NO_BOOSTED_OR_UNBOOSTED_CACHE_REQUIREMENT) {
                boostedDelta.addTo(nodeId, sign * requirement.boostedCacheRequirementInBytes());
            }
            if (requirement.unboostedCacheRequirementInBytes() != NO_BOOSTED_OR_UNBOOSTED_CACHE_REQUIREMENT) {
                unboostedDelta.addTo(nodeId, sign * requirement.unboostedCacheRequirementInBytes());
            }
        }

        @Override
        public Map<String, NodeCacheSizeAndCommitments> simulated() {
            final Map<String, NodeCacheSizeAndCommitments> result = Maps.newMapWithExpectedSize(
                polled.getNodeCacheSizeAndCommitments().size()
            );
            polled.getNodeCacheSizeAndCommitments()
                .forEach(
                    (nodeId, initial) -> result.put(
                        nodeId,
                        new NodeCacheSizeAndCommitments(
                            initial.cacheSizeInBytes(),
                            Math.max(0, initial.boostedCacheCommitmentInBytes() + boostedDelta.get(nodeId)),
                            Math.max(0, initial.unboostedCacheCommitmentInBytes() + unboostedDelta.get(nodeId))
                        )
                    )
                );
            return Collections.unmodifiableMap(result);
        }
    }

    /**
     * Node heap. Unlike the other aspects it is derived from the routing table alone, so the poll placement plays no part and the
     * polled heap estimates are replaced, not adjusted. It is not a plain sum: an index's heap is counted once per node, and indexing
     * nodes are charged the cluster-wide maximum postings heap, so it keeps {@link NodeHeapUsageCalculator}'s incremental calculation
     * and seeds itself from the routing table instead of from {@link #shardPlaced} events.
     */
    public static final class NodeHeap implements PlacementAspect<Map<String, NodeHeapMetrics>> {

        private final ClusterInfo polled;
        @Nullable
        private final NodeHeapUsageCalculator.IncrementalRoutingNodeHeapCalculation calculation;

        /**
         * @param nonShardHeapUsage the fixed overhead on every indexing node, which does not move with shard placements
         */
        public NodeHeap(RoutingAllocation allocation, long nonShardHeapUsage) {
            this.polled = allocation.clusterInfo();
            this.calculation = polled.getNodeHeapMetrics().isEmpty()
                ? null
                : new NodeHeapUsageCalculator.IncrementalRoutingNodeHeapCalculation(
                    allocation.routingNodes(),
                    nonShardHeapUsage,
                    new ShardHeapUsageEstimates(
                        polled.getEstimatedShardHeapUsages(),
                        polled.getDefaultShardHeapUsageForShardsWithoutMetrics()
                    )
                );
        }

        @Override
        public void shardPlaced(ShardRouting shard) {
            // seeded from the routing table in the constructor
        }

        @Override
        public void shardPlacedAtPoll(String nodeId, ShardId shardId, @Nullable ShardRouting currentOnNode) {
            // heap is not derived from the polled placement
        }

        @Override
        public void shardStarted(ShardRouting target) {
            if (calculation == null) {
                return;
            }
            if (target.relocatingNodeId() != null) {
                calculation.shardDeactivated(target.relocatingNodeId(), target.shardId());
            }
            calculation.shardActivated(target.currentNodeId(), target.shardId());
        }

        @Override
        public Map<String, NodeHeapMetrics> simulated() {
            if (calculation == null) {
                return polled.getNodeHeapMetrics();
            }
            final Map<String, NodeHeapEstimates> calculated = calculation.calculation().nodeHeapEstimates();
            final Map<String, NodeHeapMetrics> result = Maps.newHashMapWithExpectedSize(polled.getNodeHeapMetrics().size());
            for (var entry : polled.getNodeHeapMetrics().entrySet()) {
                final NodeHeapEstimates recalculated = calculated.get(entry.getKey());
                final NodeHeapMetrics initial = entry.getValue();
                result.put(
                    entry.getKey(),
                    recalculated == null ? initial : new NodeHeapMetrics(initial.nodeId(), initial.totalBytes(), recalculated)
                );
            }
            return Collections.unmodifiableMap(result);
        }
    }

    /** Write thread pool utilisation and queue latency: {@code f} is the shard write load. */
    public static final class WriteLoad extends ShardIdKeyedAspect<Map<String, NodeUsageStatsForThreadPools>> {

        private final ClusterInfo polled;
        private final ObjectDoubleMap<String> writeLoadDelta = new ObjectDoubleHashMap<>();
        private final Set<String> nodesWithDeparture = new HashSet<>();

        public WriteLoad(ClusterInfo polled) {
            this.polled = polled;
        }

        @Override
        void apply(String nodeId, ShardId shardId, int sign) {
            final Double writeLoad = polled.getShardWriteLoads().get(shardId);
            if (writeLoad != null) {
                writeLoadDelta.addTo(nodeId, sign * writeLoad);
            }
        }

        @Override
        void shardDeparted(String nodeId) {
            nodesWithDeparture.add(nodeId);
        }

        @Override
        public Map<String, NodeUsageStatsForThreadPools> simulated() {
            final Map<String, NodeUsageStatsForThreadPools> result = Maps.newMapWithExpectedSize(
                polled.getNodeUsageStatsForThreadPools().size()
            );
            polled.getNodeUsageStatsForThreadPools().forEach((nodeId, original) -> {
                final var write = original.threadPoolUsageStatsMap().get(ThreadPool.Names.WRITE);
                if (write == null || (writeLoadDelta.get(nodeId) == 0.0 && nodesWithDeparture.contains(nodeId) == false)) {
                    result.put(nodeId, original);
                    return;
                }
                final var adjusted = new NodeUsageStatsForThreadPools.ThreadPoolUsageStats(
                    write.totalThreadPoolThreads(),
                    ShardMovementWriteLoadSimulator.updateNodeUtilizationWithShardMovements(
                        write.averageThreadPoolUtilization(),
                        (float) writeLoadDelta.get(nodeId),
                        write.totalThreadPoolThreads()
                    ),
                    ShardMovementWriteLoadSimulator.adjustThreadPoolQueueLatencyWithShardMovements(
                        write.maxThreadPoolQueueLatencyMillis(),
                        nodesWithDeparture.contains(nodeId)
                    )
                );
                result.put(
                    nodeId,
                    new NodeUsageStatsForThreadPools(
                        nodeId,
                        Maps.copyMapWithAddedOrReplacedEntry(original.threadPoolUsageStatsMap(), ThreadPool.Names.WRITE, adjusted)
                    )
                );
            });
            return Collections.unmodifiableMap(result);
        }
    }
}
