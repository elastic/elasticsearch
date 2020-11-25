/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCapacity;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderService;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ReactiveStorageDeciderService implements AutoscalingDeciderService {
    public static final String NAME = "reactive_storage";

    private static final Logger logger = LogManager.getLogger(ReactiveStorageDeciderService.class);
    private final DiskThresholdSettings diskThresholdSettings;

    public ReactiveStorageDeciderService(Settings settings, ClusterSettings clusterSettings) {
        this.diskThresholdSettings = new DiskThresholdSettings(settings, clusterSettings);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public List<Setting<?>> deciderSettings() {
        return List.of();
    }

    @Override
    public AutoscalingDeciderResult scale(Settings configuration, AutoscalingDeciderContext context) {
        AutoscalingCapacity autoscalingCapacity = context.currentCapacity();
        if (autoscalingCapacity == null || autoscalingCapacity.tier().storage() == null) {
            return new AutoscalingDeciderResult(null, new ReactiveReason("current capacity not available", -1, -1));
        }

        AllocationState allocationState = new AllocationState(context, diskThresholdSettings);
        long unassigned = allocationState.storagePreventsAllocation();
        long assigned = allocationState.storagePreventsRemainOrMove();
        if (unassigned > 0 || assigned > 0) {
            AutoscalingCapacity plusOne = AutoscalingCapacity.builder()
                .total(autoscalingCapacity.tier().storage().getBytes() + unassigned + assigned, null)
                .build();
            return new AutoscalingDeciderResult(
                plusOne,
                new ReactiveReason("not enough storage available, needs " + (unassigned + assigned), unassigned, assigned)
            );
        } else {
            AutoscalingCapacity ok = AutoscalingCapacity.builder().total(autoscalingCapacity.tier().storage(), null).build();
            return new AutoscalingDeciderResult(ok, new ReactiveReason("storage ok", unassigned, assigned));
        }
    }

    static boolean isDiskOnlyNoDecision(Decision decision) {
        // we consider throttling==yes, throttling should be temporary.
        List<Decision> nos = decision.getDecisions()
            .stream()
            .filter(single -> single.type() == Decision.Type.NO)
            .collect(Collectors.toList());
        return nos.size() == 1 && DiskThresholdDecider.NAME.equals(nos.get(0).label());

    }

    static Stream<RoutingNode> nodesInTier(RoutingNodes routingNodes, Predicate<DiscoveryNode> nodeTierPredicate) {
        Predicate<RoutingNode> routingNodePredicate = rn -> nodeTierPredicate.test(rn.node());
        return StreamSupport.stream(routingNodes.spliterator(), false).filter(routingNodePredicate);
    }

    public static class AllocationState {
        private final ClusterState state;
        private final AllocationDeciders allocationDeciders;
        private final DiskThresholdSettings diskThresholdSettings;
        private final ClusterInfo info;
        private final SnapshotShardSizeInfo shardSizeInfo;
        private final Predicate<DiscoveryNode> nodeTierPredicate;

        public AllocationState(AutoscalingDeciderContext context, DiskThresholdSettings diskThresholdSettings) {
            this(
                context.state(),
                context.allocationDeciders(),
                diskThresholdSettings,
                context.info(),
                context.snapshotShardSizeInfo(),
                context.nodes()::contains
            );
        }

        public AllocationState(
            ClusterState state,
            AllocationDeciders allocationDeciders,
            DiskThresholdSettings diskThresholdSettings,
            ClusterInfo info,
            SnapshotShardSizeInfo shardSizeInfo,
            Predicate<DiscoveryNode> nodeTierPredicate
        ) {
            this.state = state;
            this.allocationDeciders = allocationDeciders;
            this.diskThresholdSettings = diskThresholdSettings;
            this.info = info;
            this.shardSizeInfo = shardSizeInfo;
            this.nodeTierPredicate = nodeTierPredicate;
        }

        public long storagePreventsAllocation() {
            RoutingNodes routingNodes = new RoutingNodes(state, false);
            RoutingAllocation allocation = new RoutingAllocation(
                allocationDeciders,
                routingNodes,
                state,
                info,
                shardSizeInfo,
                System.nanoTime()
            );
            return StreamSupport.stream(state.getRoutingNodes().unassigned().spliterator(), false)
                .filter(shard -> canAllocate(shard, allocation) == false)
                .filter(shard -> cannotAllocateDueToStorage(shard, allocation))
                .mapToLong(this::sizeOf)
                .sum();
        }

        public long storagePreventsRemainOrMove() {
            RoutingNodes routingNodes = new RoutingNodes(state, false);
            RoutingAllocation allocation = new RoutingAllocation(
                allocationDeciders,
                routingNodes,
                state,
                info,
                shardSizeInfo,
                System.nanoTime()
            );
            List<ShardRouting> candidates = state.getRoutingNodes()
                .shardsWithState(ShardRoutingState.STARTED)
                .stream()
                .filter(shard -> allocationDeciders.canRemain(shard, routingNodes.node(shard.currentNodeId()), allocation) == Decision.NO)
                .filter(shard -> canAllocate(shard, allocation) == false)
                .collect(Collectors.toList());

            // track these to ensure we do not double account if they both cannot remain and allocated due to storage.
            Set<ShardRouting> unmovableShards = candidates.stream()
                .filter(s -> allocatedToTier(s, allocation))
                .filter(s -> cannotRemainDueToStorage(s, allocation))
                .collect(Collectors.toSet());
            long unmovableBytes = unmovableShards.stream()
                .collect(Collectors.groupingBy(ShardRouting::currentNodeId))
                .entrySet()
                .stream()
                .mapToLong(e -> unmovableSize(e.getKey(), e.getValue()))
                .sum();

            long unallocatableBytes = candidates.stream()
                .filter(Predicate.not(unmovableShards::contains))
                .filter(s1 -> cannotAllocateDueToStorage(s1, allocation))
                .mapToLong(this::sizeOf)
                .sum();

            return unallocatableBytes + unmovableBytes;
        }

        private boolean allocatedToTier(ShardRouting s, RoutingAllocation allocation) {
            return nodeTierPredicate.test(allocation.routingNodes().node(s.currentNodeId()).node());
        }

        long sizeOf(ShardRouting shard) {
            long expectedShardSize = getExpectedShardSize(shard);
            if (expectedShardSize == 0L && shard.primary() == false) {
                ShardRouting primary = state.getRoutingNodes().activePrimary(shard.shardId());
                if (primary != null) {
                    expectedShardSize = getExpectedShardSize(primary);
                }
            }
            assert expectedShardSize >= 0;
            // todo: we should ideally not have the level of uncertainty we have here.
            return expectedShardSize == 0L ? ByteSizeUnit.KB.toBytes(1) : expectedShardSize;
        }

        private long getExpectedShardSize(ShardRouting shard) {
            return DiskThresholdDecider.getExpectedShardSize(shard, 0L, info, shardSizeInfo, state.metadata(), state.routingTable());
        }

        long unmovableSize(String nodeId, Collection<ShardRouting> shards) {
            ClusterInfo info = this.info;
            DiskUsage diskUsage = info.getNodeMostAvailableDiskUsages().get(nodeId);
            if (diskUsage == null) {
                // do not want to scale up then, since this should only happen when node has just joined (clearly edge case).
                return 0;
            }

            long threshold = Math.max(
                diskThresholdSettings.getFreeBytesThresholdHigh().getBytes(),
                thresholdFromPercentage(diskThresholdSettings.getFreeDiskThresholdHigh(), diskUsage)
            );
            long missing = threshold - diskUsage.getFreeBytes();
            return Math.max(missing, shards.stream().mapToLong(this::sizeOf).min().orElseThrow());
        }

        private long thresholdFromPercentage(Double percentage, DiskUsage diskUsage) {
            if (percentage == null) {
                return 0L;
            }

            return (long) Math.ceil(diskUsage.getTotalBytes() * percentage / 100);
        }

        /**
         * Check that disk decider is only decider for a node preventing allocation of the shard.
         * @return true if and only if a node exists in the tier where only disk decider prevents allocation
         */
        private boolean cannotAllocateDueToStorage(ShardRouting shard, RoutingAllocation allocation) {
            assert allocation.debugDecision() == false;
            allocation.debugDecision(true);
            try {
                return nodesInTier(allocation.routingNodes(), nodeTierPredicate).map(
                    node -> allocationDeciders.canAllocate(shard, node, allocation)
                ).anyMatch(ReactiveStorageDeciderService::isDiskOnlyNoDecision);
            } finally {
                allocation.debugDecision(false);
            }
        }

        /**
         * Check that the disk decider is only decider that says NO to let shard remain on current node.
         * @return true if and only if disk decider is only decider that says NO to canRemain.
         */
        private boolean cannotRemainDueToStorage(ShardRouting shard, RoutingAllocation allocation) {
            assert allocation.debugDecision() == false;
            allocation.debugDecision(true);
            try {
                return isDiskOnlyNoDecision(
                    allocationDeciders.canRemain(shard, allocation.routingNodes().node(shard.currentNodeId()), allocation)
                );
            } finally {
                allocation.debugDecision(false);
            }
        }

        private boolean canAllocate(ShardRouting shard, RoutingAllocation allocation) {
            return nodesInTier(allocation.routingNodes(), nodeTierPredicate).anyMatch(
                node -> allocationDeciders.canAllocate(shard, node, allocation) != Decision.NO
            );
        }

        public ClusterState state() {
            return state;
        }
    }

    public static class ReactiveReason implements AutoscalingDeciderResult.Reason {
        private final String reason;
        private final long unassigned;
        private final long assigned;

        public ReactiveReason(String reason, long unassigned, long assigned) {
            this.reason = reason;
            this.unassigned = unassigned;
            this.assigned = assigned;
        }

        public ReactiveReason(StreamInput in) throws IOException {
            this.reason = in.readString();
            this.unassigned = in.readLong();
            this.assigned = in.readLong();
        }

        @Override
        public String summary() {
            return reason;
        }

        @Override
        public String getWriteableName() {
            return ReactiveStorageDeciderService.NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(reason);
            out.writeLong(unassigned);
            out.writeLong(assigned);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("reason", reason);
            builder.field("unassgined", unassigned);
            builder.field("assgined", assigned);
            builder.endObject();
            return builder;
        }
    }
}
