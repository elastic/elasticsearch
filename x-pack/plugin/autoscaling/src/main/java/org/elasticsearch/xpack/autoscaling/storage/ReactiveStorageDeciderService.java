/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamMetadata;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCapacity;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderService;
import org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider;
import org.elasticsearch.xpack.core.DataTier;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ReactiveStorageDeciderService implements AutoscalingDeciderService {
    public static final String NAME = "reactive_storage";

    private final DiskThresholdSettings diskThresholdSettings;
    private final DataTierAllocationDecider dataTierAllocationDecider;
    private final AllocationDeciders allocationDeciders;

    public ReactiveStorageDeciderService(Settings settings, ClusterSettings clusterSettings, AllocationDeciders allocationDeciders) {
        this.diskThresholdSettings = new DiskThresholdSettings(settings, clusterSettings);
        this.dataTierAllocationDecider = new DataTierAllocationDecider(settings, clusterSettings);
        this.allocationDeciders = allocationDeciders;
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
    public List<DiscoveryNodeRole> roles() {
        return List.of(
            DiscoveryNodeRole.DATA_ROLE,
            DataTier.DATA_CONTENT_NODE_ROLE,
            DataTier.DATA_HOT_NODE_ROLE,
            DataTier.DATA_WARM_NODE_ROLE,
            DataTier.DATA_COLD_NODE_ROLE
        );
    }

    @Override
    public AutoscalingDeciderResult scale(Settings configuration, AutoscalingDeciderContext context) {
        AutoscalingCapacity autoscalingCapacity = context.currentCapacity();
        if (autoscalingCapacity == null || autoscalingCapacity.total().storage() == null) {
            return new AutoscalingDeciderResult(null, new ReactiveReason("current capacity not available", -1, -1));
        }

        AllocationState allocationState = new AllocationState(
            context,
            diskThresholdSettings,
            allocationDeciders,
            dataTierAllocationDecider
        );
        long unassignedBytes = allocationState.storagePreventsAllocation();
        long assignedBytes = allocationState.storagePreventsRemainOrMove();
        long maxShardSize = allocationState.maxShardSize();
        assert assignedBytes >= 0;
        assert unassignedBytes >= 0;
        assert maxShardSize >= 0;
        String message = message(unassignedBytes, assignedBytes);
        AutoscalingCapacity requiredCapacity = AutoscalingCapacity.builder()
            .total(autoscalingCapacity.total().storage().getBytes() + unassignedBytes + assignedBytes, null)
            .node(maxShardSize, null)
            .build();
        return new AutoscalingDeciderResult(requiredCapacity, new ReactiveReason(message, unassignedBytes, assignedBytes));
    }

    static String message(long unassignedBytes, long assignedBytes) {
        return unassignedBytes > 0 || assignedBytes > 0
            ? "not enough storage available, needs " + new ByteSizeValue(unassignedBytes + assignedBytes)
            : "storage ok";
    }

    static boolean isDiskOnlyNoDecision(Decision decision) {
        // we consider throttling==yes, throttling should be temporary.
        List<Decision> nos = decision.getDecisions()
            .stream()
            .filter(single -> single.type() == Decision.Type.NO)
            .collect(Collectors.toList());
        return nos.size() == 1 && DiskThresholdDecider.NAME.equals(nos.get(0).label());
    }

    // todo: move this to top level class.
    public static class AllocationState {
        private final ClusterState state;
        private final AllocationDeciders allocationDeciders;
        private final DataTierAllocationDecider dataTierAllocationDecider;
        private final DiskThresholdSettings diskThresholdSettings;
        private final ClusterInfo info;
        private final SnapshotShardSizeInfo shardSizeInfo;
        private final Predicate<DiscoveryNode> nodeTierPredicate;
        private final Set<DiscoveryNode> nodes;
        private final Set<String> nodeIds;
        private final Set<DiscoveryNodeRole> roles;

        AllocationState(
            AutoscalingDeciderContext context,
            DiskThresholdSettings diskThresholdSettings,
            AllocationDeciders allocationDeciders,
            DataTierAllocationDecider dataTierAllocationDecider
        ) {
            this(
                context.state(),
                allocationDeciders,
                dataTierAllocationDecider,
                diskThresholdSettings,
                context.info(),
                context.snapshotShardSizeInfo(),
                context.nodes(),
                context.roles()
            );
        }

        AllocationState(
            ClusterState state,
            AllocationDeciders allocationDeciders,
            DataTierAllocationDecider dataTierAllocationDecider,
            DiskThresholdSettings diskThresholdSettings,
            ClusterInfo info,
            SnapshotShardSizeInfo shardSizeInfo,
            Set<DiscoveryNode> nodes,
            Set<DiscoveryNodeRole> roles
        ) {
            this.state = state;
            this.allocationDeciders = allocationDeciders;
            this.dataTierAllocationDecider = dataTierAllocationDecider;
            this.diskThresholdSettings = diskThresholdSettings;
            this.info = info;
            this.shardSizeInfo = shardSizeInfo;
            this.nodes = nodes;
            this.nodeIds = nodes.stream().map(DiscoveryNode::getId).collect(Collectors.toSet());
            this.nodeTierPredicate = nodes::contains;
            this.roles = roles;
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
                .filter(shard -> canRemainOnlyHighestTierPreference(shard, allocation) == false)
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

        /**
         * Check if shard can remain where it is, with the additional check that the DataTierAllocationDecider did not allow it to stay
         * on a node in a lower preference tier.
         */
        public boolean canRemainOnlyHighestTierPreference(ShardRouting shard, RoutingAllocation allocation) {
            boolean result = allocationDeciders.canRemain(
                shard,
                allocation.routingNodes().node(shard.currentNodeId()),
                allocation
            ) != Decision.NO;
            if (result
                && nodes.isEmpty()
                && Strings.hasText(
                    DataTierAllocationDecider.INDEX_ROUTING_PREFER_SETTING.get(indexMetadata(shard, allocation).getSettings())
                )) {
                // The data tier decider allows a shard to remain on a lower preference tier when no nodes exists on higher preference
                // tiers.
                // Here we ensure that if our policy governs the highest preference tier, we assume the shard needs to move to that tier
                // once a node is started for it.
                // In the case of overlapping policies, this is consistent with double accounting of unassigned.
                return isAssignedToTier(shard, allocation) == false;
            }
            return result;
        }

        private boolean allocatedToTier(ShardRouting s, RoutingAllocation allocation) {
            return nodeTierPredicate.test(allocation.routingNodes().node(s.currentNodeId()).node());
        }

        /**
         * Check that disk decider is only decider for a node preventing allocation of the shard.
         * @return true if and only if a node exists in the tier where only disk decider prevents allocation
         */
        private boolean cannotAllocateDueToStorage(ShardRouting shard, RoutingAllocation allocation) {
            if (nodeIds.isEmpty() && isAssignedToTier(shard, allocation)) {
                return true;
            }
            assert allocation.debugDecision() == false;
            // enable debug decisions to see all decisions and preserve the allocation decision label
            allocation.debugDecision(true);
            try {
                return nodesInTier(allocation.routingNodes()).map(node -> allocationDeciders.canAllocate(shard, node, allocation))
                    .anyMatch(ReactiveStorageDeciderService::isDiskOnlyNoDecision);
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
            // enable debug decisions to see all decisions and preserve the allocation decision label
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
            return nodesInTier(allocation.routingNodes()).anyMatch(
                node -> allocationDeciders.canAllocate(shard, node, allocation) != Decision.NO
            );
        }

        private boolean isAssignedToTier(ShardRouting shard, RoutingAllocation allocation) {
            IndexMetadata indexMetadata = indexMetadata(shard, allocation);
            return dataTierAllocationDecider.shouldFilter(indexMetadata, roles, this::highestPreferenceTier, allocation) != Decision.NO;
        }

        private IndexMetadata indexMetadata(ShardRouting shard, RoutingAllocation allocation) {
            return allocation.metadata().getIndexSafe(shard.index());
        }

        private Optional<String> highestPreferenceTier(String tierPreference, DiscoveryNodes nodes) {
            String[] preferredTiers = DataTierAllocationDecider.parseTierList(tierPreference);
            assert preferredTiers.length > 0;
            return Optional.of(preferredTiers[0]);
        }

        public long maxShardSize() {
            return nodesInTier(state.getRoutingNodes()).flatMap(rn -> StreamSupport.stream(rn.spliterator(), false))
                .mapToLong(this::sizeOf)
                .max()
                .orElse(0L);
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

        Stream<RoutingNode> nodesInTier(RoutingNodes routingNodes) {
            return nodeIds.stream().map(n -> routingNodes.node(n));
        }

        private static class SingleForecast {
            private final Map<IndexMetadata, Long> additionalIndices;
            private final DataStream updatedDataStream;

            private SingleForecast(Map<IndexMetadata, Long> additionalIndices, DataStream updatedDataStream) {
                this.additionalIndices = additionalIndices;
                this.updatedDataStream = updatedDataStream;
            }

            public void applyRouting(RoutingTable.Builder routing) {
                additionalIndices.keySet().forEach(routing::addAsNew);
            }

            public void applyMetadata(Metadata.Builder metadataBuilder) {
                additionalIndices.keySet().forEach(imd -> metadataBuilder.put(imd, false));
                metadataBuilder.put(updatedDataStream);
            }

            public void applySize(ImmutableOpenMap.Builder<String, Long> builder, RoutingTable updatedRoutingTable) {
                for (Map.Entry<IndexMetadata, Long> entry : additionalIndices.entrySet()) {
                    List<ShardRouting> shardRoutings = updatedRoutingTable.allShards(entry.getKey().getIndex().getName());
                    long size = entry.getValue() / shardRoutings.size();
                    shardRoutings.forEach(s -> builder.put(ClusterInfo.shardIdentifierFromRouting(s), size));
                }
            }
        }

        public AllocationState forecast(long forecastWindow, long now) {
            if (forecastWindow == 0) {
                return this;
            }
            // for now we only look at data-streams. We might want to also detect alias based time-based indices.
            DataStreamMetadata dataStreamMetadata = state.metadata().custom(DataStreamMetadata.TYPE);
            if (dataStreamMetadata == null) {
                return this;
            }
            List<SingleForecast> singleForecasts = dataStreamMetadata.dataStreams()
                .keySet()
                .stream()
                .map(state.metadata().getIndicesLookup()::get)
                .map(IndexAbstraction.DataStream.class::cast)
                .map(ds -> forecast(ds, forecastWindow, now))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
            if (singleForecasts.isEmpty()) {
                return this;
            }
            Metadata.Builder metadataBuilder = Metadata.builder(state.metadata());
            RoutingTable.Builder routingTableBuilder = RoutingTable.builder(state.routingTable());
            ImmutableOpenMap.Builder<String, Long> sizeBuilder = ImmutableOpenMap.builder();
            singleForecasts.forEach(p -> p.applyMetadata(metadataBuilder));
            singleForecasts.forEach(p -> p.applyRouting(routingTableBuilder));
            RoutingTable routingTable = routingTableBuilder.build();
            singleForecasts.forEach(p -> p.applySize(sizeBuilder, routingTable));
            ClusterState forecastClusterState = ClusterState.builder(state).metadata(metadataBuilder).routingTable(routingTable).build();
            ClusterInfo forecastInfo = new ExtendedClusterInfo(sizeBuilder.build(), AllocationState.this.info);

            return new AllocationState(
                forecastClusterState,
                allocationDeciders,
                dataTierAllocationDecider,
                diskThresholdSettings,
                forecastInfo,
                shardSizeInfo,
                nodes,
                roles
            );
        }

        private SingleForecast forecast(IndexAbstraction.DataStream stream, long forecastWindow, long now) {
            List<IndexMetadata> indices = stream.getIndices();
            if (dataStreamAllocatedToNodes(indices) == false) return null;
            long minCreationDate = Long.MAX_VALUE;
            long totalSize = 0;
            int count = 0;
            while (count < indices.size()) {
                ++count;
                IndexMetadata indexMetadata = indices.get(indices.size() - count);
                long creationDate = indexMetadata.getCreationDate();
                if (creationDate < 0) {
                    return null;
                }
                minCreationDate = Math.min(minCreationDate, creationDate);
                totalSize += state.getRoutingTable().allShards(indexMetadata.getIndex().getName()).stream().mapToLong(this::sizeOf).sum();
                // we terminate loop after collecting data to ensure we consider at least the forecast window (and likely some more).
                if (creationDate <= now - forecastWindow) {
                    break;
                }
            }

            if (totalSize == 0) {
                return null;
            }

            // round up
            long avgSizeCeil = (totalSize - 1) / count + 1;

            long actualWindow = now - minCreationDate;
            if (actualWindow == 0) {
                return null;
            }

            // rather than simulate rollover, we copy the index meta data and do minimal adjustments.
            long scaledTotalSize;
            int numberNewIndices;
            if (actualWindow > forecastWindow) {
                scaledTotalSize = BigInteger.valueOf(totalSize)
                    .multiply(BigInteger.valueOf(forecastWindow))
                    .divide(BigInteger.valueOf(actualWindow))
                    .longValueExact();
                // round up
                numberNewIndices = (int) Math.min((scaledTotalSize - 1) / avgSizeCeil + 1, indices.size());
                if (scaledTotalSize == 0) {
                    return null;
                }
            } else {
                numberNewIndices = count;
                scaledTotalSize = totalSize;
            }

            IndexMetadata writeIndex = stream.getWriteIndex();

            Map<IndexMetadata, Long> newIndices = new HashMap<>();
            DataStream dataStream = stream.getDataStream();
            for (int i = 0; i < numberNewIndices; ++i) {
                final String uuid = UUIDs.randomBase64UUID();
                dataStream = dataStream.rollover(uuid, Version.CURRENT);
                IndexMetadata newIndex = IndexMetadata.builder(writeIndex)
                    .index(dataStream.getWriteIndex().getName())
                    .settings(Settings.builder().put(writeIndex.getSettings()).put(IndexMetadata.SETTING_INDEX_UUID, uuid))
                    .build();
                long size = Math.min(avgSizeCeil, scaledTotalSize - (avgSizeCeil * i));
                assert size > 0;
                newIndices.put(newIndex, size);
            }

            return new SingleForecast(newIndices, dataStream);
        }

        /**
         * Check that at least one shard is on the set of nodes. If they are all unallocated, we do not want to make any prediction to not
         * hit the wrong policy.
         * @param indices the indices of the data stream, in original order from data stream meta.
         * @return true if the first allocated index is allocated only to the set of nodes.
         */
        private boolean dataStreamAllocatedToNodes(List<IndexMetadata> indices) {
            for (int i = 0; i < indices.size(); ++i) {
                IndexMetadata indexMetadata = indices.get(indices.size() - i - 1);
                Set<Boolean> inNodes = state.getRoutingTable()
                    .allShards(indexMetadata.getIndex().getName())
                    .stream()
                    .map(ShardRouting::currentNodeId)
                    .filter(Objects::nonNull)
                    .map(nodeIds::contains)
                    .collect(Collectors.toSet());
                if (inNodes.contains(false)) {
                    return false;
                }
                if (inNodes.contains(true)) {
                    return true;
                }
            }
            return false;
        }

        // for tests
        ClusterState state() {
            return state;
        }

        ClusterInfo info() {
            return info;
        }

        private static class ExtendedClusterInfo extends ClusterInfo {
            private final ClusterInfo delegate;

            private ExtendedClusterInfo(ImmutableOpenMap<String, Long> extraShardSizes, ClusterInfo info) {
                super(info.getNodeLeastAvailableDiskUsages(), info.getNodeMostAvailableDiskUsages(), extraShardSizes, null, null);
                this.delegate = info;
            }

            @Override
            public Long getShardSize(ShardRouting shardRouting) {
                Long shardSize = super.getShardSize(shardRouting);
                if (shardSize != null) {
                    return shardSize;
                } else {
                    return delegate.getShardSize(shardRouting);
                }
            }

            @Override
            public long getShardSize(ShardRouting shardRouting, long defaultValue) {
                Long shardSize = super.getShardSize(shardRouting);
                if (shardSize != null) {
                    return shardSize;
                } else {
                    return delegate.getShardSize(shardRouting, defaultValue);
                }
            }

            @Override
            public String getDataPath(ShardRouting shardRouting) {
                return delegate.getDataPath(shardRouting);
            }

            @Override
            public ReservedSpace getReservedSpace(String nodeId, String dataPath) {
                return delegate.getReservedSpace(nodeId, dataPath);
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                throw new UnsupportedOperationException();
            }
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

        public long unassigned() {
            return unassigned;
        }

        public long assigned() {
            return assigned;
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
            builder.field("unassigned", unassigned);
            builder.field("assigned", assigned);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ReactiveReason that = (ReactiveReason) o;
            return unassigned == that.unassigned && assigned == that.assigned && reason.equals(that.reason);
        }

        @Override
        public int hashCode() {
            return Objects.hash(reason, unassigned, assigned);
        }
    }
}
