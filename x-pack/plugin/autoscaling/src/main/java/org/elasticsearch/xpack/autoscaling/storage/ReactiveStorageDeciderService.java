/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamMetadata;
import org.elasticsearch.cluster.metadata.DesiredNodes;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeFilters;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ExpectedShardSizeEstimator;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingRoleStrategy;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ResizeAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCapacity;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderService;
import org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toMap;

public class ReactiveStorageDeciderService implements AutoscalingDeciderService {
    public static final String NAME = "reactive_storage";
    /**
     * An estimate of what space other things than accounted for by shard sizes in ClusterInfo use on disk.
     * Set conservatively low for now.
     */
    static final long NODE_DISK_OVERHEAD = ByteSizeValue.ofMb(10).getBytes();

    private final DiskThresholdSettings diskThresholdSettings;
    private final AllocationDeciders allocationDeciders;
    private final ShardRoutingRoleStrategy shardRoutingRoleStrategy;

    private static final Predicate<String> REMOVE_NODE_LOCKED_FILTER_INITIAL = removeNodeLockedFilterPredicate(
        IndexMetadata.INDEX_ROUTING_INITIAL_RECOVERY_GROUP_SETTING.getKey()
    );

    private static final Predicate<String> REMOVE_NODE_LOCKED_FILTER_REQUIRE = removeNodeLockedFilterPredicate(
        IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey()
    );

    private static final Predicate<String> REMOVE_NODE_LOCKED_FILTER_INCLUDE = removeNodeLockedFilterPredicate(
        IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey()
    );

    private static Predicate<String> removeNodeLockedFilterPredicate(String settingPrefix) {
        return Predicate.not(
            DiscoveryNodeFilters.SINGLE_NODE_NAMES.stream().map(settingPrefix::concat).collect(Collectors.toSet())::contains
        );
    }

    public ReactiveStorageDeciderService(
        Settings settings,
        ClusterSettings clusterSettings,
        AllocationDeciders allocationDeciders,
        ShardRoutingRoleStrategy shardRoutingRoleStrategy
    ) {
        this.diskThresholdSettings = new DiskThresholdSettings(settings, clusterSettings);
        this.allocationDeciders = allocationDeciders;
        this.shardRoutingRoleStrategy = shardRoutingRoleStrategy;
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
            DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE,
            DiscoveryNodeRole.DATA_HOT_NODE_ROLE,
            DiscoveryNodeRole.DATA_WARM_NODE_ROLE,
            DiscoveryNodeRole.DATA_COLD_NODE_ROLE
        );
    }

    @Override
    public AutoscalingDeciderResult scale(Settings configuration, AutoscalingDeciderContext context) {
        AutoscalingCapacity autoscalingCapacity = context.currentCapacity();
        if (autoscalingCapacity == null || autoscalingCapacity.total().storage() == null) {
            return new AutoscalingDeciderResult(null, new ReactiveReason("current capacity not available", -1, -1));
        }

        AllocationState allocationState = allocationState(context);
        var assignedBytesUnmovableShards = allocationState.storagePreventsRemainOrMove();
        long assignedBytes = assignedBytesUnmovableShards.sizeInBytes();
        var unassignedBytesUnassignedShards = allocationState.storagePreventsAllocation();
        long unassignedBytes = unassignedBytesUnassignedShards.sizeInBytes();
        long maxShardSize = allocationState.maxShardSize();
        long maxNodeLockedSize = allocationState.maxNodeLockedSize();
        assert assignedBytes >= 0;
        assert unassignedBytes >= 0;
        assert maxShardSize >= 0;
        String message = message(unassignedBytes, assignedBytes);
        long requiredTotalStorage = autoscalingCapacity.total().storage().getBytes() + unassignedBytes + assignedBytes;
        long minimumNodeSize = requiredTotalStorage > 0L
            ? nodeSizeForDataBelowLowWatermark(Math.max(maxShardSize, maxNodeLockedSize), diskThresholdSettings) + NODE_DISK_OVERHEAD
            : 0L;
        AutoscalingCapacity requiredCapacity = AutoscalingCapacity.builder()
            .total(requiredTotalStorage, null, null)
            .node(minimumNodeSize, null, null)
            .build();
        return new AutoscalingDeciderResult(
            requiredCapacity,
            new ReactiveReason(
                message,
                unassignedBytes,
                unassignedBytesUnassignedShards.shardIds(),
                assignedBytes,
                assignedBytesUnmovableShards.shardIds(),
                unassignedBytesUnassignedShards.shardNodeDecisions(),
                assignedBytesUnmovableShards.shardNodeDecisions()
            )
        );
    }

    AllocationState allocationState(AutoscalingDeciderContext context) {
        return new AllocationState(context, diskThresholdSettings, allocationDeciders, shardRoutingRoleStrategy);
    }

    static String message(long unassignedBytes, long assignedBytes) {
        return unassignedBytes > 0 || assignedBytes > 0
            ? "not enough storage available, needs " + ByteSizeValue.ofBytes(unassignedBytes + assignedBytes)
            : "storage ok";
    }

    static boolean isDiskOnlyNoDecision(Decision decision) {
        return singleNoDecision(decision, Predicates.always()).map(DiskThresholdDecider.NAME::equals).orElse(false);
    }

    static boolean isResizeOnlyNoDecision(Decision decision) {
        return singleNoDecision(decision, Predicates.always()).map(ResizeAllocationDecider.NAME::equals).orElse(false);
    }

    static boolean isFilterTierOnlyDecision(Decision decision, IndexMetadata indexMetadata) {
        // only primary shards are handled here, allowing us to disregard same shard allocation decider.
        return singleNoDecision(decision, single -> SameShardAllocationDecider.NAME.equals(single.label()) == false).filter(
            FilterAllocationDecider.NAME::equals
        ).map(d -> filterLooksLikeTier(indexMetadata)).orElse(false);
    }

    static boolean filterLooksLikeTier(IndexMetadata indexMetadata) {
        return isOnlyAttributeValueFilter(indexMetadata.requireFilters())
            && isOnlyAttributeValueFilter(indexMetadata.includeFilters())
            && isOnlyAttributeValueFilter(indexMetadata.excludeFilters());
    }

    private static boolean isOnlyAttributeValueFilter(DiscoveryNodeFilters filters) {
        if (filters == null) {
            return true;
        } else {
            return DiscoveryNodeFilters.trimTier(filters).isOnlyAttributeValueFilter();
        }
    }

    static Optional<String> singleNoDecision(Decision decision, Predicate<Decision> predicate) {
        List<Decision> nos = decision.getDecisions()
            .stream()
            .filter(single -> single.type() == Decision.Type.NO)
            .filter(predicate)
            .toList();

        if (nos.size() == 1) {
            return Optional.ofNullable(nos.get(0).label());
        } else {
            return Optional.empty();
        }
    }

    static long nodeSizeForDataBelowLowWatermark(long neededBytes, DiskThresholdSettings thresholdSettings) {
        return thresholdSettings.getMinimumTotalSizeForBelowLowWatermark(ByteSizeValue.ofBytes(neededBytes)).getBytes();
    }

    // todo: move this to top level class.
    public static class AllocationState {

        static final int MAX_AMOUNT_OF_SHARD_DECISIONS = 5;

        private final ClusterState state;
        private final ClusterState originalState;
        private final AllocationDeciders allocationDeciders;
        private final ShardRoutingRoleStrategy shardRoutingRoleStrategy;
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
            ShardRoutingRoleStrategy shardRoutingRoleStrategy
        ) {
            this(
                context.state(),
                allocationDeciders,
                shardRoutingRoleStrategy,
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
            ShardRoutingRoleStrategy shardRoutingRoleStrategy,
            DiskThresholdSettings diskThresholdSettings,
            ClusterInfo info,
            SnapshotShardSizeInfo shardSizeInfo,
            Set<DiscoveryNode> nodes,
            Set<DiscoveryNodeRole> roles
        ) {
            this.state = removeNodeLockFilters(state);
            this.originalState = state;
            this.allocationDeciders = allocationDeciders;
            this.shardRoutingRoleStrategy = shardRoutingRoleStrategy;
            this.diskThresholdSettings = diskThresholdSettings;
            this.info = info;
            this.shardSizeInfo = shardSizeInfo;
            this.nodes = nodes;
            this.nodeIds = nodes.stream().map(DiscoveryNode::getId).collect(Collectors.toSet());
            this.nodeTierPredicate = nodes::contains;
            this.roles = roles;
        }

        private interface ShardNodeDecision {
            ShardRouting shard();
        }

        record ShardNodeAllocationDecision(ShardRouting shard, List<NodeDecision> nodeDecisions) implements ShardNodeDecision {}

        record ShardNodeRemainDecision(ShardRouting shard, NodeDecision nodeDecision) implements ShardNodeDecision {}

        static <T extends ShardNodeDecision> T preferPrimary(T snd1, T snd2) {
            return snd1.shard().primary() ? snd1 : snd2;
        }

        public ShardsAllocationResults storagePreventsAllocation() {
            RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state, info, shardSizeInfo, System.nanoTime());
            List<ShardNodeAllocationDecision> unassignedShards = state.getRoutingNodes()
                .unassigned()
                .stream()
                .filter(shard -> canAllocate(shard, allocation) == false)
                .flatMap(shard -> cannotAllocateDueToStorage(shard, allocation).stream())
                .toList();
            return new ShardsAllocationResults(
                unassignedShards.stream().map(e -> e.shard).mapToLong(this::sizeOf).sum(),
                unassignedShards.stream().map(e -> e.shard.shardId()).collect(Collectors.toCollection(TreeSet::new)),
                unassignedShards.stream()
                    .filter(shardNodeDecisions -> shardNodeDecisions.nodeDecisions.size() > 0)
                    .collect(toSortedMap(snd -> snd.shard.shardId(), snd -> new NodeDecisions(snd.nodeDecisions, null)))
                    .entrySet()
                    .stream()
                    .limit(MAX_AMOUNT_OF_SHARD_DECISIONS)
                    .collect(toSortedMap(Map.Entry::getKey, Map.Entry::getValue))
            );
        }

        public ShardsAllocationResults storagePreventsRemainOrMove() {
            RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state, info, shardSizeInfo, System.nanoTime());

            List<ShardRouting> candidates = new LinkedList<>();
            for (RoutingNode routingNode : state.getRoutingNodes()) {
                for (ShardRouting shard : routingNode) {
                    if (shard.started()
                        && canRemainOnlyHighestTierPreference(shard, allocation) == false
                        && canAllocate(shard, allocation) == false) {
                        candidates.add(shard);
                    }
                }
            }

            // track these to ensure we do not double account if they both cannot remain and allocated due to storage.
            List<ShardNodeRemainDecision> unmovableShardNodeDecisions = candidates.stream()
                .filter(s -> allocatedToTier(s, allocation))
                .flatMap(shard -> cannotRemainDueToStorage(shard, allocation).stream())
                .toList();
            Map<ShardId, ShardNodeRemainDecision> perShardIdUnmovable = unmovableShardNodeDecisions.stream()
                .collect(toMap(snd -> snd.shard().shardId(), Function.identity(), AllocationState::preferPrimary, TreeMap::new));
            Map<ShardId, NodeDecisions> otherNodesOnTierAllocationDecisions = perShardIdUnmovable.values()
                .stream()
                .limit(MAX_AMOUNT_OF_SHARD_DECISIONS)
                .collect(
                    toMap(
                        snd -> snd.shard().shardId(),
                        snd -> new NodeDecisions(canAllocateDecisions(allocation, snd.shard()), snd.nodeDecision())
                    )
                );
            Set<ShardRouting> unmovableShards = unmovableShardNodeDecisions.stream().map(e -> e.shard).collect(Collectors.toSet());

            long unmovableBytes = unmovableShards.stream()
                .collect(Collectors.groupingBy(ShardRouting::currentNodeId))
                .entrySet()
                .stream()
                .mapToLong(e -> unmovableSize(e.getKey(), e.getValue()))
                .sum();

            List<ShardNodeAllocationDecision> unallocatedShardNodeDecisions = candidates.stream()
                .filter(Predicate.not(unmovableShards::contains))
                .flatMap(shard -> cannotAllocateDueToStorage(shard, allocation).stream())
                .toList();
            Map<ShardId, ShardNodeAllocationDecision> perShardIdUnallocated = unallocatedShardNodeDecisions.stream()
                .collect(toMap(snd -> snd.shard().shardId(), Function.identity(), AllocationState::preferPrimary, TreeMap::new));
            Map<ShardId, NodeDecisions> canRemainDecisionsForUnallocatedShards = perShardIdUnallocated.values()
                .stream()
                .limit(MAX_AMOUNT_OF_SHARD_DECISIONS)
                .collect(
                    toMap(
                        snd -> snd.shard().shardId(),
                        snd -> new NodeDecisions(snd.nodeDecisions(), canRemainDecision(allocation, snd.shard()))
                    )
                );
            long unallocatableBytes = unallocatedShardNodeDecisions.stream().map(e -> e.shard).mapToLong(this::sizeOf).sum();

            Map<ShardId, NodeDecisions> shardDecisions = Stream.concat(
                canRemainDecisionsForUnallocatedShards.entrySet().stream(),
                otherNodesOnTierAllocationDecisions.entrySet().stream()
            )
                .collect(toSortedMap(Map.Entry::getKey, Map.Entry::getValue))
                .entrySet()
                .stream()
                .limit(MAX_AMOUNT_OF_SHARD_DECISIONS)
                .collect(toSortedMap(Map.Entry::getKey, Map.Entry::getValue));
            return new ShardsAllocationResults(
                unallocatableBytes + unmovableBytes,
                Stream.concat(unmovableShardNodeDecisions.stream(), unallocatedShardNodeDecisions.stream())
                    .map(e -> e.shard().shardId())
                    .collect(Collectors.toCollection(TreeSet::new)),
                shardDecisions
            );
        }

        private List<NodeDecision> canAllocateDecisions(RoutingAllocation allocation, ShardRouting shardRouting) {
            return state.getRoutingNodes()
                .stream()
                .filter(n -> nodeTierPredicate.test(n.node()))
                .filter(n -> shardRouting.currentNodeId().equals(n.nodeId()) == false)
                .map(
                    n -> new NodeDecision(
                        n.node(),
                        withAllocationDebugEnabled(allocation, () -> allocationDeciders.canAllocate(shardRouting, n, allocation))
                    )
                )
                .toList();
        }

        private NodeDecision canRemainDecision(RoutingAllocation allocation, ShardRouting shard) {
            return new NodeDecision(
                allocation.routingNodes().node(shard.currentNodeId()).node(),
                withAllocationDebugEnabled(
                    allocation,
                    () -> allocationDeciders.canRemain(shard, allocation.routingNodes().node(shard.currentNodeId()), allocation)
                )
            );
        }

        private static <T extends Decision> T withAllocationDebugEnabled(RoutingAllocation allocation, Supplier<T> supplier) {
            assert allocation.debugDecision() == false;
            allocation.debugDecision(true);
            try {
                return supplier.get();
            } finally {
                allocation.debugDecision(false);
            }
        }

        private static <T> Collector<T, ?, SortedMap<ShardId, NodeDecisions>> toSortedMap(
            Function<T, ShardId> keyMapper,
            Function<T, NodeDecisions> valueMapper
        ) {
            return toMap(keyMapper, valueMapper, (a, b) -> b, TreeMap::new);
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
                && Strings.hasText(DataTier.TIER_PREFERENCE_SETTING.get(indexMetadata(shard, allocation).getSettings()))) {
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
         * @return Non-empty {@link ShardNodeAllocationDecision} if and only if a node exists in the tier
         *         where only disk decider prevents allocation
         */
        private Optional<ShardNodeAllocationDecision> cannotAllocateDueToStorage(ShardRouting shard, RoutingAllocation allocation) {
            if (nodeIds.isEmpty() && needsThisTier(shard, allocation)) {
                return Optional.of(new ShardNodeAllocationDecision(shard, List.of()));
            }
            assert allocation.debugDecision() == false;
            // enable debug decisions to see all decisions and preserve the allocation decision label
            allocation.debugDecision(true);
            try {
                List<NodeDecision> nodeDecisions = nodesInTier(allocation.routingNodes()).map(
                    node -> new NodeDecision(node.node(), allocationDeciders.canAllocate(shard, node, allocation))
                ).toList();
                List<NodeDecision> diskOnly = nodeDecisions.stream()
                    .filter(nar -> ReactiveStorageDeciderService.isDiskOnlyNoDecision(nar.decision()))
                    .toList();
                if (diskOnly.size() > 0 && shard.unassigned() && shard.recoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS) {
                    // For resize shards only allow autoscaling if there is no other node where the shard could fit had it not been
                    // a resize shard. Notice that we already removed any initial_recovery filters.
                    boolean hasResizeOnly = nodesInTier(allocation.routingNodes()).map(
                        node -> allocationDeciders.canAllocate(shard, node, allocation)
                    ).anyMatch(ReactiveStorageDeciderService::isResizeOnlyNoDecision);
                    if (hasResizeOnly) {
                        return Optional.empty();
                    }
                }
                return diskOnly.size() > 0 ? Optional.of(new ShardNodeAllocationDecision(shard, nodeDecisions)) : Optional.empty();
            } finally {
                allocation.debugDecision(false);
            }
        }

        /**
         * Check that the disk decider is only decider that says NO to let shard remain on current node.
         * @return Non-empty {@link ShardNodeAllocationDecision} if and only if disk decider is only decider that says NO to canRemain.
         */
        private Optional<ShardNodeRemainDecision> cannotRemainDueToStorage(ShardRouting shard, RoutingAllocation allocation) {
            assert allocation.debugDecision() == false;
            // enable debug decisions to see all decisions and preserve the allocation decision label
            allocation.debugDecision(true);
            try {
                RoutingNode node = allocation.routingNodes().node(shard.currentNodeId());
                Decision decision = allocationDeciders.canRemain(shard, node, allocation);
                return isDiskOnlyNoDecision(decision)
                    ? Optional.of(new ShardNodeRemainDecision(shard, new NodeDecision(node.node(), decision)))
                    : Optional.empty();
            } finally {
                allocation.debugDecision(false);
            }
        }

        private boolean canAllocate(ShardRouting shard, RoutingAllocation allocation) {
            return nodesInTier(allocation.routingNodes()).anyMatch(
                node -> allocationDeciders.canAllocate(shard, node, allocation) != Decision.NO
            );
        }

        boolean needsThisTier(ShardRouting shard, RoutingAllocation allocation) {
            if (isAssignedToTier(shard, allocation) == false) {
                return false;
            }
            IndexMetadata indexMetadata = indexMetadata(shard, allocation);
            Set<Decision.Type> decisionTypes = allocation.routingNodes()
                .stream()
                .map(
                    node -> DataTierAllocationDecider.shouldFilter(
                        indexMetadata,
                        node.node(),
                        AllocationState::highestPreferenceTier,
                        allocation
                    )
                )
                .map(Decision::type)
                .collect(Collectors.toSet());
            if (decisionTypes.contains(Decision.Type.NO)) {
                // we know we have some filter and can respond. Only need this tier if ALL responses where NO.
                return decisionTypes.size() == 1;
            }

            // check for using allocation filters for data tiers. For simplicity, only scale up new tier based on primary shard
            if (shard.primary() == false) {
                return false;
            }
            assert allocation.debugDecision() == false;
            // enable debug decisions to see all decisions and preserve the allocation decision label
            allocation.debugDecision(true);
            try {
                // check that it does not belong on any existing node, i.e., there must be only a tier like reason it cannot be allocated
                return allocation.routingNodes()
                    .stream()
                    .anyMatch(node -> isFilterTierOnlyDecision(allocationDeciders.canAllocate(shard, node, allocation), indexMetadata));
            } finally {
                allocation.debugDecision(false);
            }
        }

        private boolean isAssignedToTier(ShardRouting shard, RoutingAllocation allocation) {
            IndexMetadata indexMetadata = indexMetadata(shard, allocation);
            return isAssignedToTier(indexMetadata, roles);
        }

        private static boolean isAssignedToTier(IndexMetadata indexMetadata, Set<DiscoveryNodeRole> roles) {
            List<String> tierPreference = indexMetadata.getTierPreference();
            return tierPreference.isEmpty() || DataTierAllocationDecider.allocationAllowed(highestPreferenceTier(tierPreference), roles);
        }

        private static IndexMetadata indexMetadata(ShardRouting shard, RoutingAllocation allocation) {
            return allocation.metadata().getProject().getIndexSafe(shard.index());
        }

        private static Optional<String> highestPreferenceTier(
            List<String> preferredTiers,
            DiscoveryNodes unused,
            DesiredNodes desiredNodes,
            NodesShutdownMetadata shutdownMetadata
        ) {
            return Optional.of(highestPreferenceTier(preferredTiers));
        }

        private static String highestPreferenceTier(List<String> preferredTiers) {
            assert preferredTiers.isEmpty() == false;
            return preferredTiers.get(0);
        }

        public long maxShardSize() {
            return nodesInTier(state.getRoutingNodes()).flatMap(rn -> StreamSupport.stream(rn.spliterator(), false))
                .mapToLong(this::sizeOf)
                .max()
                .orElse(0L);
        }

        public long maxNodeLockedSize() {
            Metadata metadata = originalState.getMetadata();
            return metadata.getProject().indices().values().stream().mapToLong(imd -> nodeLockedSize(imd, metadata)).max().orElse(0L);
        }

        private long nodeLockedSize(IndexMetadata indexMetadata, Metadata metadata) {
            if (isNodeLocked(indexMetadata)) {
                IndexRoutingTable indexRoutingTable = state.getRoutingTable().index(indexMetadata.getIndex());
                long sum = 0;
                for (int s = 0; s < indexMetadata.getNumberOfShards(); ++s) {
                    ShardRouting shard = indexRoutingTable.shard(s).primaryShard();
                    long size = sizeOf(shard);
                    sum += size;
                }
                if (indexMetadata.getResizeSourceIndex() != null) {
                    // since we only report the max size for an index, count a shrink/clone/split 2x if it is node locked.
                    sum = sum * 2;
                }
                return sum;
            } else {
                Index resizeSourceIndex = indexMetadata.getResizeSourceIndex();
                if (resizeSourceIndex != null) {
                    IndexMetadata sourceIndexMetadata = metadata.getProject().index(resizeSourceIndex);
                    // source indicators stay on the index even after started and also after source is deleted.
                    if (sourceIndexMetadata != null) {
                        // ResizeAllocationDecider only handles clone or split, do the same here.
                        if (indexMetadata.getNumberOfShards() >= sourceIndexMetadata.getNumberOfShards()) {
                            IndexRoutingTable indexRoutingTable = state.getRoutingTable().index(resizeSourceIndex);
                            long max = 0;
                            for (int s = 0; s < sourceIndexMetadata.getNumberOfShards(); ++s) {
                                ShardRouting shard = indexRoutingTable.shard(s).primaryShard();
                                long size = sizeOf(shard);
                                max = Math.max(max, size);
                            }

                            // 2x to account for the extra copy residing on the same node
                            return max * 2;
                        }
                    }
                }
            }

            return 0;
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
            final ProjectMetadata project = state.metadata().projectFor(shard.index());
            return ExpectedShardSizeEstimator.getExpectedShardSize(
                shard,
                0L,
                info,
                shardSizeInfo,
                project,
                state.globalRoutingTable().routingTable(project.id())
            );
        }

        long unmovableSize(String nodeId, Collection<ShardRouting> shards) {
            DiskUsage diskUsage = this.info.getNodeMostAvailableDiskUsages().get(nodeId);
            if (diskUsage == null) {
                // do not want to scale up then, since this should only happen when node has just joined (clearly edge case).
                return 0;
            }

            long threshold = diskThresholdSettings.getFreeBytesThresholdHighStage(ByteSizeValue.ofBytes(diskUsage.totalBytes())).getBytes();
            long missing = threshold - diskUsage.freeBytes();
            return Math.max(missing, shards.stream().mapToLong(this::sizeOf).min().orElseThrow());
        }

        Stream<RoutingNode> nodesInTier(RoutingNodes routingNodes) {
            return nodeIds.stream().map(routingNodes::node);
        }

        private static class SingleForecast {
            private final Map<IndexMetadata, Long> additionalIndices;
            private final DataStream updatedDataStream;

            private SingleForecast(Map<IndexMetadata, Long> additionalIndices, DataStream updatedDataStream) {
                this.additionalIndices = additionalIndices;
                this.updatedDataStream = updatedDataStream;
            }

            public void applyRouting(RoutingTable.Builder routing) {
                additionalIndices.keySet().forEach(indexMetadata -> routing.addAsNew(indexMetadata));
            }

            public void applyMetadata(Metadata.Builder metadataBuilder) {
                @FixForMultiProject
                final ProjectId projectId = ProjectId.DEFAULT;
                ProjectMetadata.Builder projectBuilder = metadataBuilder.getProject(projectId);
                if (projectBuilder == null) {
                    projectBuilder = ProjectMetadata.builder(projectId);
                    metadataBuilder.put(projectBuilder);
                }
                applyProjectMetadata(projectBuilder);
            }

            public void applyProjectMetadata(ProjectMetadata.Builder projectBuilder) {
                additionalIndices.keySet().forEach(imd -> projectBuilder.put(imd, false));
                projectBuilder.put(updatedDataStream);
            }

            public void applySize(Map<String, Long> builder, RoutingTable updatedRoutingTable) {
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
            DataStreamMetadata dataStreamMetadata = state.metadata().getProject().custom(DataStreamMetadata.TYPE);
            if (dataStreamMetadata == null) {
                return this;
            }
            List<SingleForecast> singleForecasts = dataStreamMetadata.dataStreams()
                .keySet()
                .stream()
                .map(state.metadata().getProject().getIndicesLookup()::get)
                .map(DataStream.class::cast)
                .map(ds -> forecast(state.metadata(), ds, forecastWindow, now))
                .filter(Objects::nonNull)
                .toList();
            if (singleForecasts.isEmpty()) {
                return this;
            }
            Metadata.Builder metadataBuilder = Metadata.builder(state.metadata());
            RoutingTable.Builder routingTableBuilder = RoutingTable.builder(shardRoutingRoleStrategy, state.routingTable());
            Map<String, Long> sizeBuilder = new HashMap<>();
            singleForecasts.forEach(p -> p.applyMetadata(metadataBuilder));
            singleForecasts.forEach(p -> p.applyRouting(routingTableBuilder));
            RoutingTable routingTable = routingTableBuilder.build();
            singleForecasts.forEach(p -> p.applySize(sizeBuilder, routingTable));
            ClusterState forecastClusterState = ClusterState.builder(state).metadata(metadataBuilder).routingTable(routingTable).build();
            ClusterInfo forecastInfo = new ExtendedClusterInfo(Collections.unmodifiableMap(sizeBuilder), AllocationState.this.info);

            return new AllocationState(
                forecastClusterState,
                allocationDeciders,
                shardRoutingRoleStrategy,
                diskThresholdSettings,
                forecastInfo,
                shardSizeInfo,
                nodes,
                roles
            );
        }

        private SingleForecast forecast(Metadata metadata, DataStream stream, long forecastWindow, long now) {
            List<Index> indices = stream.getIndices();
            if (dataStreamAllocatedToNodes(metadata, indices) == false) return null;
            long minCreationDate = Long.MAX_VALUE;
            long totalSize = 0;
            int count = 0;
            while (count < indices.size()) {
                ++count;
                IndexMetadata indexMetadata = metadata.getProject().index(indices.get(indices.size() - count));
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

            IndexMetadata writeIndex = metadata.getProject().index(stream.getWriteIndex());

            Map<IndexMetadata, Long> newIndices = new HashMap<>();
            for (int i = 0; i < numberNewIndices; ++i) {
                final String uuid = UUIDs.randomBase64UUID();
                final Tuple<String, Long> rolledDataStreamInfo = stream.unsafeNextWriteIndexAndGeneration(
                    state.metadata().getProject(),
                    stream.getDataComponent()
                );
                stream = stream.unsafeRollover(
                    new Index(rolledDataStreamInfo.v1(), uuid),
                    rolledDataStreamInfo.v2(),
                    null,
                    stream.getAutoShardingEvent()
                );

                // this unintentionally copies the in-sync allocation ids too. This has the fortunate effect of these indices
                // not being regarded new by the disk threshold decider, thereby respecting the low watermark threshold even for primaries.
                // This is highly desirable so fixing this to clear the in-sync allocation ids will require a more elaborate solution,
                // ensuring at least that when replicas are involved, we still respect the low watermark. This is therefore left as is
                // for now with the intention to fix in a follow-up.
                IndexMetadata newIndex = IndexMetadata.builder(writeIndex)
                    .index(stream.getWriteIndex().getName())
                    .settings(Settings.builder().put(writeIndex.getSettings()).put(IndexMetadata.SETTING_INDEX_UUID, uuid))
                    .build();
                long size = Math.min(avgSizeCeil, scaledTotalSize - (avgSizeCeil * i));
                assert size > 0;
                newIndices.put(newIndex, size);
            }

            return new SingleForecast(newIndices, stream);
        }

        /**
         * Check that at least one shard is on the set of nodes. If they are all unallocated, we do not want to make any prediction to not
         * hit the wrong policy.
         * @param indices the indices of the data stream, in original order from data stream meta.
         * @return true if the first allocated index is allocated only to the set of nodes.
         */
        private boolean dataStreamAllocatedToNodes(Metadata metadata, List<Index> indices) {
            for (int i = 0; i < indices.size(); ++i) {
                IndexMetadata indexMetadata = metadata.getProject().index(indices.get(indices.size() - i - 1));
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

        private static ClusterState removeNodeLockFilters(ClusterState state) {
            ClusterState.Builder builder = ClusterState.builder(state);
            builder.metadata(removeNodeLockFilters(state.metadata()));
            return builder.build();
        }

        private static Metadata removeNodeLockFilters(Metadata metadata) {
            @FixForMultiProject
            final ProjectMetadata updatedProject = removeNodeLockFilters(metadata.getProject());
            return Metadata.builder(metadata).put(updatedProject).build();
        }

        private static ProjectMetadata removeNodeLockFilters(ProjectMetadata project) {
            ProjectMetadata.Builder builder = ProjectMetadata.builder(project);
            project.stream()
                .filter(AllocationState::isNodeLocked)
                .map(AllocationState::removeNodeLockFilters)
                .forEach(imd -> builder.put(imd, false));
            return builder.build();
        }

        private static IndexMetadata removeNodeLockFilters(IndexMetadata indexMetadata) {
            Settings settings = indexMetadata.getSettings();
            settings = removeNodeLockFilters(settings, REMOVE_NODE_LOCKED_FILTER_INITIAL, indexMetadata.getInitialRecoveryFilters());
            settings = removeNodeLockFilters(settings, REMOVE_NODE_LOCKED_FILTER_REQUIRE, indexMetadata.requireFilters());
            settings = removeNodeLockFilters(settings, REMOVE_NODE_LOCKED_FILTER_INCLUDE, indexMetadata.includeFilters());
            return IndexMetadata.builder(indexMetadata).settings(settings).build();
        }

        private static Settings removeNodeLockFilters(Settings settings, Predicate<String> predicate, DiscoveryNodeFilters filters) {
            // only filter if it is a single node filter - otherwise removing it risks narrowing legal nodes for OR filters.
            if (filters != null && filters.isSingleNodeFilter()) {
                return settings.filter(predicate);
            } else {
                return settings;
            }
        }

        private static boolean isNodeLocked(IndexMetadata indexMetadata) {
            return isNodeLocked(indexMetadata.requireFilters())
                || isNodeLocked(indexMetadata.includeFilters())
                || isNodeLocked(indexMetadata.getInitialRecoveryFilters());
        }

        private static boolean isNodeLocked(DiscoveryNodeFilters filters) {
            return filters != null && filters.isSingleNodeFilter();
        }

        private static class ExtendedClusterInfo extends ClusterInfo {
            private final ClusterInfo delegate;

            private ExtendedClusterInfo(Map<String, Long> extraShardSizes, ClusterInfo info) {
                super(
                    info.getNodeLeastAvailableDiskUsages(),
                    info.getNodeMostAvailableDiskUsages(),
                    extraShardSizes,
                    Map.of(),
                    Map.of(),
                    Map.of(),
                    Map.of()
                );
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
            public Optional<Long> getShardDataSetSize(ShardId shardId) {
                return delegate.getShardDataSetSize(shardId);
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
            public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
                throw new UnsupportedOperationException();
            }
        }

    }

    public static class ReactiveReason implements AutoscalingDeciderResult.Reason {

        static final int MAX_AMOUNT_OF_SHARDS = 512;
        private static final TransportVersion SHARD_IDS_OUTPUT_VERSION = TransportVersions.V_8_4_0;
        private static final TransportVersion UNASSIGNED_NODE_DECISIONS_OUTPUT_VERSION = TransportVersions.V_8_9_X;

        private final String reason;
        private final long unassigned;
        private final long assigned;
        private final SortedSet<ShardId> unassignedShardIds;
        private final SortedSet<ShardId> assignedShardIds;
        private final Map<ShardId, NodeDecisions> unassignedNodeDecisions;
        private final Map<ShardId, NodeDecisions> assignedNodeDecisions;

        public ReactiveReason(String reason, long unassigned, long assigned) {
            this(reason, unassigned, Collections.emptySortedSet(), assigned, Collections.emptySortedSet(), Map.of(), Map.of());
        }

        ReactiveReason(
            String reason,
            long unassigned,
            SortedSet<ShardId> unassignedShardIds,
            long assigned,
            SortedSet<ShardId> assignedShardIds,
            Map<ShardId, NodeDecisions> unassignedNodeDecisions,
            Map<ShardId, NodeDecisions> assignedNodeDecisions
        ) {
            this.reason = reason;
            this.unassigned = unassigned;
            this.assigned = assigned;
            this.unassignedShardIds = unassignedShardIds;
            this.assignedShardIds = assignedShardIds;
            this.unassignedNodeDecisions = Objects.requireNonNull(unassignedNodeDecisions);
            this.assignedNodeDecisions = Objects.requireNonNull(assignedNodeDecisions);
        }

        public ReactiveReason(StreamInput in) throws IOException {
            this.reason = in.readString();
            this.unassigned = in.readLong();
            this.assigned = in.readLong();
            if (in.getTransportVersion().onOrAfter(SHARD_IDS_OUTPUT_VERSION)) {
                unassignedShardIds = Collections.unmodifiableSortedSet(new TreeSet<>(in.readCollectionAsSet(ShardId::new)));
                assignedShardIds = Collections.unmodifiableSortedSet(new TreeSet<>(in.readCollectionAsSet(ShardId::new)));
            } else {
                unassignedShardIds = Collections.emptySortedSet();
                assignedShardIds = Collections.emptySortedSet();
            }
            if (in.getTransportVersion().onOrAfter(UNASSIGNED_NODE_DECISIONS_OUTPUT_VERSION)) {
                unassignedNodeDecisions = in.readMap(ShardId::new, NodeDecisions::new);
                assignedNodeDecisions = in.readMap(ShardId::new, NodeDecisions::new);
            } else {
                unassignedNodeDecisions = Map.of();
                assignedNodeDecisions = Map.of();
            }
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

        public SortedSet<ShardId> unassignedShardIds() {
            return unassignedShardIds;
        }

        public SortedSet<ShardId> assignedShardIds() {
            return assignedShardIds;
        }

        public Map<ShardId, NodeDecisions> unassignedNodeDecisions() {
            return unassignedNodeDecisions;
        }

        public Map<ShardId, NodeDecisions> assignedNodeDecisions() {
            return assignedNodeDecisions;
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
            if (out.getTransportVersion().onOrAfter(SHARD_IDS_OUTPUT_VERSION)) {
                out.writeCollection(unassignedShardIds);
                out.writeCollection(assignedShardIds);
            }
            if (out.getTransportVersion().onOrAfter(UNASSIGNED_NODE_DECISIONS_OUTPUT_VERSION)) {
                out.writeMap(unassignedNodeDecisions);
                out.writeMap(assignedNodeDecisions);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("reason", reason);
            builder.field("unassigned", unassigned);
            builder.field("unassigned_shards", unassignedShardIds.stream().limit(MAX_AMOUNT_OF_SHARDS).toList());
            builder.field("unassigned_shards_count", unassignedShardIds.size());
            builder.field("assigned", assigned);
            builder.field("assigned_shards", assignedShardIds.stream().limit(MAX_AMOUNT_OF_SHARDS).toList());
            builder.field("assigned_shards_count", assignedShardIds.size());
            builder.xContentValuesMap(
                "unassigned_node_decisions",
                unassignedNodeDecisions.entrySet().stream().collect(toMap(e -> e.getKey().toString(), Map.Entry::getValue))
            );
            builder.xContentValuesMap(
                "assigned_node_decisions",
                assignedNodeDecisions.entrySet().stream().collect(toMap(e -> e.getKey().toString(), Map.Entry::getValue))
            );
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ReactiveReason that = (ReactiveReason) o;
            return unassigned == that.unassigned
                && assigned == that.assigned
                && reason.equals(that.reason)
                && unassignedShardIds.equals(that.unassignedShardIds)
                && assignedShardIds.equals(that.assignedShardIds)
                && Objects.equals(unassignedNodeDecisions, that.unassignedNodeDecisions)
                && Objects.equals(assignedNodeDecisions, that.assignedNodeDecisions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                reason,
                unassigned,
                assigned,
                unassignedShardIds,
                assignedShardIds,
                unassignedNodeDecisions,
                assignedNodeDecisions
            );
        }
    }
}
