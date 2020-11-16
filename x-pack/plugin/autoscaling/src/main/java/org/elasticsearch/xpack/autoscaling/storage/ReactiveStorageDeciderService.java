/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCapacity;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderService;

import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider.INDEX_ROUTING_EXCLUDE_SETTING;
import static org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider.INDEX_ROUTING_INCLUDE_SETTING;
import static org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider.INDEX_ROUTING_PREFER_SETTING;
import static org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider.INDEX_ROUTING_REQUIRE_SETTING;

public class ReactiveStorageDeciderService implements AutoscalingDeciderService<ReactiveStorageDeciderConfiguration> {
    public static final String NAME = "reactive_storage";

    private static final Logger logger = LogManager.getLogger(ReactiveStorageDeciderService.class);

    private static final Predicate<DiscoveryNodeRole> DATA_ROLE_PREDICATE = DiscoveryNode.getPossibleRoles()
        .stream()
        .filter(DiscoveryNodeRole::canContainData)
        .collect(Collectors.toSet())::contains;

    public ReactiveStorageDeciderService() {}

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public AutoscalingDeciderResult scale(ReactiveStorageDeciderConfiguration decider, AutoscalingDeciderContext context) {
        AutoscalingCapacity autoscalingCapacity = context.currentCapacity();
        if (autoscalingCapacity == null || autoscalingCapacity.tier().storage() == null) {
            return new AutoscalingDeciderResult(null, new ReactiveReason("current capacity not available"));
        }

        ClusterState state = simulateStartAndAllocate(context.state(), context);
        Predicate<IndexMetadata> indexTierPredicate = indexTierPredicate(context);
        Predicate<DiscoveryNode> nodeTierPredicate = context.nodes()::contains;

        AutoscalingCapacity plusOne = AutoscalingCapacity.builder()
            .total(autoscalingCapacity.tier().storage().getBytes() + 1, null)
            .build();
        if (storagePreventsAllocation(state, context, indexTierPredicate, nodeTierPredicate)) {
            return new AutoscalingDeciderResult(plusOne, new ReactiveReason("not enough storage available for unassigned shards"));
        } else if (storagePreventsRemainOrMove(state, context, indexTierPredicate, nodeTierPredicate)) {
            return new AutoscalingDeciderResult(plusOne, new ReactiveReason("not enough storage available for assigned shards"));
        } else {
            // the message here is tricky, since storage might not be OK, but in that case, increasing storage alone would not help since
            // other deciders prevents allocation/moving shards.
            AutoscalingCapacity ok = AutoscalingCapacity.builder().total(autoscalingCapacity.tier().storage(), null).build();
            return new AutoscalingDeciderResult(ok, new ReactiveReason("storage ok"));
        }
    }

    static boolean storagePreventsAllocation(
        ClusterState state,
        AutoscalingDeciderContext context,
        Predicate<IndexMetadata> indexTierPredicate,
        Predicate<DiscoveryNode> nodeTierPredicate
    ) {
        RoutingNodes routingNodes = new RoutingNodes(state, false);
        RoutingAllocation allocation = new RoutingAllocation(
            context.allocationDeciders(),
            routingNodes,
            state,
            context.info(),
            context.snapshotShardSizeInfo(),
            System.nanoTime()
        );
        Metadata metadata = state.metadata();
        return StreamSupport.stream(state.getRoutingNodes().unassigned().spliterator(), false)
            .filter(u -> indexTierPredicate.test(metadata.getIndexSafe(u.index())))
            .anyMatch(shard -> cannotAllocateDueToStorage(shard, allocation, context, nodeTierPredicate));
    }

    static boolean storagePreventsRemainOrMove(
        ClusterState state,
        AutoscalingDeciderContext context,
        Predicate<IndexMetadata> tierPredicate,
        Predicate<DiscoveryNode> nodeTierPredicate
    ) {
        RoutingNodes routingNodes = new RoutingNodes(state, false);
        RoutingAllocation allocation = new RoutingAllocation(
            context.allocationDeciders(),
            routingNodes,
            state,
            context.info(),
            context.snapshotShardSizeInfo(),
            System.nanoTime()
        );
        Metadata metadata = state.metadata();
        return state.getRoutingNodes()
            .shardsWithState(ShardRoutingState.STARTED)
            .stream()
            .filter(shard -> tierPredicate.test(metadata.getIndexSafe(shard.index())))
            .filter(
                shard -> context.allocationDeciders().canRemain(shard, routingNodes.node(shard.currentNodeId()), allocation) == Decision.NO
            )
            .filter(shard -> canAllocate(shard, allocation, context, nodeTierPredicate) == false)
            .anyMatch(
                shard -> cannotAllocateDueToStorage(shard, allocation, context, nodeTierPredicate)
                    || cannotRemainDueToStorage(shard, allocation, context)
            );
    }

    private static boolean canAllocate(
        ShardRouting shard,
        RoutingAllocation allocation,
        AutoscalingDeciderContext context,
        Predicate<DiscoveryNode> nodeTierPredicate
    ) {
        return nodesInTier(allocation.routingNodes(), nodeTierPredicate).anyMatch(
            node -> context.allocationDeciders().canAllocate(shard, node, allocation) != Decision.NO
        );
    }

    /**
     * Check that disk decider is only decider for a node preventing allocation of the shard.
     * @return true if and only if a node exists in the tier where only disk decider prevents allocation
     */
    private static boolean cannotAllocateDueToStorage(
        ShardRouting shard,
        RoutingAllocation allocation,
        AutoscalingDeciderContext context,
        Predicate<DiscoveryNode> nodeTierPredicate
    ) {
        assert allocation.debugDecision() == false;
        allocation.debugDecision(true);
        try {
            return nodesInTier(allocation.routingNodes(), nodeTierPredicate).map(
                node -> context.allocationDeciders().canAllocate(shard, node, allocation)
            ).anyMatch(ReactiveStorageDeciderService::isDiskOnlyNoDecision);
        } finally {
            allocation.debugDecision(false);
        }
    }

    /**
     * Check that the disk decider is only decider that says NO to let shard remain on current node.
     * @return true if and only if disk decider is only decider that says NO to canRemain.
     */
    private static boolean cannotRemainDueToStorage(ShardRouting shard, RoutingAllocation allocation, AutoscalingDeciderContext context) {
        assert allocation.debugDecision() == false;
        allocation.debugDecision(true);
        try {
            return isDiskOnlyNoDecision(
                context.allocationDeciders().canRemain(shard, allocation.routingNodes().node(shard.currentNodeId()), allocation)
            );
        } finally {
            allocation.debugDecision(false);
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

    static ClusterState simulateStartAndAllocate(ClusterState state, AutoscalingDeciderContext context) {
        while (true) {
            RoutingNodes routingNodes = new RoutingNodes(state, false);
            RoutingAllocation allocation = new RoutingAllocation(
                context.allocationDeciders(),
                routingNodes,
                state,
                context.info(),
                context.snapshotShardSizeInfo(),
                System.nanoTime()
            );

            List<ShardRouting> shards = allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING);
            // replicas before primaries, since replicas can be reinit'ed, resulting in a new ShardRouting instance.
            shards.stream()
                .filter(Predicate.not(ShardRouting::primary))
                .forEach(s -> { allocation.routingNodes().startShard(logger, s, allocation.changes()); });
            shards.stream()
                .filter(ShardRouting::primary)
                .forEach(s -> { allocation.routingNodes().startShard(logger, s, allocation.changes()); });
            context.shardsAllocator().allocate(allocation);
            ClusterState nextState = updateClusterState(state, allocation);

            if (nextState == state) {
                return state;
            } else {
                state = nextState;
            }
        }
    }

    static ClusterState updateClusterState(ClusterState oldState, RoutingAllocation allocation) {
        assert allocation.metadata() == oldState.metadata();
        if (allocation.routingNodesChanged() == false) {
            return oldState;
        }
        final RoutingTable oldRoutingTable = oldState.routingTable();
        final RoutingNodes newRoutingNodes = allocation.routingNodes();
        final RoutingTable newRoutingTable = new RoutingTable.Builder().updateNodes(oldRoutingTable.version(), newRoutingNodes).build();
        final Metadata newMetadata = allocation.updateMetadataWithRoutingChanges(newRoutingTable);
        assert newRoutingTable.validate(newMetadata); // validates the routing table is coherent with the cluster state metadata

        return ClusterState.builder(oldState).routingTable(newRoutingTable).metadata(newMetadata).build();
    }

    static Predicate<IndexMetadata> indexTierPredicate(AutoscalingDeciderContext context) {
        return imd -> belongsToTier(
            imd,
            context.roles().stream().filter(DATA_ROLE_PREDICATE).map(DiscoveryNodeRole::roleName).collect(Collectors.toSet())::contains
        );
    }

    private enum OpType {
        AND,
        OR
    }

    private static boolean belongsToTier(IndexMetadata imd, Predicate<String> dataRoles) {
        // some logic replication to DataTierAllcationDecider here, since we do not necessarily have a node.
        Settings indexSettings = imd.getSettings();
        String indexRequire = INDEX_ROUTING_REQUIRE_SETTING.get(indexSettings);
        String indexInclude = INDEX_ROUTING_INCLUDE_SETTING.get(indexSettings);
        String indexExclude = INDEX_ROUTING_EXCLUDE_SETTING.get(indexSettings);

        if (Strings.hasText(indexRequire)) {
            if (allocationAllowed(OpType.AND, indexRequire, dataRoles) == false) {
                return false;
            }
        }
        if (Strings.hasText(indexInclude)) {
            if (allocationAllowed(OpType.OR, indexInclude, dataRoles) == false) {
                return false;
            }
        }
        if (Strings.hasText(indexExclude)) {
            if (allocationAllowed(OpType.OR, indexExclude, dataRoles)) {
                return false;
            }
        }

        String tierPreference = INDEX_ROUTING_PREFER_SETTING.get(indexSettings);
        // we only take first preference to ensure we spin up new tiers as required.
        if (Strings.hasText(tierPreference)) {
            String tier = Strings.tokenizeToStringArray(tierPreference, ",")[0];
            if (allocationAllowed(OpType.AND, tier, dataRoles) == false) {
                return false;
            }
        }

        return true;
    }

    private static boolean allocationAllowed(OpType opType, String tierSetting, Predicate<String> dataRoles) {
        // minor logic replication to DataTierAllcationDecider here, since we do not necessarily have a node.
        String[] values = Strings.tokenizeToStringArray(tierSetting, ",");
        for (String value : values) {
            // generic "data" roles are considered to have all tiers
            if (dataRoles.test(DiscoveryNodeRole.DATA_ROLE.roleName()) || dataRoles.test(value)) {
                if (opType == OpType.OR) {
                    return true;
                }
            } else {
                if (opType == OpType.AND) {
                    return false;
                }
            }
        }
        if (opType == OpType.OR) {
            return false;
        } else {
            return true;
        }
    }

    public static class ReactiveReason implements AutoscalingDeciderResult.Reason {
        private String reason;

        public ReactiveReason(String reason) {
            this.reason = reason;
        }

        public ReactiveReason(StreamInput in) throws IOException {
            this.reason = in.readString();
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
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("reason", reason);
            builder.endObject();
            return builder;
        }
    }
}
