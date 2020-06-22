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
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecider;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecision;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecisionType;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ReactiveStorageDecider implements AutoscalingDecider {
    public static final String NAME = "reactive_storage";

    private static final Logger logger = LogManager.getLogger(ReactiveStorageDecider.class);

    private static final ConstructingObjectParser<ReactiveStorageDecider, Void> PARSER;
    private static final ParseField TIER_ATTRIBUTE_FIELD = new ParseField("tier_attribute");
    private static final ParseField TIER_FIELD = new ParseField("tier");

    static {
        PARSER = new ConstructingObjectParser<>(NAME, a -> new ReactiveStorageDecider((String) a[0], (String) a[1]));
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TIER_ATTRIBUTE_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TIER_FIELD);
    }

    private final String tierAttribute;
    private final String tier;

    public ReactiveStorageDecider(String tierAttribute, String tier) {
        if ((tier == null || tierAttribute == null)) {
            throw new IllegalArgumentException("must specify both [tier_attribute] [" + tierAttribute + "] and [tier] [" + tier + "]");
        }
        this.tierAttribute = tierAttribute;
        this.tier = tier;
    }

    public ReactiveStorageDecider(StreamInput in) throws IOException {
        this(in.readString(), in.readString());
    }

    public static ReactiveStorageDecider parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public String getTierAttribute() {
        return tierAttribute;
    }

    public String getTier() {
        return tier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReactiveStorageDecider that = (ReactiveStorageDecider) o;
        return tierAttribute.equals(that.tierAttribute) && tier.equals(that.tier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tierAttribute, tier);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(tierAttribute);
        out.writeString(tier);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(TIER_ATTRIBUTE_FIELD.getPreferredName(), tierAttribute);
            builder.field(TIER_FIELD.getPreferredName(), tier);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public AutoscalingDecision scale(AutoscalingDeciderContext context) {
        ClusterState state = simulateStartAndAllocate(context.state(), context);
        Predicate<IndexMetadata> indexTierPredicate =
            // we check the specific attribute to not match indices with no tier spec.
            imd -> tier.equals(imd.getSettings().get(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + tierAttribute));
        Predicate<DiscoveryNode> nodeTierPredicate = n -> tier.equals(n.getAttributes().get(tierAttribute));

        if (storagePreventsAllocation(state, context, indexTierPredicate, nodeTierPredicate)) {
            return new AutoscalingDecision(NAME, AutoscalingDecisionType.SCALE_UP, "not enough storage available for unassigned shards");
        } else if (storagePreventsRemainOrMove(state, context, indexTierPredicate, nodeTierPredicate)) {
            return new AutoscalingDecision(NAME, AutoscalingDecisionType.SCALE_UP, "not enough storage available for assigned shards");
        } else {
            // the message here is tricky, since storage might not be OK, but in that case, increasing storage alone would not help since
            // other deciders prevents allocation/moving shards.
            return new AutoscalingDecision(NAME, AutoscalingDecisionType.NO_SCALE, "storage ok");
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
     * @return true iff a node exists in the tier where only disk decider prevents allocation
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
            ).anyMatch(ReactiveStorageDecider::isDiskOnlyNoDecision);
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
        return nos.size() == 1 && DiskThresholdDecider.NAME.equals(nos.get(0).label()) && nos.get(0).type() == Decision.Type.NO;

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
}
