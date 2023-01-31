/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static org.hamcrest.Matchers.equalTo;

public class AllocationDecidersTests extends ESTestCase {

    public void testCallAllDecidersWhenThereAreNoNoDecisions() {
        var expectedDecision = switch (randomIntBetween(0, 2)) {
            case 0 -> Decision.YES;
            case 1 -> Decision.THROTTLE;
            case 2 -> Decision.single(Decision.Type.THROTTLE, "throttle with label", "explanation");
            default -> throw new AssertionError("Unexpected input");
        };
        var allDecisions = addDecisionsAndShuffle(expectedDecision, () -> Decision.YES);

        verifyDecidersCall(RoutingAllocation.DebugMode.OFF, allDecisions, allDecisions.size(), expectedDecision);
    }

    public void testExitsAfterFirstNoDecision() {
        var expectedDecision = randomBoolean() ? Decision.NO : Decision.single(Decision.Type.NO, "no with label", "explanation");
        var allDecisions = addDecisionsAndShuffle(expectedDecision, () -> randomFrom(Decision.YES, Decision.THROTTLE));
        var expectedCalls = allDecisions.indexOf(expectedDecision) + 1;

        verifyDecidersCall(RoutingAllocation.DebugMode.OFF, allDecisions, expectedCalls, expectedDecision);
    }

    public void testCollectsAllDecisionsForDebugModeOn() {
        var allDecisions = shuffledList(
            randomList(
                1,
                25,
                () -> randomFrom(
                    Decision.YES,
                    Decision.THROTTLE,
                    Decision.NO,
                    Decision.single(Decision.Type.NO, "no with label", "explanation")
                )
            )
        );
        var expectedDecision = allDecisions.stream()
            .collect(
                Collector.of(Decision.Multi::new, Decision.Multi::add, (a, b) -> { throw new AssertionError("should not be called"); })
            );

        verifyDecidersCall(RoutingAllocation.DebugMode.ON, allDecisions, allDecisions.size(), expectedDecision);
    }

    public void testCollectsNoAndThrottleDecisionsForDebugModeExcludeYesDecisions() {
        var allDecisions = shuffledList(
            randomList(
                1,
                25,
                () -> randomFrom(
                    Decision.YES,
                    Decision.THROTTLE,
                    Decision.NO,
                    Decision.single(Decision.Type.NO, "no with label", "explanation")
                )
            )
        );
        var expectedDecision = allDecisions.stream()
            .filter(decision -> decision.type() != Decision.Type.YES)
            .collect(
                Collector.of(Decision.Multi::new, Decision.Multi::add, (a, b) -> { throw new AssertionError("should not be called"); })
            );

        verifyDecidersCall(RoutingAllocation.DebugMode.EXCLUDE_YES_DECISIONS, allDecisions, allDecisions.size(), expectedDecision);
    }

    private static List<Decision> addDecisionsAndShuffle(Decision expected, Supplier<Decision> others) {
        var decisions = new ArrayList<Decision>();
        decisions.add(expected);
        decisions.addAll(randomList(1, 25, others));
        return shuffledList(decisions);
    }

    private void verifyDecidersCall(
        RoutingAllocation.DebugMode debugMode,
        List<Decision> decisions,
        int expectedAllocationDecidersCalls,
        Decision expectedDecision
    ) {
        IndexMetadata index = IndexMetadata.builder("index")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(index, false).build())
            .build();
        ShardRouting shardRouting = createShardRouting(index.getIndex());
        RoutingNode routingNode = RoutingNodesHelper.routingNode("node", null);
        DiscoveryNode discoveryNode = new DiscoveryNode("node", new TransportAddress(TransportAddress.META_ADDRESS, 0), Version.CURRENT);

        var decidersCalled = new int[] { 0 };
        var deciders = new AllocationDeciders(decisions.stream().map(decision -> new TestAllocationDecider(() -> {
            decidersCalled[0]++;
            return decision;
        })).toList());

        RoutingAllocation allocation = new RoutingAllocation(deciders, clusterState, null, null, 0L);
        allocation.setDebugMode(debugMode);

        var decision = switch (randomIntBetween(0, 9)) {
            case 0 -> deciders.canAllocate(shardRouting, allocation);
            case 1 -> deciders.canAllocate(shardRouting, routingNode, allocation);
            case 2 -> deciders.canAllocate(index, routingNode, allocation);
            case 3 -> deciders.canRebalance(allocation);
            case 4 -> deciders.canRebalance(shardRouting, allocation);
            case 5 -> deciders.canRemain(shardRouting, routingNode, allocation);
            case 6 -> deciders.shouldAutoExpandToNode(index, discoveryNode, allocation);
            case 7 -> deciders.canForceAllocatePrimary(shardRouting, routingNode, allocation);
            case 8 -> deciders.canForceAllocateDuringReplace(shardRouting, routingNode, allocation);
            case 9 -> deciders.canAllocateReplicaWhenThereIsRetentionLease(shardRouting, routingNode, allocation);
            default -> throw new AssertionError("Unexpected input");
        };

        assertThat(decision, equalTo(expectedDecision));
        assertThat(decidersCalled[0], equalTo(expectedAllocationDecidersCalls));
    }

    public void testGetForcedInitialShardAllocation() {
        var deciders = new AllocationDeciders(
            shuffledList(
                List.of(
                    new AnyNodeInitialShardAllocationDecider(),
                    new AnyNodeInitialShardAllocationDecider(),
                    new AnyNodeInitialShardAllocationDecider()
                )
            )
        );

        assertThat(
            deciders.getForcedInitialShardAllocationToNodes(createShardRouting(), createRoutingAllocation(deciders)),
            equalTo(Optional.empty())
        );
    }

    public void testGetForcedInitialShardAllocationToFixedNode() {
        var deciders = new AllocationDeciders(
            shuffledList(
                List.of(
                    new AnyNodeInitialShardAllocationDecider(),
                    new FixedNodesInitialShardAllocationDecider(Set.of("node-1", "node-2")),
                    new AnyNodeInitialShardAllocationDecider()
                )
            )
        );

        assertThat(
            deciders.getForcedInitialShardAllocationToNodes(createShardRouting(), createRoutingAllocation(deciders)),
            equalTo(Optional.of(Set.of("node-1", "node-2")))
        );
    }

    public void testGetForcedInitialShardAllocationToFixedNodeFromMultipleDeciders() {
        var deciders = new AllocationDeciders(
            shuffledList(
                List.of(
                    new AnyNodeInitialShardAllocationDecider(),
                    new FixedNodesInitialShardAllocationDecider(Set.of("node-1", "node-2")),
                    new FixedNodesInitialShardAllocationDecider(Set.of("node-2", "node-3")),
                    new AnyNodeInitialShardAllocationDecider()
                )
            )
        );

        assertThat(
            deciders.getForcedInitialShardAllocationToNodes(createShardRouting(), createRoutingAllocation(deciders)),
            equalTo(Optional.of(Set.of("node-2")))
        );
    }

    private static ShardRouting createShardRouting(Index index) {
        return ShardRouting.newUnassigned(
            new ShardId(index, 0),
            true,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "_message"),
            ShardRouting.Role.DEFAULT
        );
    }

    private static ShardRouting createShardRouting() {
        return createShardRouting(new Index("test", "testUUID"));
    }

    private static RoutingAllocation createRoutingAllocation(AllocationDeciders deciders) {
        return new RoutingAllocation(deciders, ClusterState.builder(new ClusterName("test")).build(), null, null, 0L);
    }

    private static final class AnyNodeInitialShardAllocationDecider extends AllocationDecider {

    }

    private static final class FixedNodesInitialShardAllocationDecider extends AllocationDecider {
        private final Set<String> initialNodeIds;

        private FixedNodesInitialShardAllocationDecider(Set<String> initialNodeIds) {
            this.initialNodeIds = initialNodeIds;
        }

        @Override
        public Optional<Set<String>> getForcedInitialShardAllocationToNodes(ShardRouting shardRouting, RoutingAllocation allocation) {
            return Optional.of(initialNodeIds);
        }
    }

    private static final class TestAllocationDecider extends AllocationDecider {

        private final Supplier<Decision> decision;

        private TestAllocationDecider(Supplier<Decision> decision) {
            this.decision = decision;
        }

        @Override
        public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
            return decision.get();
        }

        @Override
        public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            return decision.get();
        }

        @Override
        public Decision canAllocate(IndexMetadata indexMetadata, RoutingNode node, RoutingAllocation allocation) {
            return decision.get();
        }

        @Override
        public Decision canRebalance(RoutingAllocation allocation) {
            return decision.get();
        }

        @Override
        public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
            return decision.get();
        }

        @Override
        public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            return decision.get();
        }

        @Override
        public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
            return decision.get();
        }

        @Override
        public Decision canForceAllocatePrimary(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            return decision.get();
        }

        @Override
        public Decision canForceAllocateDuringReplace(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            return decision.get();
        }

        @Override
        public Decision canAllocateReplicaWhenThereIsRetentionLease(
            ShardRouting shardRouting,
            RoutingNode node,
            RoutingAllocation allocation
        ) {
            return decision.get();
        }
    }
}
