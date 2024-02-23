/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static org.hamcrest.Matchers.equalTo;

public class AllocationDecidersTests extends ESAllocationTestCase {

    public void testCheckAllDecidersBeforeReturningYes() {
        var allDecisions = generateDecisions(() -> Decision.YES);
        var debugMode = randomFrom(RoutingAllocation.DebugMode.values());
        var expectedDecision = switch (debugMode) {
            case OFF -> Decision.YES;
            case EXCLUDE_YES_DECISIONS -> new Decision.Multi();
            case ON -> collectToMultiDecision(allDecisions);
        };

        verifyDecidersCall(debugMode, allDecisions, allDecisions.size(), expectedDecision);
    }

    public void testCheckAllDecidersBeforeReturningThrottle() {
        var allDecisions = generateDecisions(Decision.THROTTLE, () -> Decision.YES);
        var debugMode = randomFrom(RoutingAllocation.DebugMode.values());
        var expectedDecision = switch (debugMode) {
            case OFF -> Decision.THROTTLE;
            case EXCLUDE_YES_DECISIONS -> new Decision.Multi().add(Decision.THROTTLE);
            case ON -> collectToMultiDecision(allDecisions);
        };

        verifyDecidersCall(debugMode, allDecisions, allDecisions.size(), expectedDecision);
    }

    public void testExitsAfterFirstNoDecision() {
        var expectedDecision = randomFrom(Decision.NO, Decision.single(Decision.Type.NO, "no with label", "explanation"));
        var allDecisions = generateDecisions(expectedDecision, () -> randomFrom(Decision.YES, Decision.THROTTLE));
        var expectedCalls = allDecisions.indexOf(expectedDecision) + 1;

        verifyDecidersCall(RoutingAllocation.DebugMode.OFF, allDecisions, expectedCalls, expectedDecision);
    }

    public void testCollectsAllDecisionsForDebugModeOn() {
        var allDecisions = generateDecisions(
            () -> randomFrom(
                Decision.YES,
                Decision.THROTTLE,
                Decision.single(Decision.Type.THROTTLE, "throttle with label", "explanation"),
                Decision.NO,
                Decision.single(Decision.Type.NO, "no with label", "explanation")
            )
        );
        var expectedDecision = collectToMultiDecision(allDecisions);

        verifyDecidersCall(RoutingAllocation.DebugMode.ON, allDecisions, allDecisions.size(), expectedDecision);
    }

    public void testCollectsNoAndThrottleDecisionsForDebugModeExcludeYesDecisions() {
        var allDecisions = generateDecisions(
            () -> randomFrom(
                Decision.YES,
                Decision.THROTTLE,
                Decision.single(Decision.Type.THROTTLE, "throttle with label", "explanation"),
                Decision.NO,
                Decision.single(Decision.Type.NO, "no with label", "explanation")
            )
        );
        var expectedDecision = collectToMultiDecision(allDecisions, decision -> decision.type() != Decision.Type.YES);

        verifyDecidersCall(RoutingAllocation.DebugMode.EXCLUDE_YES_DECISIONS, allDecisions, allDecisions.size(), expectedDecision);
    }

    private static List<Decision> generateDecisions(Supplier<Decision> others) {
        return shuffledList(randomList(1, 25, others));
    }

    private static List<Decision> generateDecisions(Decision mandatory, Supplier<Decision> others) {
        var decisions = new ArrayList<Decision>();
        decisions.add(mandatory);
        decisions.addAll(randomList(1, 25, others));
        return shuffledList(decisions);
    }

    private static Decision.Multi collectToMultiDecision(List<Decision> decisions) {
        return collectToMultiDecision(decisions, ignored -> true);
    }

    private static Decision.Multi collectToMultiDecision(List<Decision> decisions, Predicate<Decision> filter) {
        return decisions.stream().filter(filter).collect(Collector.of(Decision.Multi::new, Decision.Multi::add, (a, b) -> {
            throw new AssertionError("should not be called");
        }));
    }

    private void verifyDecidersCall(
        RoutingAllocation.DebugMode debugMode,
        List<Decision> decisions,
        int expectedAllocationDecidersCalls,
        Decision expectedDecision
    ) {
        IndexMetadata index = IndexMetadata.builder("index").settings(indexSettings(IndexVersion.current(), 1, 0)).build();
        ShardId shardId = new ShardId(index.getIndex(), 0);
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(index, false).build())
            .build();

        ShardRouting startedShard = TestShardRouting.newShardRouting(shardId, "node", true, ShardRoutingState.STARTED);
        ShardRouting unassignedShard = createUnassignedShard(index.getIndex());

        RoutingNode routingNode = RoutingNodesHelper.routingNode("node", null);
        DiscoveryNode discoveryNode = newNode("node");

        List.<BiFunction<RoutingAllocation, AllocationDeciders, Decision>>of(
            (allocation, deciders) -> deciders.canAllocate(unassignedShard, allocation),
            (allocation, deciders) -> deciders.canAllocate(unassignedShard, routingNode, allocation),
            (allocation, deciders) -> deciders.canAllocate(index, routingNode, allocation),
            (allocation, deciders) -> deciders.canRebalance(allocation),
            (allocation, deciders) -> deciders.canRebalance(startedShard, allocation),
            (allocation, deciders) -> deciders.canRemain(unassignedShard, routingNode, allocation),
            (allocation, deciders) -> deciders.shouldAutoExpandToNode(index, discoveryNode, allocation),
            (allocation, deciders) -> deciders.canForceAllocatePrimary(unassignedShard, routingNode, allocation),
            (allocation, deciders) -> deciders.canForceAllocateDuringReplace(unassignedShard, routingNode, allocation),
            (allocation, deciders) -> deciders.canAllocateReplicaWhenThereIsRetentionLease(unassignedShard, routingNode, allocation)
        ).forEach(operation -> {
            var decidersCalled = new int[] { 0 };
            var deciders = new AllocationDeciders(decisions.stream().map(decision -> new TestAllocationDecider(() -> {
                decidersCalled[0]++;
                return decision;
            })).toList());

            RoutingAllocation allocation = new RoutingAllocation(deciders, clusterState, null, null, 0L);
            allocation.setDebugMode(debugMode);

            var decision = operation.apply(allocation, deciders);

            assertThat(decision, equalTo(expectedDecision));
            assertThat(decidersCalled[0], equalTo(expectedAllocationDecidersCalls));
        });
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
            deciders.getForcedInitialShardAllocationToNodes(createUnassignedShard(), createRoutingAllocation(deciders)),
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
            deciders.getForcedInitialShardAllocationToNodes(createUnassignedShard(), createRoutingAllocation(deciders)),
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
            deciders.getForcedInitialShardAllocationToNodes(createUnassignedShard(), createRoutingAllocation(deciders)),
            equalTo(Optional.of(Set.of("node-2")))
        );
    }

    private static ShardRouting createUnassignedShard(Index index) {
        return ShardRouting.newUnassigned(
            new ShardId(index, 0),
            true,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "_message"),
            ShardRouting.Role.DEFAULT
        );
    }

    private static ShardRouting createUnassignedShard() {
        return createUnassignedShard(new Index("test", "testUUID"));
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
