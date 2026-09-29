/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.TestDecisions;
import org.elasticsearch.cluster.routing.allocation.TestRoutingAllocationFactory;
import org.elasticsearch.core.Predicates;
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
import static org.hamcrest.Matchers.nullValue;

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

    public void testCheckAllDecidersBeforeReturningNotPreferred() {
        var allDecisions = generateDecisions(TestDecisions.NOT_PREFERRED, () -> randomFrom(Decision.YES, Decision.THROTTLE));
        var debugMode = randomFrom(RoutingAllocation.DebugMode.values());
        var expectedDecision = switch (debugMode) {
            case OFF -> allDecisions.contains(Decision.THROTTLE) ? Decision.THROTTLE : TestDecisions.NOT_PREFERRED;
            case EXCLUDE_YES_DECISIONS -> filterAndCollectToMultiDecision(allDecisions, d -> d.type() != Decision.Type.YES);
            case ON -> collectToMultiDecision(allDecisions);
        };

        verifyDecidersCall(debugMode, allDecisions, allDecisions.size(), expectedDecision);
    }

    public void testExitsAfterFirstNoDecision() {
        var expectedDecision = Decision.single(Decision.Type.NO, "no with label", "explanation");
        final var notPreferred = Decision.single(Decision.Type.NOT_PREFERRED, "test_decider", null);
        var allDecisions = generateDecisions(expectedDecision, () -> randomFrom(Decision.YES, notPreferred, Decision.THROTTLE));
        var expectedCalls = allDecisions.indexOf(expectedDecision) + 1;

        verifyDecidersCall(RoutingAllocation.DebugMode.OFF, allDecisions, expectedCalls, expectedDecision);
    }

    public void testCollectsAllDecisionsForDebugModeOn() {
        var allDecisions = generateDecisions(
            () -> randomFrom(
                Decision.YES,
                TestDecisions.NOT_PREFERRED,
                Decision.THROTTLE,
                Decision.single(Decision.Type.THROTTLE, "throttle with label", "explanation"),
                TestDecisions.NO
            )
        );
        var expectedDecision = collectToMultiDecision(allDecisions);

        verifyDecidersCall(RoutingAllocation.DebugMode.ON, allDecisions, allDecisions.size(), expectedDecision);
    }

    public void testCollectsNoAndThrottleDecisionsForDebugModeExcludeYesDecisions() {
        var allDecisions = generateDecisions(
            () -> randomFrom(
                Decision.YES,
                TestDecisions.NOT_PREFERRED,
                Decision.THROTTLE,
                Decision.single(Decision.Type.THROTTLE, "throttle with label", "explanation"),
                TestDecisions.NO
            )
        );
        var expectedDecision = filterAndCollectToMultiDecision(allDecisions, decision -> decision.type() != Decision.Type.YES);

        verifyDecidersCall(RoutingAllocation.DebugMode.EXCLUDE_YES_DECISIONS, allDecisions, allDecisions.size(), expectedDecision);
    }

    private static List<Decision> generateDecisions(Supplier<Decision> others) {
        return shuffledList(randomList(1, 25, others));
    }

    /**
     * Generate a list of decisions that include the 'mandatory' decision as well as a random number of decision types supplied by 'others'.
     */
    private static List<Decision> generateDecisions(Decision mandatory, Supplier<Decision> others) {
        var decisions = new ArrayList<Decision>();
        decisions.add(mandatory);
        decisions.addAll(randomList(1, 25, others));
        return shuffledList(decisions);
    }

    private static Decision.Multi collectToMultiDecision(List<Decision> decisions) {
        return filterAndCollectToMultiDecision(decisions, Predicates.always());
    }

    /**
     * Filters the 'decisions' list to only decisions matching 'filter'. Returns a Decision.Multi encompassing the resulting decisions.
     */
    private static Decision.Multi filterAndCollectToMultiDecision(List<Decision> decisions, Predicate<Decision> filter) {
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
        final RoutingTable projectRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(index)
            .build();
        final ProjectId projectId = randomProjectIdOrDefault();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(ProjectMetadata.builder(projectId).put(index, false)).build())
            .routingTable(GlobalRoutingTable.builder().put(projectId, projectRoutingTable).build())
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

            RoutingAllocation allocation = TestRoutingAllocationFactory.forClusterState(clusterState).allocationDeciders(deciders).build();
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

    // === canRemain label tests ===

    public void testCanRemainLabelYesDecision() {
        var decision = doCanRemain(new AllocationDeciders(List.of(new TestAllocationDecider(() -> Decision.YES))));
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(decision.label(), nullValue());
    }

    public void testCanRemainLabelNotPreferredDecision() {
        var decision = doCanRemain(
            new AllocationDeciders(List.of(new TestAllocationDecider(() -> Decision.YES), new FirstNotPreferredDecider()))
        );
        assertThat(decision.type(), equalTo(Decision.Type.NOT_PREFERRED));
        assertThat(decision.label(), equalTo(FirstNotPreferredDecider.NAME));
    }

    public void testCanRemainLabelNoDecision() {
        var decision = doCanRemain(new AllocationDeciders(List.of(new TestAllocationDecider(() -> Decision.YES), new NoDecider())));
        assertThat(decision.type(), equalTo(Decision.Type.NO));
        assertThat(decision.label(), equalTo(NoDecider.NAME));
    }

    public void testCanRemainLabelNoOverridesNotPreferred() {
        // When a NOT_PREFERRED is followed by a NO, the NO decision is returned with its label
        var decision = doCanRemain(new AllocationDeciders(List.of(new FirstNotPreferredDecider(), new NoDecider())));
        assertThat(decision.type(), equalTo(Decision.Type.NO));
        assertThat(decision.label(), equalTo(NoDecider.NAME));
    }

    public void testCanRemainLabelFirstNotPreferredWins() {
        // When multiple NOT_PREFERRED deciders, the first one's decision (and label) is returned
        var decision = doCanRemain(new AllocationDeciders(List.of(new FirstNotPreferredDecider(), new SecondNotPreferredDecider())));
        assertThat(decision.type(), equalTo(Decision.Type.NOT_PREFERRED));
        assertThat(decision.label(), equalTo(FirstNotPreferredDecider.NAME));
    }

    public void testCanRemainLabelThrottleDecision() {
        // THROTTLE from canRemain (e.g. HasFrozenCacheAllocationDecider when cache state is still fetching)
        // uses the shared THROTTLE constant, which has no label
        var decision = doCanRemain(new AllocationDeciders(List.of(new TestAllocationDecider(() -> Decision.THROTTLE))));
        assertThat(decision.type(), equalTo(Decision.Type.THROTTLE));
        assertThat(decision.label(), nullValue());
    }

    public void testCanRemainLabelThrottleOverridesNotPreferred() {
        // When NOT_PREFERRED is followed by THROTTLE, THROTTLE wins overall (it is more negative).
        // THROTTLE uses the shared constant so its label is null.
        var decision = doCanRemain(
            new AllocationDeciders(List.of(new FirstNotPreferredDecider(), new TestAllocationDecider(() -> Decision.THROTTLE)))
        );
        assertThat(decision.type(), equalTo(Decision.Type.THROTTLE));
        assertThat(decision.label(), nullValue());
    }

    public void testCanRemainLabelIgnoredShard() {
        // When the shard is ignored for the node, the NO decision comes from the AllocationDeciders
        // ignored-shard constant, which carries the label "ignored_shards_for_node"
        var deciders = new AllocationDeciders(List.of(new NoDecider()));
        IndexMetadata index = IndexMetadata.builder(randomIndexName()).settings(indexSettings(IndexVersion.current(), 1, 0)).build();
        ShardId shardId = new ShardId(index.getIndex(), 0);
        ProjectId projectId = randomProjectIdOrDefault();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(ProjectMetadata.builder(projectId).put(index, false)).build())
            .build();
        String currentNodeId = randomIdentifier();
        ShardRouting shard = TestShardRouting.newShardRouting(shardId, currentNodeId, true, ShardRoutingState.STARTED);
        RoutingNode routingNode = RoutingNodesHelper.routingNode(currentNodeId, null);
        RoutingAllocation allocation = TestRoutingAllocationFactory.forClusterState(clusterState).allocationDeciders(deciders).build();
        allocation.setDebugMode(RoutingAllocation.DebugMode.OFF);
        allocation.addIgnoreShardForNode(shardId, currentNodeId);

        var decision = deciders.canRemain(shard, routingNode, allocation);
        assertThat(decision.type(), equalTo(Decision.Type.NO));
        assertThat(decision.label(), equalTo("ignored_shards_for_node"));
    }

    private Decision doCanRemain(AllocationDeciders deciders) {
        IndexMetadata index = IndexMetadata.builder(randomIndexName()).settings(indexSettings(IndexVersion.current(), 1, 0)).build();
        ShardId shardId = new ShardId(index.getIndex(), 0);
        ProjectId projectId = randomProjectIdOrDefault();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(ProjectMetadata.builder(projectId).put(index, false)).build())
            .build();
        String currentNodeId = randomIdentifier();
        ShardRouting shard = TestShardRouting.newShardRouting(shardId, currentNodeId, true, ShardRoutingState.STARTED);
        RoutingNode routingNode = RoutingNodesHelper.routingNode(currentNodeId, null);
        RoutingAllocation allocation = TestRoutingAllocationFactory.forClusterState(clusterState).allocationDeciders(deciders).build();
        allocation.setDebugMode(RoutingAllocation.DebugMode.OFF);
        return deciders.canRemain(shard, routingNode, allocation);
    }

    // === canAllocate label tests ===

    public void testCanAllocateLabelYesDecision() {
        var decision = doCanAllocate(new AllocationDeciders(List.of(new TestAllocationDecider(() -> Decision.YES))));
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(decision.label(), nullValue());
    }

    public void testCanAllocateLabelThrottleOverridesNotPreferred() {
        // THROTTLE is more negative than NOT_PREFERRED; shared THROTTLE constant has no label
        var decision = doCanAllocate(
            new AllocationDeciders(List.of(new FirstNotPreferredDecider(), new TestAllocationDecider(() -> Decision.THROTTLE)))
        );
        assertThat(decision.type(), equalTo(Decision.Type.THROTTLE));
        assertThat(decision.label(), nullValue());
    }

    public void testCanAllocateLabelNoOverridesNotPreferred() {
        // Labeled NO from NoDecider overrides labeled NOT_PREFERRED from FirstNotPreferredDecider
        var decision = doCanAllocate(new AllocationDeciders(List.of(new FirstNotPreferredDecider(), new NoDecider())));
        assertThat(decision.type(), equalTo(Decision.Type.NO));
        assertThat(decision.label(), equalTo(NoDecider.NAME));
    }

    public void testCanAllocateLabelNotPreferredDecision() {
        var decision = doCanAllocate(new AllocationDeciders(List.of(new FirstNotPreferredDecider())));
        assertThat(decision.type(), equalTo(Decision.Type.NOT_PREFERRED));
        assertThat(decision.label(), equalTo(FirstNotPreferredDecider.NAME));
    }

    public void testCanAllocateLabelFirstNotPreferredWins() {
        // When multiple NOT_PREFERRED deciders, the first one's decision (and label) is returned
        var decision = doCanAllocate(new AllocationDeciders(List.of(new FirstNotPreferredDecider(), new SecondNotPreferredDecider())));
        assertThat(decision.type(), equalTo(Decision.Type.NOT_PREFERRED));
        assertThat(decision.label(), equalTo(FirstNotPreferredDecider.NAME));
    }

    private Decision doCanAllocate(AllocationDeciders deciders) {
        ShardRouting shard = createUnassignedShard();
        RoutingNode routingNode = RoutingNodesHelper.routingNode(randomIdentifier(), null);
        RoutingAllocation allocation = createRoutingAllocation(deciders);
        allocation.setDebugMode(RoutingAllocation.DebugMode.OFF);
        return deciders.canAllocate(shard, routingNode, allocation);
    }

    private static final class FirstNotPreferredDecider extends AllocationDecider {
        static final String NAME = "first_not_preferred";
        private static final Decision NOT_PREFERRED_DECISION = Decision.single(Decision.Type.NOT_PREFERRED, NAME, null);

        @Override
        public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            return NOT_PREFERRED_DECISION;
        }

        @Override
        public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            return NOT_PREFERRED_DECISION;
        }
    }

    private static final class SecondNotPreferredDecider extends AllocationDecider {
        static final String NAME = "second_not_preferred";
        private static final Decision NOT_PREFERRED_DECISION = Decision.single(Decision.Type.NOT_PREFERRED, NAME, null);

        @Override
        public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            return NOT_PREFERRED_DECISION;
        }

        @Override
        public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            return NOT_PREFERRED_DECISION;
        }
    }

    private static final class NoDecider extends AllocationDecider {
        static final String NAME = "no_decider";
        private static final Decision NO_DECISION = Decision.single(Decision.Type.NO, NAME, null);

        @Override
        public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            return NO_DECISION;
        }

        @Override
        public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            return NO_DECISION;
        }
    }

    private static ShardRouting createUnassignedShard(Index index) {
        return ShardRouting.newUnassigned(
            new ShardId(index, 0),
            true,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "_message"),
            ShardRouting.Role.DEFAULT,
            ShardRouting.RecoveryPriority.UNASSIGNED_NEW_PRIMARY
        );
    }

    private static ShardRouting createUnassignedShard() {
        return createUnassignedShard(new Index("test", "testUUID"));
    }

    private static RoutingAllocation createRoutingAllocation(AllocationDeciders deciders) {
        return TestRoutingAllocationFactory.forClusterState(ClusterState.builder(new ClusterName("test")).build())
            .allocationDeciders(deciders)
            .build();
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

}
