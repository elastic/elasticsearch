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
        var allDecisions = generateDecisions(Decision.NOT_PREFERRED, () -> randomFrom(Decision.YES, Decision.THROTTLE));
        var debugMode = randomFrom(RoutingAllocation.DebugMode.values());
        var expectedDecision = switch (debugMode) {
            case OFF -> allDecisions.contains(Decision.THROTTLE) ? Decision.THROTTLE : Decision.NOT_PREFERRED;
            case EXCLUDE_YES_DECISIONS -> filterAndCollectToMultiDecision(allDecisions, d -> d.type() != Decision.Type.YES);
            case ON -> collectToMultiDecision(allDecisions);
        };

        verifyDecidersCall(debugMode, allDecisions, allDecisions.size(), expectedDecision);
    }

    public void testExitsAfterFirstNoDecision() {
        var expectedDecision = randomFrom(Decision.NO, Decision.single(Decision.Type.NO, "no with label", "explanation"));
        var allDecisions = generateDecisions(expectedDecision, () -> randomFrom(Decision.YES, Decision.NOT_PREFERRED, Decision.THROTTLE));
        var expectedCalls = allDecisions.indexOf(expectedDecision) + 1;

        verifyDecidersCall(RoutingAllocation.DebugMode.OFF, allDecisions, expectedCalls, expectedDecision);
    }

    public void testCollectsAllDecisionsForDebugModeOn() {
        var allDecisions = generateDecisions(
            () -> randomFrom(
                Decision.YES,
                Decision.NOT_PREFERRED,
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
                Decision.NOT_PREFERRED,
                Decision.THROTTLE,
                Decision.single(Decision.Type.THROTTLE, "throttle with label", "explanation"),
                Decision.NO,
                Decision.single(Decision.Type.NO, "no with label", "explanation")
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

    // === canRemainWithDeciderName tests ===

    public void testCanRemainWithDeciderNameYesDecision() {
        var result = doCanRemainWithDeciderName(new AllocationDeciders(List.of(new TestAllocationDecider(() -> Decision.YES))));
        assertThat(result.decision().type(), equalTo(Decision.Type.YES));
        assertThat(result.deciderName(), nullValue());
    }

    public void testCanRemainWithDeciderNameNotPreferredDecision() {
        var result = doCanRemainWithDeciderName(
            new AllocationDeciders(List.of(new TestAllocationDecider(() -> Decision.YES), new FirstNotPreferredDecider()))
        );
        assertThat(result.decision().type(), equalTo(Decision.Type.NOT_PREFERRED));
        assertThat(result.deciderName(), equalTo(FirstNotPreferredDecider.class.getSimpleName()));
    }

    public void testCanRemainWithDeciderNameNoDecision() {
        var result = doCanRemainWithDeciderName(
            new AllocationDeciders(List.of(new TestAllocationDecider(() -> Decision.YES), new NoDecider()))
        );
        assertThat(result.decision().type(), equalTo(Decision.Type.NO));
        assertThat(result.deciderName(), equalTo(NoDecider.class.getSimpleName()));
    }

    public void testCanRemainWithDeciderNameNoOverridesNotPreferred() {
        // When a NOT_PREFERRED is followed by a NO, the NO decider's name is returned
        var result = doCanRemainWithDeciderName(new AllocationDeciders(List.of(new FirstNotPreferredDecider(), new NoDecider())));
        assertThat(result.decision().type(), equalTo(Decision.Type.NO));
        assertThat(result.deciderName(), equalTo(NoDecider.class.getSimpleName()));
    }

    public void testCanRemainWithDeciderNameFirstNotPreferredWins() {
        // When multiple NOT_PREFERRED deciders, only the first one's name is returned
        var result = doCanRemainWithDeciderName(
            new AllocationDeciders(List.of(new FirstNotPreferredDecider(), new SecondNotPreferredDecider()))
        );
        assertThat(result.decision().type(), equalTo(Decision.Type.NOT_PREFERRED));
        assertThat(result.deciderName(), equalTo(FirstNotPreferredDecider.class.getSimpleName()));
    }

    public void testCanRemainWithDeciderNameIgnoredShard() {
        // When the shard is ignored for the node, the NO decision comes from the ignored-shard check
        // (no individual decider callback fires), so deciderName is null even though the result is NO
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

        var result = deciders.canRemainWithDeciderName(shard, routingNode, allocation);
        assertThat(result.decision().type(), equalTo(Decision.Type.NO));
        assertThat(result.deciderName(), nullValue());
    }

    private AllocationDeciders.CanRemainWithDeciderName doCanRemainWithDeciderName(AllocationDeciders deciders) {
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
        return deciders.canRemainWithDeciderName(shard, routingNode, allocation);
    }

    // === canAllocateNotPreferredDeciderLabel tests ===

    public void testCanAllocateNotPreferredDeciderLabelYesDecision() {
        assertThat(
            doCanAllocateNotPreferredDeciderLabel(new AllocationDeciders(List.of(new TestAllocationDecider(() -> Decision.YES)))),
            nullValue()
        );
    }

    public void testCanAllocateNotPreferredDeciderLabelThrottleDecision() {
        // THROTTLE is more negative than NOT_PREFERRED, so it wins; label is null
        assertThat(
            doCanAllocateNotPreferredDeciderLabel(
                new AllocationDeciders(List.of(new FirstNotPreferredDecider(), new TestAllocationDecider(() -> Decision.THROTTLE)))
            ),
            nullValue()
        );
    }

    public void testCanAllocateNotPreferredDeciderLabelNoDecision() {
        // NO overrides NOT_PREFERRED; label is null even though NOT_PREFERRED was seen first
        assertThat(
            doCanAllocateNotPreferredDeciderLabel(
                new AllocationDeciders(List.of(new FirstNotPreferredDecider(), new TestAllocationDecider(() -> Decision.NO)))
            ),
            nullValue()
        );
    }

    public void testCanAllocateNotPreferredDeciderLabelNotPreferredDecision() {
        assertThat(
            doCanAllocateNotPreferredDeciderLabel(new AllocationDeciders(List.of(new FirstNotPreferredDecider()))),
            equalTo(FirstNotPreferredDecider.class.getSimpleName())
        );
    }

    public void testCanAllocateNotPreferredDeciderLabelFirstNotPreferredWins() {
        // When multiple NOT_PREFERRED deciders, only the first one's name is returned
        assertThat(
            doCanAllocateNotPreferredDeciderLabel(
                new AllocationDeciders(List.of(new FirstNotPreferredDecider(), new SecondNotPreferredDecider()))
            ),
            equalTo(FirstNotPreferredDecider.class.getSimpleName())
        );
    }

    private String doCanAllocateNotPreferredDeciderLabel(AllocationDeciders deciders) {
        ShardRouting shard = createUnassignedShard();
        RoutingNode routingNode = RoutingNodesHelper.routingNode(randomIdentifier(), null);
        RoutingAllocation allocation = createRoutingAllocation(deciders);
        allocation.setDebugMode(RoutingAllocation.DebugMode.OFF);
        return deciders.canAllocateNotPreferredDeciderName(shard, routingNode, allocation);
    }

    // === canForceAllocateDuringReplaceNotPreferredDeciderLabel tests ===

    public void testCanForceAllocateDuringReplaceNotPreferredDeciderLabelYesDecision() {
        assertThat(
            doCanForceAllocateDuringReplaceNotPreferredDeciderLabel(
                new AllocationDeciders(List.of(new TestAllocationDecider(() -> Decision.YES)))
            ),
            nullValue()
        );
    }

    public void testCanForceAllocateDuringReplaceNotPreferredDeciderLabelNotPreferredDecision() {
        assertThat(
            doCanForceAllocateDuringReplaceNotPreferredDeciderLabel(
                new AllocationDeciders(List.of(new TestAllocationDecider(() -> Decision.NOT_PREFERRED)))
            ),
            equalTo(TestAllocationDecider.class.getSimpleName())
        );
    }

    public void testCanForceAllocateDuringReplaceNotPreferredDeciderLabelNoDecision() {
        // NO overrides NOT_PREFERRED; label is null even though NOT_PREFERRED was seen first
        assertThat(
            doCanForceAllocateDuringReplaceNotPreferredDeciderLabel(
                new AllocationDeciders(List.of(new FirstNotPreferredDecider(), new TestAllocationDecider(() -> Decision.NO)))
            ),
            nullValue()
        );
    }

    public void testCanForceAllocateDuringReplaceNotPreferredDeciderLabelIgnoredShardNotBlocked() {
        // Unlike canAllocateNotPreferredDeciderLabel, this method uses withDeciders (no shard-ignored
        // check), so it identifies the responsible decider even when the shard is marked ignored for
        // the target node — which is the correct behaviour for the vacate path.
        ShardRouting shard = createUnassignedShard();
        RoutingNode routingNode = RoutingNodesHelper.routingNode(randomIdentifier(), null);
        AllocationDeciders deciders = new AllocationDeciders(List.of(new TestAllocationDecider(() -> Decision.NOT_PREFERRED)));
        RoutingAllocation allocation = createRoutingAllocation(deciders);
        allocation.setDebugMode(RoutingAllocation.DebugMode.OFF);
        allocation.addIgnoreShardForNode(shard.shardId(), routingNode.nodeId());

        // canAllocateNotPreferredDeciderLabel returns null: shard-ignored check short-circuits to NO
        assertThat(deciders.canAllocateNotPreferredDeciderName(shard, routingNode, allocation), nullValue());
        // canForceAllocateDuringReplaceNotPreferredDeciderLabel skips the ignored check
        assertThat(
            deciders.canForceAllocateDuringReplaceNotPreferredDeciderName(shard, routingNode, allocation),
            equalTo(TestAllocationDecider.class.getSimpleName())
        );
    }

    private String doCanForceAllocateDuringReplaceNotPreferredDeciderLabel(AllocationDeciders deciders) {
        ShardRouting shard = createUnassignedShard();
        RoutingNode routingNode = RoutingNodesHelper.routingNode(randomIdentifier(), null);
        RoutingAllocation allocation = createRoutingAllocation(deciders);
        allocation.setDebugMode(RoutingAllocation.DebugMode.OFF);
        return deciders.canForceAllocateDuringReplaceNotPreferredDeciderName(shard, routingNode, allocation);
    }

    private static final class FirstNotPreferredDecider extends AllocationDecider {
        @Override
        public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            return Decision.NOT_PREFERRED;
        }

        @Override
        public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            return Decision.NOT_PREFERRED;
        }
    }

    private static final class SecondNotPreferredDecider extends AllocationDecider {
        @Override
        public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            return Decision.NOT_PREFERRED;
        }

        @Override
        public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            return Decision.NOT_PREFERRED;
        }
    }

    private static final class NoDecider extends AllocationDecider {
        @Override
        public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            return Decision.NO;
        }

        @Override
        public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            return Decision.NO;
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
