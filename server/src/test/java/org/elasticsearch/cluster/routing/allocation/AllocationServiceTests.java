/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.elasticsearch.cluster.routing.RoutingNodesHelper.shardsWithState;
import static org.elasticsearch.cluster.routing.UnassignedInfo.AllocationStatus.DECIDERS_NO;
import static org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING;
import static org.elasticsearch.common.settings.ClusterSettings.createBuiltInClusterSettings;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class AllocationServiceTests extends ESTestCase {

    public void testFirstListElementsToCommaDelimitedStringReportsAllElementsIfShort() {
        List<String> strings = IntStream.range(0, between(0, 10)).mapToObj(i -> randomAlphaOfLength(10)).toList();
        assertAllElementsReported(strings, randomBoolean());
    }

    public void testFirstListElementsToCommaDelimitedStringReportsAllElementsIfDebugEnabled() {
        List<String> strings = IntStream.range(0, between(0, 100)).mapToObj(i -> randomAlphaOfLength(10)).toList();
        assertAllElementsReported(strings, true);
    }

    private void assertAllElementsReported(List<String> strings, boolean isDebugEnabled) {
        final String abbreviated = AllocationService.firstListElementsToCommaDelimitedString(strings, Function.identity(), isDebugEnabled);
        for (String string : strings) {
            assertThat(abbreviated, containsString(string));
        }
        assertThat(abbreviated, not(containsString("...")));
    }

    public void testFirstListElementsToCommaDelimitedStringReportsFirstElementsIfLong() {
        List<String> strings = IntStream.range(0, between(0, 100)).mapToObj(i -> randomAlphaOfLength(between(6, 10))).distinct().toList();
        final String abbreviated = AllocationService.firstListElementsToCommaDelimitedString(strings, Function.identity(), false);
        for (int i = 0; i < strings.size(); i++) {
            if (i < 10) {
                assertThat(abbreviated, containsString(strings.get(i)));
            } else {
                assertThat(abbreviated, not(containsString(strings.get(i))));
            }
        }

        if (strings.size() > 10) {
            assertThat(abbreviated, containsString("..."));
            assertThat(abbreviated, containsString("[" + strings.size() + " items in total]"));
        } else {
            assertThat(abbreviated, not(containsString("...")));
        }
    }

    public void testFirstListElementsToCommaDelimitedStringUsesFormatterNotToString() {
        List<String> strings = IntStream.range(0, between(1, 100)).mapToObj(i -> "original").toList();
        final String abbreviated = AllocationService.firstListElementsToCommaDelimitedString(strings, s -> "formatted", randomBoolean());
        assertThat(abbreviated, containsString("formatted"));
        assertThat(abbreviated, not(containsString("original")));
    }

    public void testAssignsPrimariesInPriorityOrderThenReplicas() {
        // throttle (incoming) recoveries in order to observe the order of operations, but do not throttle outgoing recoveries since
        // the effects of that depend on the earlier (random) allocations
        final Settings settings = Settings.builder()
            .put(CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(), 1)
            .put(CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 1)
            .put(CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), Integer.MAX_VALUE)
            .build();
        final ClusterSettings clusterSettings = createBuiltInClusterSettings(settings);
        final AllocationService allocationService = new AllocationService(
            new AllocationDeciders(
                Arrays.asList(new SameShardAllocationDecider(clusterSettings), new ThrottlingAllocationDecider(clusterSettings))
            ),
            new ShardsAllocator() {
                @Override
                public void allocate(RoutingAllocation allocation) {
                    // all primaries are handled by existing shards allocators in these tests; even the invalid allocator prevents shards
                    // from falling through to here
                    assertThat(allocation.routingNodes().unassigned().getNumPrimaries(), equalTo(0));
                }

                @Override
                public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                    return ShardAllocationDecision.NOT_TAKEN;
                }
            },
            new EmptyClusterInfoService(),
            EmptySnapshotsInfoService.INSTANCE,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );

        final String unrealisticAllocatorName = "unrealistic";
        final Map<String, ExistingShardsAllocator> allocatorMap = new HashMap<>();
        final TestGatewayAllocator testGatewayAllocator = new TestGatewayAllocator();
        allocatorMap.put(GatewayAllocator.ALLOCATOR_NAME, testGatewayAllocator);
        allocatorMap.put(unrealisticAllocatorName, new UnrealisticAllocator());
        allocationService.setExistingShardsAllocators(allocatorMap);

        final DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.add(DiscoveryNodeUtils.create("node1"));
        nodesBuilder.add(DiscoveryNodeUtils.create("node2"));
        nodesBuilder.add(DiscoveryNodeUtils.create("node3"));

        final Metadata.Builder metadata = Metadata.builder()
            // create 3 indices with different priorities. The high and low priority indices use the default allocator which (in this test)
            // does not allocate any replicas, whereas the medium priority one uses the unrealistic allocator which does allocate replicas
            .put(indexMetadata("highPriority", Settings.builder().put(IndexMetadata.SETTING_PRIORITY, 10)))
            .put(
                indexMetadata(
                    "mediumPriority",
                    Settings.builder()
                        .put(IndexMetadata.SETTING_PRIORITY, 5)
                        .put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey(), unrealisticAllocatorName)
                )
            )
            .put(indexMetadata("lowPriority", Settings.builder().put(IndexMetadata.SETTING_PRIORITY, 3)))

            // also create a 4th index with arbitrary priority and an invalid allocator that we expect to ignore
            .put(
                indexMetadata(
                    "invalid",
                    Settings.builder()
                        .put(IndexMetadata.SETTING_PRIORITY, between(0, 15))
                        .put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey(), "unknown")
                )
            );

        final RoutingTable.Builder routingTableBuilder = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsRecovery(metadata.get("highPriority"))
            .addAsRecovery(metadata.get("mediumPriority"))
            .addAsRecovery(metadata.get("lowPriority"))
            .addAsRecovery(metadata.get("invalid"));

        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(nodesBuilder)
            .metadata(metadata)
            .routingTable(routingTableBuilder.build())
            .build();

        // permit the testGatewayAllocator to allocate primaries to every node
        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (int i = 0; i < indexRoutingTable.size(); i++) {
                final ShardRouting primaryShard = indexRoutingTable.shard(i).primaryShard();
                for (DiscoveryNode node : clusterState.nodes()) {
                    testGatewayAllocator.addKnownAllocation(primaryShard.initialize(node.getId(), FAKE_IN_SYNC_ALLOCATION_ID, 0L));
                }
            }
        }

        final ClusterState reroutedState1 = rerouteAndStartShards(allocationService, clusterState);
        final RoutingTable routingTable1 = reroutedState1.routingTable();
        final RoutingNodes routingNodes1 = reroutedState1.getRoutingNodes();
        // the test harness only permits one recovery per node, so we must have allocated all the high-priority primaries and one of the
        // medium-priority ones
        assertThat(shardsWithState(routingNodes1, ShardRoutingState.INITIALIZING), empty());
        assertThat(shardsWithState(routingNodes1, ShardRoutingState.RELOCATING), empty());
        assertTrue(shardsWithState(routingNodes1, ShardRoutingState.STARTED).stream().allMatch(ShardRouting::primary));
        assertThat(routingTable1.index("highPriority").primaryShardsActive(), equalTo(2));
        assertThat(routingTable1.index("mediumPriority").primaryShardsActive(), equalTo(1));
        assertThat(routingTable1.index("lowPriority").shardsWithState(ShardRoutingState.STARTED), empty());
        assertThat(routingTable1.index("invalid").shardsWithState(ShardRoutingState.STARTED), empty());

        final ClusterState reroutedState2 = rerouteAndStartShards(allocationService, reroutedState1);
        final RoutingTable routingTable2 = reroutedState2.routingTable();
        final RoutingNodes routingNodes2 = reroutedState2.getRoutingNodes();
        // this reroute starts the one remaining medium-priority primary and both of the low-priority ones, but no replicas
        assertThat(shardsWithState(routingNodes2, ShardRoutingState.INITIALIZING), empty());
        assertThat(shardsWithState(routingNodes2, ShardRoutingState.RELOCATING), empty());
        assertTrue(shardsWithState(routingNodes2, ShardRoutingState.STARTED).stream().allMatch(ShardRouting::primary));
        assertTrue(routingTable2.index("highPriority").allPrimaryShardsActive());
        assertTrue(routingTable2.index("mediumPriority").allPrimaryShardsActive());
        assertTrue(routingTable2.index("lowPriority").allPrimaryShardsActive());
        assertThat(routingTable2.index("invalid").shardsWithState(ShardRoutingState.STARTED), empty());

        final ClusterState reroutedState3 = rerouteAndStartShards(allocationService, reroutedState2);
        final RoutingTable routingTable3 = reroutedState3.routingTable();
        final RoutingNodes routingNodes3 = reroutedState3.getRoutingNodes();
        // this reroute starts the two medium-priority replicas since their allocator permits this
        assertThat(shardsWithState(routingNodes3, ShardRoutingState.INITIALIZING), empty());
        assertThat(shardsWithState(routingNodes3, ShardRoutingState.RELOCATING), empty());
        assertTrue(routingTable3.index("highPriority").allPrimaryShardsActive());
        assertThat(routingTable3.index("mediumPriority").shardsWithState(ShardRoutingState.UNASSIGNED), empty());
        assertTrue(routingTable3.index("lowPriority").allPrimaryShardsActive());
        assertThat(routingTable3.index("invalid").shardsWithState(ShardRoutingState.STARTED), empty());
    }

    public void testExplainsNonAllocationOfShardWithUnknownAllocator() {
        final AllocationService allocationService = new AllocationService(
            null,
            null,
            null,
            null,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );
        allocationService.setExistingShardsAllocators(
            Collections.singletonMap(GatewayAllocator.ALLOCATOR_NAME, new TestGatewayAllocator())
        );

        final DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.add(DiscoveryNodeUtils.create("node1"));
        nodesBuilder.add(DiscoveryNodeUtils.create("node2"));

        final Metadata.Builder metadata = Metadata.builder()
            .put(
                indexMetadata(
                    "index",
                    Settings.builder().put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey(), "unknown")
                )
            );

        final RoutingTable.Builder routingTableBuilder = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsRecovery(metadata.get("index"));

        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(nodesBuilder)
            .metadata(metadata)
            .routingTable(routingTableBuilder.build())
            .build();

        final RoutingAllocation allocation = new RoutingAllocation(
            new AllocationDeciders(Collections.emptyList()),
            clusterState,
            ClusterInfo.EMPTY,
            null,
            0L
        );
        allocation.setDebugMode(randomBoolean() ? RoutingAllocation.DebugMode.ON : RoutingAllocation.DebugMode.EXCLUDE_YES_DECISIONS);

        final ShardAllocationDecision shardAllocationDecision = allocationService.explainShardAllocation(
            clusterState.routingTable().index("index").shard(0).primaryShard(),
            allocation
        );

        assertTrue(shardAllocationDecision.isDecisionTaken());
        assertThat(
            shardAllocationDecision.getAllocateDecision().getAllocationStatus(),
            equalTo(UnassignedInfo.AllocationStatus.NO_VALID_SHARD_COPY)
        );
        assertThat(shardAllocationDecision.getAllocateDecision().getAllocationDecision(), equalTo(AllocationDecision.NO_VALID_SHARD_COPY));
        assertThat(shardAllocationDecision.getAllocateDecision().getExplanation(), equalTo(Explanations.Allocation.NO_COPIES));

        for (NodeAllocationResult nodeAllocationResult : shardAllocationDecision.getAllocateDecision().nodeDecisions) {
            assertThat(nodeAllocationResult.getNodeDecision(), equalTo(AllocationDecision.NO));
            assertThat(nodeAllocationResult.getCanAllocateDecision().type(), equalTo(Decision.Type.NO));
            assertThat(nodeAllocationResult.getCanAllocateDecision().label(), equalTo("allocator_plugin"));
            assertThat(nodeAllocationResult.getCanAllocateDecision().getExplanation(), equalTo("""
                finding the previous copies of this shard requires an allocator called [unknown] but that allocator was not found; \
                perhaps the corresponding plugin is not installed"""));
        }
    }

    private static final String FAKE_IN_SYNC_ALLOCATION_ID = "_in_sync_"; // so we can allocate primaries anywhere

    private static IndexMetadata.Builder indexMetadata(String name, Settings.Builder settings) {
        return IndexMetadata.builder(name)
            .settings(settings(Version.CURRENT).put(settings.build()))
            .numberOfShards(2)
            .numberOfReplicas(1)
            .putInSyncAllocationIds(0, Collections.singleton(FAKE_IN_SYNC_ALLOCATION_ID))
            .putInSyncAllocationIds(1, Collections.singleton(FAKE_IN_SYNC_ALLOCATION_ID));
    }

    /**
     * Allocates shards to nodes regardless of whether there's already a shard copy there.
     */
    private static class UnrealisticAllocator implements ExistingShardsAllocator {

        @Override
        public void beforeAllocation(RoutingAllocation allocation) {}

        @Override
        public void afterPrimariesBeforeReplicas(RoutingAllocation allocation) {}

        @Override
        public void allocateUnassigned(
            ShardRouting shardRouting,
            RoutingAllocation allocation,
            UnassignedAllocationHandler unassignedAllocationHandler
        ) {
            final AllocateUnassignedDecision allocateUnassignedDecision = explainUnassignedShardAllocation(shardRouting, allocation);
            if (allocateUnassignedDecision.getAllocationDecision() == AllocationDecision.YES) {
                unassignedAllocationHandler.initialize(
                    allocateUnassignedDecision.getTargetNode().getId(),
                    shardRouting.primary() ? FAKE_IN_SYNC_ALLOCATION_ID : null,
                    0L,
                    allocation.changes()
                );
            } else {
                unassignedAllocationHandler.removeAndIgnore(allocateUnassignedDecision.getAllocationStatus(), allocation.changes());
            }
        }

        @Override
        public AllocateUnassignedDecision explainUnassignedShardAllocation(ShardRouting shardRouting, RoutingAllocation allocation) {
            boolean throttled = false;

            for (final RoutingNode routingNode : allocation.routingNodes()) {
                final Decision decision = allocation.deciders().canAllocate(shardRouting, routingNode, allocation);
                if (decision.type() == Decision.Type.YES) {
                    return AllocateUnassignedDecision.yes(routingNode.node(), null, null, false);
                } else {
                    if (shardRouting.index().getName().equals("mediumPriority")
                        && shardRouting.primary() == false
                        && decision.type() == Decision.Type.THROTTLE) {
                        allocation.deciders().canAllocate(shardRouting, routingNode, allocation);
                    }
                }

                throttled = throttled || decision.type() == Decision.Type.THROTTLE;
            }

            return throttled ? AllocateUnassignedDecision.throttle(null) : AllocateUnassignedDecision.no(DECIDERS_NO, null);
        }

        @Override
        public void cleanCaches() {}

        @Override
        public void applyStartedShards(List<ShardRouting> startedShards, RoutingAllocation allocation) {}

        @Override
        public void applyFailedShards(List<FailedShard> failedShards, RoutingAllocation allocation) {}

        @Override
        public int getNumberOfInFlightFetches() {
            return 0;
        }
    }

    private static ClusterState rerouteAndStartShards(final AllocationService allocationService, final ClusterState clusterState) {
        final ClusterState reroutedState = allocationService.reroute(clusterState, "test", ActionListener.noop());
        return allocationService.applyStartedShards(
            reroutedState,
            shardsWithState(reroutedState.getRoutingNodes(), ShardRoutingState.INITIALIZING)
        );
    }

}
