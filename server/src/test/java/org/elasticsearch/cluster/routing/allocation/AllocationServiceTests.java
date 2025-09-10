/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.GlobalRoutingTableTestHelper;
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
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.elasticsearch.cluster.routing.RoutingNodesHelper.shardsWithState;
import static org.elasticsearch.cluster.routing.UnassignedInfo.AllocationStatus.DECIDERS_NO;
import static org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING;
import static org.elasticsearch.common.settings.ClusterSettings.createBuiltInClusterSettings;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

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
        final int numberOfProjects = randomIntBetween(1, 5);
        final List<ProjectMetadata.Builder> projects = numberOfProjects == 1
            ? List.of(ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID))
            : IntStream.range(0, numberOfProjects)
                .mapToObj(n -> ProjectMetadata.builder(ProjectId.fromId(randomUUID() + "-" + n)))
                .toList();

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

        // create 3 indices with different priorities. The high and low priority indices use the default allocator which (in this test)
        // does not allocate any replicas, whereas the medium priority one uses the unrealistic allocator which does allocate replicas
        // Each index is assigned to a random project (because the only thing that should matter is the priority)
        randomFrom(projects).put(indexMetadata("highPriority", Settings.builder().put(IndexMetadata.SETTING_PRIORITY, 10)));
        randomFrom(projects).put(
            indexMetadata(
                "mediumPriority",
                Settings.builder()
                    .put(IndexMetadata.SETTING_PRIORITY, 5)
                    .put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey(), unrealisticAllocatorName)
            )
        );
        randomFrom(projects).put(indexMetadata("lowPriority", Settings.builder().put(IndexMetadata.SETTING_PRIORITY, 3)));
        // also create a 4th index with arbitrary priority and an invalid allocator that we expect to ignore
        randomFrom(projects).put(
            indexMetadata(
                "invalid",
                Settings.builder()
                    .put(IndexMetadata.SETTING_PRIORITY, between(0, 15))
                    .put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey(), "unknown")
            )
        );

        final var metadataBuilder = Metadata.builder();
        projects.forEach(metadataBuilder::put);
        final var metadata = metadataBuilder.build();

        final GlobalRoutingTable origRoutingTable = GlobalRoutingTableTestHelper.buildRoutingTable(
            metadata,
            RoutingTable.Builder::addAsRecovery
        );
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(nodesBuilder)
            .metadata(metadata)
            .routingTable(origRoutingTable)
            .build();

        // permit the testGatewayAllocator to allocate primaries to every node
        for (IndexRoutingTable indexRoutingTable : clusterState.globalRoutingTable().indexRouting()) {
            for (int i = 0; i < indexRoutingTable.size(); i++) {
                final ShardRouting primaryShard = indexRoutingTable.shard(i).primaryShard();
                for (DiscoveryNode node : clusterState.nodes()) {
                    testGatewayAllocator.addKnownAllocation(primaryShard.initialize(node.getId(), FAKE_IN_SYNC_ALLOCATION_ID, 0L));
                }
            }
        }

        BiFunction<GlobalRoutingTable, String, IndexRoutingTable> findIndex = (routingTable, indexName) -> {
            for (IndexRoutingTable irt : routingTable.indexRouting()) {
                if (irt.getIndex().getName().equals(indexName)) {
                    return irt;
                }
            }
            throw new IndexNotFoundException("Cannot find index " + indexName + " in routing table");
        };

        final ClusterState reroutedState1 = rerouteAndStartShards(allocationService, clusterState);
        final GlobalRoutingTable routingTable1 = reroutedState1.globalRoutingTable();
        final RoutingNodes routingNodes1 = reroutedState1.getRoutingNodes();
        // the test harness only permits one recovery per node, so we must have allocated all the high-priority primaries and one of the
        // medium-priority ones
        assertThat(shardsWithState(routingNodes1, ShardRoutingState.INITIALIZING), empty());
        assertThat(shardsWithState(routingNodes1, ShardRoutingState.RELOCATING), empty());
        assertTrue(shardsWithState(routingNodes1, ShardRoutingState.STARTED).stream().allMatch(ShardRouting::primary));
        assertThat(findIndex.apply(routingTable1, "highPriority").primaryShardsActive(), equalTo(2));
        assertThat(findIndex.apply(routingTable1, "mediumPriority").primaryShardsActive(), equalTo(1));
        assertThat(findIndex.apply(routingTable1, "lowPriority").shardsWithState(ShardRoutingState.STARTED), empty());
        assertThat(findIndex.apply(routingTable1, "invalid").shardsWithState(ShardRoutingState.STARTED), empty());

        final ClusterState reroutedState2 = rerouteAndStartShards(allocationService, reroutedState1);
        final GlobalRoutingTable routingTable2 = reroutedState2.globalRoutingTable();
        final RoutingNodes routingNodes2 = reroutedState2.getRoutingNodes();
        // this reroute starts the one remaining medium-priority primary and both of the low-priority ones, but no replicas
        assertThat(shardsWithState(routingNodes2, ShardRoutingState.INITIALIZING), empty());
        assertThat(shardsWithState(routingNodes2, ShardRoutingState.RELOCATING), empty());
        assertTrue(shardsWithState(routingNodes2, ShardRoutingState.STARTED).stream().allMatch(ShardRouting::primary));
        assertTrue(findIndex.apply(routingTable2, "highPriority").allPrimaryShardsActive());
        assertTrue(findIndex.apply(routingTable2, "mediumPriority").allPrimaryShardsActive());
        assertTrue(findIndex.apply(routingTable2, "lowPriority").allPrimaryShardsActive());
        assertThat(findIndex.apply(routingTable2, "invalid").shardsWithState(ShardRoutingState.STARTED), empty());

        final ClusterState reroutedState3 = rerouteAndStartShards(allocationService, reroutedState2);
        final GlobalRoutingTable routingTable3 = reroutedState3.globalRoutingTable();
        final RoutingNodes routingNodes3 = reroutedState3.getRoutingNodes();
        // this reroute starts the two medium-priority replicas since their allocator permits this
        assertThat(shardsWithState(routingNodes3, ShardRoutingState.INITIALIZING), empty());
        assertThat(shardsWithState(routingNodes3, ShardRoutingState.RELOCATING), empty());
        assertTrue(findIndex.apply(routingTable3, "highPriority").allPrimaryShardsActive());
        assertThat(findIndex.apply(routingTable3, "mediumPriority").shardsWithState(ShardRoutingState.UNASSIGNED), empty());
        assertTrue(findIndex.apply(routingTable3, "lowPriority").allPrimaryShardsActive());
        assertThat(findIndex.apply(routingTable3, "invalid").shardsWithState(ShardRoutingState.STARTED), empty());
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

        var projectId = Metadata.DEFAULT_PROJECT_ID;
        final ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(projectId)
            .put(
                indexMetadata(
                    "index",
                    Settings.builder().put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey(), "unknown")
                )
            );

        final RoutingTable.Builder routingTableBuilder = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsRecovery(projectBuilder.get("index"));

        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(nodesBuilder)
            .metadata(Metadata.builder().put(projectBuilder).build())
            .routingTable(routingTableBuilder.build())
            .build();

        assertThat(clusterState.metadata().projects(), aMapWithSize(1));

        final RoutingAllocation allocation = new RoutingAllocation(
            new AllocationDeciders(Collections.emptyList()),
            clusterState,
            ClusterInfo.EMPTY,
            null,
            0L
        );
        allocation.setDebugMode(randomBoolean() ? RoutingAllocation.DebugMode.ON : RoutingAllocation.DebugMode.EXCLUDE_YES_DECISIONS);

        final ShardAllocationDecision shardAllocationDecision = allocationService.explainShardAllocation(
            clusterState.globalRoutingTable().routingTable(projectId).index("index").shard(0).primaryShard(),
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

    public void testHealthStatusWithMultipleProjects() {
        final Supplier<ProjectMetadata> buildProject = () -> {
            final ProjectMetadata.Builder builder = ProjectMetadata.builder(randomUniqueProjectId());
            final Set<String> indices = randomSet(1, 8, () -> randomAlphaOfLengthBetween(3, 12));
            indices.forEach(
                indexName -> builder.put(
                    IndexMetadata.builder(indexName)
                        .settings(indexSettings(IndexVersion.current(), 1, 1).put(IndexMetadata.SETTING_INDEX_UUID, randomUUID()))
                )
            );
            return builder.build();
        };
        final ProjectMetadata project1 = buildProject.get();
        final ProjectMetadata project2 = buildProject.get();
        final ProjectMetadata project3 = buildProject.get();

        final Function<GlobalRoutingTable, ClusterState> buildClusterState = routing -> ClusterState.builder(
            new ClusterName(randomAlphaOfLength(8))
        ).metadata(Metadata.builder().put(project1).put(project2).put(project3).build()).routingTable(routing).build();

        // Test a cluster state in "green" health - all shards active, no blocks
        final GlobalRoutingTable.Builder greenRoutingBuilder = GlobalRoutingTable.builder();
        for (ProjectMetadata project : List.of(project1, project2, project3)) {
            final RoutingTable.Builder builder = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);
            project.indices().values().forEach(indexMetadata -> builder.add(IndexRoutingTable.builder(indexMetadata.getIndex()).build()));
            greenRoutingBuilder.put(project.id(), builder.build());
        }
        final ClusterState green = buildClusterState.apply(greenRoutingBuilder.build());
        assertThat(AllocationService.getHealthStatus(green), is(ClusterHealthStatus.GREEN));

        // Test a cluster state in "yellow" health - shards for 1 of the projects are new (not yet allocated)
        final GlobalRoutingTable.Builder yellowRoutingBuilder = GlobalRoutingTable.builder(green.globalRoutingTable());
        final ProjectMetadata randomProject = randomFrom(project1, project2, project3);
        final RoutingTable.Builder projectRoutingbuilder = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);
        randomProject.indices().values().forEach(projectRoutingbuilder::addAsNew);
        yellowRoutingBuilder.put(randomProject.id(), projectRoutingbuilder.build());
        final ClusterState yellow = buildClusterState.apply(yellowRoutingBuilder.build());
        assertThat(AllocationService.getHealthStatus(yellow), is(ClusterHealthStatus.YELLOW));

        // Test a cluster state in "red" health - cluster state not yet recovered
        final ClusterState red = ClusterState.builder(randomFrom(green, yellow))
            .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK))
            .build();
        assertThat(AllocationService.getHealthStatus(red), is(ClusterHealthStatus.RED));
    }

    public void testAutoExpandReplicas() throws Exception {
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
            null,
            new EmptyClusterInfoService(),
            EmptySnapshotsInfoService.INSTANCE,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );

        final ProjectId project1 = randomUniqueProjectId();
        final var project2 = randomUniqueProjectId();
        final var project3 = randomUniqueProjectId();

        // return same cluster state when there are no changes
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(
                ProjectMetadata.builder(project1).put(IndexMetadata.builder("index").settings(indexSettings(IndexVersion.current(), 1, 1)))
            )
            .build();
        assertThat(allocationService.adaptAutoExpandReplicas(state), sameInstance(state));

        final DiscoveryNode node1 = DiscoveryNodeUtils.create("n1");
        final DiscoveryNode node2 = DiscoveryNodeUtils.create("n2");
        final DiscoveryNode node3 = DiscoveryNodeUtils.create("n3");

        final DiscoveryNodes singleNode = DiscoveryNodes.builder().add(node1).build();
        final DiscoveryNodes twoNodes = DiscoveryNodes.builder().add(node1).add(node2).build();
        final DiscoveryNodes threeNodes = DiscoveryNodes.builder().add(node1).add(node2).add(node3).build();

        // single project, 1 index with auto-expand-replicas
        state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(singleNode)
            .putProjectMetadata(
                ProjectMetadata.builder(project1)
                    .put(
                        IndexMetadata.builder("index")
                            .settings(
                                settings(IndexVersion.current()).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                                    .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
                            )
                    )
            )
            .build();
        // 1 node == 0 replicas == no change
        ClusterState expanded = allocationService.adaptAutoExpandReplicas(state);
        assertThat(expanded, sameInstance(state));
        assertThat(expanded.metadata().getProject(project1).index("index").getNumberOfReplicas(), is(0));

        // 2 nodes == 1 replica == changed state
        state = ClusterState.builder(state).nodes(twoNodes).build();
        expanded = allocationService.adaptAutoExpandReplicas(state);
        assertThat(expanded, not(sameInstance(state)));
        assertThat(expanded.metadata().getProject(project1).index("index").getNumberOfReplicas(), is(1));

        // 3 nodes == 1 replica == no change
        state = ClusterState.builder(expanded).nodes(threeNodes).build();
        expanded = allocationService.adaptAutoExpandReplicas(state);
        assertThat(expanded.metadata().getProject(project1).index("index").getNumberOfReplicas(), is(1));
        assertThat(expanded, sameInstance(state));

        // multiple project, multiple indices, some with auto-expand-replicas
        state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(singleNode)
            .putProjectMetadata(
                ProjectMetadata.builder(project1)
                    .put(
                        IndexMetadata.builder("expand-1")
                            .settings(
                                settings(IndexVersion.current()).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                                    .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
                            )
                    )
                    .put(IndexMetadata.builder("regular").settings(indexSettings(IndexVersion.current(), 1, 1)))
            )
            .putProjectMetadata(
                ProjectMetadata.builder(project2)
                    .put(IndexMetadata.builder("regular").settings(indexSettings(IndexVersion.current(), 1, 1)))
                    .put(
                        IndexMetadata.builder("expand-all")
                            .settings(
                                settings(IndexVersion.current()).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                                    .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-all")
                            )
                    )
                    .put(
                        IndexMetadata.builder("expand-1")
                            .settings(
                                settings(IndexVersion.current()).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                                    .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
                            )
                    )
            )
            .putProjectMetadata(
                ProjectMetadata.builder(project3)
                    .put(IndexMetadata.builder("regular").settings(indexSettings(IndexVersion.current(), 1, 1)))
            )
            .build();

        // 1 node == 0 replicas == no change
        expanded = allocationService.adaptAutoExpandReplicas(state);
        assertThat(expanded, sameInstance(state));
        assertThat(expanded.metadata().getProject(project1).index("expand-1").getNumberOfReplicas(), is(0));
        assertThat(expanded.metadata().getProject(project2).index("expand-1").getNumberOfReplicas(), is(0));
        assertThat(expanded.metadata().getProject(project2).index("expand-all").getNumberOfReplicas(), is(0));

        // 2 nodes == 1 replica == change
        state = ClusterState.builder(expanded).nodes(twoNodes).build();
        expanded = allocationService.adaptAutoExpandReplicas(state);
        assertThat(expanded, not(sameInstance(state)));
        assertThat(expanded.metadata().getProject(project1).index("expand-1").getNumberOfReplicas(), is(1));
        assertThat(expanded.metadata().getProject(project2).index("expand-1").getNumberOfReplicas(), is(1));
        assertThat(expanded.metadata().getProject(project2).index("expand-all").getNumberOfReplicas(), is(1));

        // 3 nodes == 1 or 2 replicas == change
        state = ClusterState.builder(expanded).nodes(threeNodes).build();
        expanded = allocationService.adaptAutoExpandReplicas(state);
        assertThat(expanded, not(sameInstance(state)));
        assertThat(expanded.metadata().getProject(project1).index("expand-1").getNumberOfReplicas(), is(1));
        assertThat(expanded.metadata().getProject(project2).index("expand-1").getNumberOfReplicas(), is(1));
        assertThat(expanded.metadata().getProject(project2).index("expand-all").getNumberOfReplicas(), is(2));

        final DiscoveryNodes threeDifferentNodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.create("alt1"))
            .add(DiscoveryNodeUtils.create("alt2"))
            .add(DiscoveryNodeUtils.create("alt3"))
            .build();
        // 3 different nodes == 1 or 2 replicas == no change
        state = ClusterState.builder(expanded).nodes(threeDifferentNodes).build();
        expanded = allocationService.adaptAutoExpandReplicas(state);
        assertThat(expanded, sameInstance(state));
    }

    private static final String FAKE_IN_SYNC_ALLOCATION_ID = "_in_sync_"; // so we can allocate primaries anywhere

    private static IndexMetadata.Builder indexMetadata(String name, Settings.Builder settings) {
        return IndexMetadata.builder(name)
            .settings(settings(IndexVersion.current()).put(IndexMetadata.SETTING_INDEX_UUID, randomUUID()).put(settings.build()))
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
        public void afterPrimariesBeforeReplicas(RoutingAllocation allocation, Predicate<ShardRouting> isRelevantShardPredicate) {}

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
