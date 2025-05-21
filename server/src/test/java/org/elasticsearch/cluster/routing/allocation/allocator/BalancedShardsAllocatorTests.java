/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.summingDouble;
import static java.util.stream.Collectors.summingLong;
import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.DISK_USAGE_BALANCE_FACTOR_SETTING;
import static org.elasticsearch.cluster.routing.allocation.allocator.WeightFunction.getIndexDiskUsageInBytes;
import static org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider.SETTING_IGNORE_DISK_WATERMARKS;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class BalancedShardsAllocatorTests extends ESAllocationTestCase {

    private static final Settings WITH_DISK_BALANCING = Settings.builder().put(DISK_USAGE_BALANCE_FACTOR_SETTING.getKey(), "1e-9").build();

    public void testDecideShardAllocation() {
        BalancedShardsAllocator allocator = new BalancedShardsAllocator(Settings.EMPTY);
        ClusterState clusterState = ClusterStateCreationUtils.state("idx", false, ShardRoutingState.STARTED);
        assertEquals(clusterState.nodes().getSize(), 3);

        // add new index
        String index = "idx_new";
        Metadata metadata = Metadata.builder(clusterState.metadata())
            .put(IndexMetadata.builder(index).settings(indexSettings(IndexVersion.current(), 1, 0)))
            .build();
        RoutingTable initialRoutingTable = RoutingTable.builder(
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY,
            clusterState.routingTable()
        ).addAsNew(metadata.getProject().index(index)).build();
        clusterState = ClusterState.builder(clusterState).metadata(metadata).routingTable(initialRoutingTable).build();

        assertThat(clusterState.metadata().projects(), aMapWithSize(1));
        final ProjectId projectId = clusterState.metadata().projects().keySet().iterator().next();

        ShardRouting shard = clusterState.globalRoutingTable().routingTable(projectId).index("idx_new").shard(0).primaryShard();
        RoutingAllocation allocation = createRoutingAllocation(clusterState);

        allocation.debugDecision(false);
        AllocateUnassignedDecision allocateDecision = allocator.decideShardAllocation(shard, allocation).getAllocateDecision();
        allocation.debugDecision(true);
        AllocateUnassignedDecision allocateDecisionWithExplain = allocator.decideShardAllocation(shard, allocation).getAllocateDecision();
        // the allocation decision should have same target node no matter the debug is on or off
        assertEquals(allocateDecision.getTargetNode().getId(), allocateDecisionWithExplain.getTargetNode().getId());

        allocator.allocate(allocation);
        List<ShardRouting> assignedShards = allocation.routingNodes().assignedShards(shard.shardId());
        assertEquals(1, assignedShards.size());
        // the allocation result be consistent with allocation decision
        assertEquals(allocateDecision.getTargetNode().getId(), assignedShards.get(0).currentNodeId());
    }

    public void testDecideShardAllocationWhenThereAreMultipleProjects() {
        final int numberOfNodes = randomIntBetween(3, 8);
        final int numberOfProjects = randomIntBetween(3, 8);

        final Metadata.Builder metadataBuilder = Metadata.builder();
        final GlobalRoutingTable.Builder routingTableBuilder = GlobalRoutingTable.builder();

        // Create some nodes
        final DiscoveryNodes.Builder nodeBuilder = DiscoveryNodes.builder();
        for (int i = 1; i <= numberOfNodes; i++) {
            final String nodeId = Strings.format("node_%02d", i);
            final DiscoveryNode node = DiscoveryNodeUtils.create(nodeId);
            nodeBuilder.add(node);
            if (i == 1) {
                nodeBuilder.localNodeId(nodeId);
            }
            if (i == 2) {
                nodeBuilder.masterNodeId(nodeId);
            }
        }
        final DiscoveryNodes nodes = nodeBuilder.build();

        // Create some projects with some assigned indices
        for (int i = 1; i <= numberOfProjects; i++) {
            var projectId = ProjectId.fromId(Strings.format("proj_%02d", i));
            String[] indices = {
                // 2 indices that are unique to this project
                "index_proj_" + i + "a",
                "index_proj_" + i + "b",
                // 1 index that is shared in all projects
                "common_index",
                // 1 index that is shared with some projects
                "common_index_" + (i % 3) };
            final Tuple<ProjectMetadata.Builder, RoutingTable.Builder> project = ClusterStateCreationUtils
                .projectWithAssignedPrimariesAndReplicas(projectId, indices, 1, 0, nodes);
            metadataBuilder.put(project.v1());
            routingTableBuilder.put(projectId, project.v2());
        }

        ClusterState.Builder stateBuilder = ClusterState.builder(new ClusterName("test"));
        stateBuilder.nodes(nodes);
        stateBuilder.metadata(metadataBuilder.generateClusterUuidIfNeeded());
        stateBuilder.routingTable(routingTableBuilder.build());

        ClusterState clusterState = stateBuilder.build();
        assertEquals(clusterState.nodes().getSize(), numberOfNodes);
        assertEquals(clusterState.metadata().projects().size(), numberOfProjects);

        // Verify that the initial state has no unassigned shards
        {
            RoutingAllocation allocation = createRoutingAllocation(clusterState);
            assertThat(allocation.routingNodes().unassigned().size(), is(0));
        }

        // add new index to a random project
        var origProject = randomFrom(clusterState.metadata().projects().values());
        final var indexName = "new_index";
        ProjectMetadata updatedProject = ProjectMetadata.builder(origProject)
            .put(IndexMetadata.builder(indexName).settings(indexSettings(IndexVersion.current(), 1, 0)))
            .build();
        GlobalRoutingTable routingTable = GlobalRoutingTable.builder(clusterState.globalRoutingTable())
            .put(
                origProject.id(),
                RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY, clusterState.routingTable(origProject.id()))
                    .addAsNew(updatedProject.index(indexName))
                    .build()
            )
            .build();
        clusterState = ClusterState.builder(clusterState).putProjectMetadata(updatedProject).routingTable(routingTable).build();
        // Verify that new state has 1 unassigned shard (with the expected name)
        {
            RoutingAllocation allocation = createRoutingAllocation(clusterState);
            final RoutingNodes.UnassignedShards unassigned = allocation.routingNodes().unassigned();
            assertThat(unassigned.size(), is(1));
            assertThat(unassigned.iterator().next().getIndexName(), equalTo(indexName));
        }

        final BalancedShardsAllocator allocator = new BalancedShardsAllocator(Settings.EMPTY);
        final RoutingAllocation allocation = createRoutingAllocation(clusterState);
        final ShardRouting shard = clusterState.globalRoutingTable()
            .routingTable(origProject.id())
            .index(indexName)
            .shard(0)
            .primaryShard();
        AllocateUnassignedDecision allocateDecision = allocator.decideShardAllocation(shard, allocation).getAllocateDecision();
        final DiscoveryNode targetNode = allocateDecision.getTargetNode();
        assertThat(targetNode, notNullValue());
        assertThat(nodes.get(targetNode.getId()), sameInstance(targetNode));

        int assignedShardsBefore = allocation.routingNodes().getAssignedShards().size();
        allocator.allocate(allocation);

        List<ShardRouting> assignedShards = allocation.routingNodes().assignedShards(shard.shardId());
        assertThat(assignedShards.size(), equalTo(1));
        // the allocation result be consistent with allocation decision
        assertThat(assignedShards.get(0).currentNodeId(), equalTo(targetNode.getId()));

        assertThat(allocation.routingNodes().getAssignedShards().size(), equalTo(assignedShardsBefore + 1));
        assertThat(allocation.routingNodes().unassigned().size(), equalTo(0));

    }

    public void testBalanceByForecastWriteLoad() {

        var allocationService = new MockAllocationService(
            yesAllocationDeciders(),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(BalancerSettings.DEFAULT, TEST_WRITE_LOAD_FORECASTER),
            EmptyClusterInfoService.INSTANCE,
            SNAPSHOT_INFO_SERVICE_WITH_NO_SHARD_SIZES
        );

        var clusterState = applyStartedShardsUntilNoChange(
            createStateWithIndices(
                anIndex("heavy-index").indexWriteLoadForecast(8.0),
                anIndex("light-index-1").indexWriteLoadForecast(1.0),
                anIndex("light-index-2").indexWriteLoadForecast(2.0),
                anIndex("light-index-3").indexWriteLoadForecast(3.0),
                anIndex("zero-write-load-index").indexWriteLoadForecast(0.0),
                anIndex("no-write-load-index")
            ),
            allocationService
        );

        assertThat(
            getShardsPerNode(clusterState).values(),
            containsInAnyOrder(
                Set.of("heavy-index"),
                Set.of("light-index-1", "light-index-2", "light-index-3", "zero-write-load-index", "no-write-load-index")
            )
        );

        assertThat(
            getPerNode(
                clusterState,
                summingDouble(
                    it -> TEST_WRITE_LOAD_FORECASTER.getForecastedWriteLoad(clusterState.metadata().getProject().index(it.index()))
                        .orElse(0.0)
                )
            ).values(),
            everyItem(lessThanOrEqualTo(8.0))
        );
    }

    public void testBalanceByForecastDiskUsage() {

        var allocationService = createAllocationService(WITH_DISK_BALANCING);

        var clusterState = applyStartedShardsUntilNoChange(
            createStateWithIndices(
                anIndex("heavy-index").shardSizeInBytesForecast(ByteSizeValue.ofGb(8).getBytes()),
                anIndex("light-index-1").shardSizeInBytesForecast(ByteSizeValue.ofGb(1).getBytes()),
                anIndex("light-index-2").shardSizeInBytesForecast(ByteSizeValue.ofGb(2).getBytes()),
                anIndex("light-index-3").shardSizeInBytesForecast(ByteSizeValue.ofGb(3).getBytes()),
                anIndex("zero-disk-usage-index").shardSizeInBytesForecast(0L),
                anIndex("no-disk-usage-index")
            ),
            allocationService
        );

        assertThat(
            getShardsPerNode(clusterState).values(),
            containsInAnyOrder(
                Set.of("heavy-index"),
                Set.of("light-index-1", "light-index-2", "light-index-3", "zero-disk-usage-index", "no-disk-usage-index")
            )
        );

        assertThat(
            getPerNode(
                clusterState,
                summingLong(it -> clusterState.metadata().getProject().index(it.index()).getForecastedShardSizeInBytes().orElse(0L))
            ).values(),
            everyItem(lessThanOrEqualTo(ByteSizeValue.ofGb(8).getBytes()))
        );
    }

    public void testBalanceByActualDiskUsage() {

        var allocationService = createAllocationService(
            WITH_DISK_BALANCING,
            () -> createClusterInfo(
                Map.ofEntries(
                    Map.entry("[heavy-index][0][p]", ByteSizeValue.ofGb(8).getBytes()),
                    Map.entry("[light-index-1][0][p]", ByteSizeValue.ofGb(1).getBytes()),
                    Map.entry("[light-index-2][0][p]", ByteSizeValue.ofGb(2).getBytes()),
                    Map.entry("[light-index-3][0][p]", ByteSizeValue.ofGb(3).getBytes()),
                    Map.entry("[zero-disk-usage-index][0][p]", 0L)
                    // no-disk-usage-index is intentionally not present in cluster info
                )
            )
        );

        var clusterState = applyStartedShardsUntilNoChange(
            createStateWithIndices(
                anIndex("heavy-index"),
                anIndex("light-index-1"),
                anIndex("light-index-2"),
                anIndex("light-index-3"),
                anIndex("zero-disk-usage-index"),
                anIndex("no-disk-usage-index")
            ),
            allocationService
        );

        assertThat(
            getShardsPerNode(clusterState).values(),
            containsInAnyOrder(
                Set.of("heavy-index"),
                Set.of("light-index-1", "light-index-2", "light-index-3", "zero-disk-usage-index", "no-disk-usage-index")
            )
        );

        assertThat(
            getPerNode(
                clusterState,
                summingLong(it -> clusterState.metadata().getProject().index(it.index()).getForecastedShardSizeInBytes().orElse(0L))
            ).values(),
            everyItem(lessThanOrEqualTo(ByteSizeValue.ofGb(8).getBytes()))
        );
    }

    public void testBalanceByActualAndForecastDiskUsage() {

        var allocationService = createAllocationService(
            WITH_DISK_BALANCING,
            () -> createClusterInfo(Map.of("[heavy-index][0][p]", ByteSizeValue.ofGb(8).getBytes()))
        );

        var clusterState = applyStartedShardsUntilNoChange(
            createStateWithIndices(
                anIndex("heavy-index"),// size is set in cluster info
                anIndex("light-index-1").shardSizeInBytesForecast(ByteSizeValue.ofGb(1).getBytes()),
                anIndex("light-index-2").shardSizeInBytesForecast(ByteSizeValue.ofGb(2).getBytes()),
                anIndex("light-index-3").shardSizeInBytesForecast(ByteSizeValue.ofGb(3).getBytes()),
                anIndex("zero-disk-usage-index").shardSizeInBytesForecast(0L),
                anIndex("no-disk-usage-index")
            ),
            allocationService
        );

        assertThat(
            getShardsPerNode(clusterState).values(),
            containsInAnyOrder(
                Set.of("heavy-index"),
                Set.of("light-index-1", "light-index-2", "light-index-3", "zero-disk-usage-index", "no-disk-usage-index")
            )
        );

        assertThat(
            getPerNode(
                clusterState,
                summingLong(it -> clusterState.metadata().getProject().index(it.index()).getForecastedShardSizeInBytes().orElse(0L))
            ).values(),
            everyItem(lessThanOrEqualTo(ByteSizeValue.ofGb(8).getBytes()))
        );
    }

    public void testDoNotBalancePartialIndicesByDiskUsage() {

        var allocationService = createAllocationService(WITH_DISK_BALANCING, () -> createClusterInfo(Map.of()));

        var partialSearchableSnapshotSettings = indexSettings(IndexVersion.current(), 1, 0) //
            .put(SETTING_IGNORE_DISK_WATERMARKS.getKey(), true);

        var clusterState = applyStartedShardsUntilNoChange(
            createStateWithIndices(
                anIndex("frozen-index-1", partialSearchableSnapshotSettings).shardSizeInBytesForecast(ByteSizeValue.ofGb(1).getBytes()),
                anIndex("frozen-index-2", partialSearchableSnapshotSettings).shardSizeInBytesForecast(ByteSizeValue.ofGb(1).getBytes()),
                anIndex("frozen-index-3", partialSearchableSnapshotSettings).shardSizeInBytesForecast(ByteSizeValue.ofGb(1).getBytes()),
                anIndex("frozen-index-4", partialSearchableSnapshotSettings).shardSizeInBytesForecast(ByteSizeValue.ofGb(10).getBytes())
            ),
            allocationService
        );

        assertThat(getShardsPerNode(clusterState).values(), everyItem(hasSize(2)));
    }

    private static Map<String, Set<String>> getShardsPerNode(ClusterState clusterState) {
        return getPerNode(clusterState, mapping(ShardRouting::getIndexName, toSet()));
    }

    private static <T> Map<String, T> getPerNode(ClusterState clusterState, Collector<ShardRouting, ?, T> collector) {
        return clusterState.getRoutingNodes()
            .stream()
            .collect(Collectors.toMap(RoutingNode::nodeId, it -> StreamSupport.stream(it.spliterator(), false).collect(collector)));
    }

    /**
     * {@see https://github.com/elastic/elasticsearch/issues/88384}
     */
    public void testRebalanceImprovesTheBalanceOfTheShards() {
        var discoveryNodesBuilder = DiscoveryNodes.builder();
        for (int node = 0; node < 3; node++) {
            discoveryNodesBuilder.add(newNode("node-" + node));
        }

        var metadataBuilder = Metadata.builder();
        var routingTableBuilder = RoutingTable.builder();

        var indices = Arrays.asList(
            Tuple.tuple(UUIDs.randomBase64UUID(random()), Map.of("node-0", 4, "node-1", 4, "node-2", 2)),
            Tuple.tuple(UUIDs.randomBase64UUID(random()), Map.of("node-0", 4, "node-1", 4, "node-2", 2)),
            Tuple.tuple(UUIDs.randomBase64UUID(random()), Map.of("node-0", 3, "node-1", 3, "node-2", 4)),
            Tuple.tuple(UUIDs.randomBase64UUID(random()), Map.of("node-0", 3, "node-1", 3, "node-2", 4)),
            Tuple.tuple(UUIDs.randomBase64UUID(random()), Map.of("node-0", 4, "node-1", 3, "node-2", 3)),
            Tuple.tuple(UUIDs.randomBase64UUID(random()), Map.of("node-0", 3, "node-1", 4, "node-2", 3)),
            Tuple.tuple(UUIDs.randomBase64UUID(random()), Map.of("node-0", 3, "node-1", 3, "node-2", 4))
        );
        Collections.shuffle(indices, random());
        for (var index : indices) {
            addIndex(metadataBuilder, routingTableBuilder, index.v1(), index.v2());
        }

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodesBuilder)
            .metadata(metadataBuilder)
            .routingTable(routingTableBuilder)
            .build();

        var allocationService = createAllocationService(
            Settings.builder()
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 1)
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 1)
                .build()
        );

        var reroutedState = allocationService.reroute(clusterState, "test", ActionListener.noop());

        for (ShardRouting relocatingShard : RoutingNodesHelper.shardsWithState(reroutedState.getRoutingNodes(), RELOCATING)) {
            assertThat(
                "new allocation should not result in indexes with 2 shards per node",
                getTargetShardPerNodeCount(reroutedState.getRoutingTable().index(relocatingShard.index())).containsValue(2),
                equalTo(false)
            );
        }
    }

    public void testGetIndexDiskUsageInBytes() {
        {
            final var indexDiskUsageInBytes = getIndexDiskUsageInBytes(
                ClusterInfo.EMPTY,
                IndexMetadata.builder("index").settings(indexSettings(IndexVersion.current(), 1, 0)).build()
            );

            // When no information is available we just return 0
            assertThat(indexDiskUsageInBytes, is(equalTo(0L)));
        }

        {
            final var shardSize = ByteSizeValue.ofGb(50).getBytes();

            final Map<String, Long> shardSizes = new HashMap<>();
            shardSizes.put("[index][0][p]", shardSize);
            shardSizes.put("[index][0][r]", shardSize - randomLongBetween(0, 10240));

            final var indexDiskUsageInBytes = getIndexDiskUsageInBytes(
                randomBoolean() ? ClusterInfo.EMPTY : createClusterInfo(shardSizes),
                IndexMetadata.builder("index")
                    .settings(indexSettings(IndexVersion.current(), 1, 1))
                    .shardSizeInBytesForecast(shardSize)
                    .build()
            );

            // We only use the clusterInfo as a fallback
            assertThat(indexDiskUsageInBytes, is(equalTo(shardSize * 2)));
        }

        {
            final var shardSize = ByteSizeValue.ofGb(50).getBytes();

            final Map<String, Long> shardSizes = new HashMap<>();
            shardSizes.put("[index][0][p]", shardSize);
            shardSizes.put("[index][0][r]", shardSize - randomLongBetween(0, 10240));

            final var indexDiskUsageInBytes = getIndexDiskUsageInBytes(
                createClusterInfo(shardSizes),
                IndexMetadata.builder("index").settings(indexSettings(IndexVersion.current(), 1, 1)).build()
            );

            // Fallback to clusterInfo when no forecast is available
            assertThat(indexDiskUsageInBytes, is(equalTo(shardSizes.values().stream().mapToLong(size -> size).sum())));
        }

        {
            // Only 2 of 4 shards sizes are available, therefore an average is calculated
            // in order to compute the total index size
            final Map<String, Long> shardSizes = new HashMap<>();
            shardSizes.put("[index][0][p]", randomLongBetween(1024, 10240));
            shardSizes.put("[index][0][r]", randomLongBetween(1024, 10240));
            shardSizes.put("[index][1][p]", randomLongBetween(1024, 10240));
            shardSizes.put("[index][1][r]", randomLongBetween(1024, 10240));

            final var averageShardSize = shardSizes.values().stream().mapToLong(size -> size).sum() / shardSizes.size();

            final var indexMetadata = IndexMetadata.builder("index").settings(indexSettings(IndexVersion.current(), 4, 1)).build();

            final var indexDiskUsageInBytes = getIndexDiskUsageInBytes(createClusterInfo(shardSizes), indexMetadata);

            final var numberOfCopies = indexMetadata.getNumberOfShards() * (1 + indexMetadata.getNumberOfReplicas());
            assertThat(indexDiskUsageInBytes, is(equalTo(averageShardSize * numberOfCopies)));
        }

        {
            var forecastedShardSize = randomLongBetween(1024, 10240);
            var observedShardSize = randomLongBetween(1024, 10240);

            final var indexMetadata = IndexMetadata.builder("index")
                .settings(indexSettings(IndexVersion.current(), 1, 0))
                .shardSizeInBytesForecast(forecastedShardSize)
                .build();

            final var indexDiskUsageInBytes = getIndexDiskUsageInBytes(
                createClusterInfo(Map.of("[index][0][p]", observedShardSize)),
                indexMetadata
            );

            // should pick the max shard size among forecast and cluster info
            assertThat(indexDiskUsageInBytes, equalTo(Math.max(forecastedShardSize, observedShardSize)));
        }

        {
            final var indexMetadata = IndexMetadata.builder("index")
                .settings(indexSettings(IndexVersion.current(), 1, 0).put(SETTING_IGNORE_DISK_WATERMARKS.getKey(), true))
                .build();

            final var indexDiskUsageInBytes = getIndexDiskUsageInBytes(
                createClusterInfo(Map.of("[index][0][p]", randomLongBetween(1024, 10240))),
                indexMetadata
            );

            // partially cached indices should not be balanced by disk usage
            assertThat(indexDiskUsageInBytes, equalTo(0L));
        }
    }

    public void testThresholdLimit() {
        final var badValue = (float) randomDoubleBetween(0.0, Math.nextDown(1.0f), true);
        expectThrows(
            IllegalArgumentException.class,
            () -> new BalancerSettings(Settings.builder().put(BalancedShardsAllocator.THRESHOLD_SETTING.getKey(), badValue).build())
        );

        final var goodValue = (float) randomDoubleBetween(1.0, 10.0, true);
        assertEquals(
            goodValue,
            new BalancerSettings(Settings.builder().put(BalancedShardsAllocator.THRESHOLD_SETTING.getKey(), goodValue).build())
                .getThreshold(),
            0.0f
        );
    }

    public void testShardSizeDiscrepancyWithinIndex() {
        var discoveryNodesBuilder = DiscoveryNodes.builder();
        for (int node = 0; node < 3; node++) {
            discoveryNodesBuilder.add(newNode("node-" + node));
        }

        var metadataBuilder = Metadata.builder();
        var routingTableBuilder = RoutingTable.builder();

        addIndex(metadataBuilder, routingTableBuilder, "testindex", Map.of("node-0", 1, "node-1", 1));

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodesBuilder)
            .metadata(metadataBuilder)
            .routingTable(routingTableBuilder)
            .build();

        var index = clusterState.routingTable().index("testindex").getIndex();

        // Even if the two shards in this index vary massively in size, we must compute the balancing threshold to be high enough that
        // we don't make pointless movements. 500GiB of difference is enough to demonstrate the bug.

        var allocationService = createAllocationService(
            Settings.EMPTY,
            () -> new ClusterInfo(
                Map.of(),
                Map.of(),
                Map.of(
                    ClusterInfo.shardIdentifierFromRouting(new ShardId(index, 0), true),
                    0L,
                    ClusterInfo.shardIdentifierFromRouting(new ShardId(index, 1), true),
                    ByteSizeUnit.GB.toBytes(500)
                ),
                Map.of(),
                Map.of(),
                Map.of()
            )
        );

        assertSame(clusterState, reroute(allocationService, clusterState));
    }

    public void testPartitionedClusterWithSeparateWeights() {
        var allocationService = new MockAllocationService(
            prefixAllocationDeciders(),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(
                BalancerSettings.DEFAULT,
                TEST_WRITE_LOAD_FORECASTER,
                new PrefixBalancingWeightsFactory(
                    Map.of("shardsOnly", new WeightFunction(1, 0, 0, 0), "weightsOnly", new WeightFunction(0, 0, 1, 0))
                )
            ),
            EmptyClusterInfoService.INSTANCE,
            SNAPSHOT_INFO_SERVICE_WITH_NO_SHARD_SIZES
        );

        var clusterState = applyStartedShardsUntilNoChange(
            createStateWithIndices(
                List.of("shardsOnly-1", "shardsOnly-2", "weightsOnly-1", "weightsOnly-2"),
                shardId -> prefix(shardId.getIndexName()) + "-1",
                anIndex("weightsOnly-heavy-index").indexWriteLoadForecast(8.0),
                anIndex("weightsOnly-light-index-1").indexWriteLoadForecast(1.0),
                anIndex("weightsOnly-light-index-2").indexWriteLoadForecast(2.0),
                anIndex("weightsOnly-light-index-3").indexWriteLoadForecast(3.0),
                anIndex("weightsOnly-zero-write-load-index").indexWriteLoadForecast(0.0),
                anIndex("weightsOnly-no-write-load-index"),
                anIndex("shardsOnly-heavy-index").indexWriteLoadForecast(8.0),
                anIndex("shardsOnly-light-index-1").indexWriteLoadForecast(1.0),
                anIndex("shardsOnly-light-index-2").indexWriteLoadForecast(2.0),
                anIndex("shardsOnly-light-index-3").indexWriteLoadForecast(3.0),
                anIndex("shardsOnly-zero-write-load-index").indexWriteLoadForecast(0.0),
                anIndex("shardsOnly-no-write-load-index")
            ),
            allocationService
        );

        Map<String, Set<String>> shardsPerNode = getShardsPerNode(clusterState);
        Map<String, Set<String>> shardBalancedPartition = shardsPerNode.entrySet()
            .stream()
            .filter(e -> e.getKey().startsWith("shardsOnly"))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<String, Set<String>> weightBalancedPartition = shardsPerNode.entrySet()
            .stream()
            .filter(e -> e.getKey().startsWith("weightsOnly"))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        // The partition that balances on weights only is skewed
        assertThat(
            weightBalancedPartition.values(),
            containsInAnyOrder(
                Set.of("weightsOnly-heavy-index"),
                Set.of(
                    "weightsOnly-light-index-1",
                    "weightsOnly-light-index-2",
                    "weightsOnly-light-index-3",
                    "weightsOnly-zero-write-load-index",
                    "weightsOnly-no-write-load-index"
                )
            )
        );

        // The partition that balances on shard count only has an even distribution of shards
        assertThat(shardBalancedPartition.get("shardsOnly-1"), hasSize(3));
        assertThat(shardBalancedPartition.get("shardsOnly-2"), hasSize(3));
    }

    private Map<String, Integer> getTargetShardPerNodeCount(IndexRoutingTable indexRoutingTable) {
        var counts = new HashMap<String, Integer>();
        for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
            var shard = indexRoutingTable.shard(shardId).primaryShard();
            counts.compute(
                shard.relocating() ? shard.relocatingNodeId() : shard.currentNodeId(),
                (nodeId, count) -> count != null ? count + 1 : 1
            );
        }
        return counts;
    }

    private RoutingAllocation createRoutingAllocation(ClusterState clusterState) {
        return new RoutingAllocation(
            new AllocationDeciders(List.of()),
            RoutingNodes.mutable(clusterState.globalRoutingTable(), clusterState.nodes()),
            clusterState,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        );
    }

    private static ClusterInfo createClusterInfo(Map<String, Long> indexSizes) {
        return new ClusterInfo(Map.of(), Map.of(), indexSizes, Map.of(), Map.of(), Map.of());
    }

    private static IndexMetadata.Builder anIndex(String name) {
        return anIndex(name, indexSettings(IndexVersion.current(), 1, 0));
    }

    private static IndexMetadata.Builder anIndex(String name, Settings.Builder settings) {
        return IndexMetadata.builder(name).settings(settings);
    }

    private static ClusterState createStateWithIndices(IndexMetadata.Builder... indexMetadataBuilders) {
        return createStateWithIndices(List.of("node-1", "node-2"), shardId -> "node-1", indexMetadataBuilders);
    }

    private static ClusterState createStateWithIndices(
        List<String> nodeNames,
        Function<ShardId, String> unbalancedAllocator,
        IndexMetadata.Builder... indexMetadataBuilders
    ) {
        var metadataBuilder = Metadata.builder();
        var routingTableBuilder = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);
        if (randomBoolean()) {
            // allocate all shards from scratch
            for (var index : indexMetadataBuilders) {
                var indexMetadata = index.build();
                metadataBuilder.put(indexMetadata, false);
                routingTableBuilder.addAsNew(indexMetadata);
            }
        } else {
            // ensure unbalanced cluster cloud be properly balanced
            // simulates a case when we add a second node and ensure shards could be evenly spread across all available nodes
            for (var index : indexMetadataBuilders) {
                var inSyncId = UUIDs.randomBase64UUID();
                var indexMetadata = index.putInSyncAllocationIds(0, Set.of(inSyncId)).build();
                metadataBuilder.put(indexMetadata, false);
                ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
                routingTableBuilder.add(
                    IndexRoutingTable.builder(indexMetadata.getIndex())
                        .addShard(
                            shardRoutingBuilder(shardId, unbalancedAllocator.apply(shardId), true, ShardRoutingState.STARTED)
                                .withAllocationId(AllocationId.newInitializing(inSyncId))
                                .build()
                        )
                );
            }
        }

        DiscoveryNodes.Builder discoveryNodesBuilder = DiscoveryNodes.builder();
        for (String nodeName : nodeNames) {
            discoveryNodesBuilder.add(newNode(nodeName));
        }

        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodesBuilder)
            .metadata(metadataBuilder)
            .routingTable(routingTableBuilder)
            .build();
    }

    private void addIndex(
        Metadata.Builder metadataBuilder,
        RoutingTable.Builder routingTableBuilder,
        String name,
        Map<String, Integer> assignments
    ) {
        var numberOfShards = assignments.entrySet().stream().mapToInt(Map.Entry::getValue).sum();
        var inSyncIds = randomList(numberOfShards, numberOfShards, () -> UUIDs.randomBase64UUID(random()));
        var indexMetadataBuilder = IndexMetadata.builder(name).settings(indexSettings(IndexVersion.current(), numberOfShards, 0));

        for (int shardId = 0; shardId < numberOfShards; shardId++) {
            indexMetadataBuilder.putInSyncAllocationIds(shardId, Set.of(inSyncIds.get(shardId)));
        }
        metadataBuilder.put(indexMetadataBuilder);

        var indexId = metadataBuilder.get(name).getIndex();
        var indexRoutingTableBuilder = IndexRoutingTable.builder(indexId);

        int shardId = 0;
        for (var assignment : assignments.entrySet()) {
            for (int i = 0; i < assignment.getValue(); i++) {
                indexRoutingTableBuilder.addShard(
                    shardRoutingBuilder(new ShardId(indexId, shardId), assignment.getKey(), true, ShardRoutingState.STARTED)
                        .withAllocationId(AllocationId.newInitializing(inSyncIds.get(shardId)))
                        .build()
                );
                shardId++;
            }
        }
        routingTableBuilder.add(indexRoutingTableBuilder);
    }

    /**
     * A {@link BalancingWeightsFactory} that assumes the cluster is partitioned by the prefix
     * of the node and shard names before the `-`.
     */
    class PrefixBalancingWeightsFactory implements BalancingWeightsFactory {

        private final Map<String, WeightFunction> prefixWeights;

        PrefixBalancingWeightsFactory(Map<String, WeightFunction> prefixWeights) {
            this.prefixWeights = prefixWeights;
        }

        @Override
        public BalancingWeights create() {
            return new PrefixBalancingWeights();
        }

        class PrefixBalancingWeights implements BalancingWeights {

            @Override
            public WeightFunction weightFunctionForShard(ShardRouting shard) {
                return prefixWeights.get(prefix(shard.getIndexName()));
            }

            @Override
            public WeightFunction weightFunctionForNode(RoutingNode node) {
                return prefixWeights.get(prefix(node.node().getId()));
            }

            @Override
            public NodeSorters createNodeSorters(
                BalancedShardsAllocator.ModelNode[] modelNodes,
                BalancedShardsAllocator.Balancer balancer
            ) {
                final HashMap<String, BalancedShardsAllocator.NodeSorter> prefixNodeSorters = new HashMap<>();
                for (var entry : prefixWeights.entrySet()) {
                    prefixNodeSorters.put(
                        entry.getKey(),
                        new BalancedShardsAllocator.NodeSorter(
                            Arrays.stream(modelNodes)
                                .filter(node -> prefix(node.getRoutingNode().node().getId()).equals(entry.getKey()))
                                .toArray(BalancedShardsAllocator.ModelNode[]::new),
                            entry.getValue(),
                            balancer
                        )
                    );
                }
                return new NodeSorters() {

                    @Override
                    public Iterator<BalancedShardsAllocator.NodeSorter> iterator() {
                        return prefixNodeSorters.values().iterator();
                    }

                    @Override
                    public BalancedShardsAllocator.NodeSorter sorterForShard(ShardRouting shard) {
                        return prefixNodeSorters.get(prefix(shard.getIndexName()));
                    }
                };
            }
        }
    }

    /**
     * Allocation deciders that only allow shards to be allocated to nodes whose names share the same prefix
     * as the index they're from
     */
    private AllocationDeciders prefixAllocationDeciders() {
        return new AllocationDeciders(List.of(new AllocationDecider() {
            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                return nodePrefixMatchesIndexPrefix(shardRouting, node);
            }

            @Override
            public Decision canRemain(
                IndexMetadata indexMetadata,
                ShardRouting shardRouting,
                RoutingNode node,
                RoutingAllocation allocation
            ) {
                return nodePrefixMatchesIndexPrefix(shardRouting, node);
            }

            private Decision nodePrefixMatchesIndexPrefix(ShardRouting shardRouting, RoutingNode node) {
                var indexPrefix = prefix(shardRouting.index().getName());
                var nodePrefix = prefix(node.node().getId());
                return nodePrefix.equals(indexPrefix) ? Decision.YES : Decision.NO;
            }
        }));
    }

    private static String prefix(String value) {
        assert value != null && value.contains("-") : "Invalid name passed: " + value;
        return value.substring(0, value.indexOf("-"));
    }
}
