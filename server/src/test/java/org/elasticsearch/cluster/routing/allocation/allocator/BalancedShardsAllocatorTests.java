/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.summingDouble;
import static java.util.stream.Collectors.summingLong;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.Balancer.getIndexDiskUsageInBytes;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class BalancedShardsAllocatorTests extends ESAllocationTestCase {

    public void testDecideShardAllocation() {
        BalancedShardsAllocator allocator = new BalancedShardsAllocator(Settings.EMPTY);
        ClusterState clusterState = ClusterStateCreationUtils.state("idx", false, ShardRoutingState.STARTED);
        assertEquals(clusterState.nodes().getSize(), 3);

        // add new index
        String index = "idx_new";
        Metadata metadata = Metadata.builder(clusterState.metadata())
            .put(IndexMetadata.builder(index).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
            .build();
        RoutingTable initialRoutingTable = RoutingTable.builder(
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY,
            clusterState.routingTable()
        ).addAsNew(metadata.index(index)).build();
        clusterState = ClusterState.builder(clusterState).metadata(metadata).routingTable(initialRoutingTable).build();

        ShardRouting shard = clusterState.routingTable().index("idx_new").shard(0).primaryShard();
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
        assertNotNull(allocateDecision.getTargetNode().getId(), assignedShards.get(0).currentNodeId());
    }

    public void testBalanceByForecastWriteLoad() {

        var allocationService = new MockAllocationService(
            yesAllocationDeciders(),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                TEST_WRITE_LOAD_FORECASTER
            ),
            EmptyClusterInfoService.INSTANCE,
            SNAPSHOT_INFO_SERVICE_WITH_NO_SHARD_SIZES
        );

        var clusterState = applyStartedShardsUntilNoChange(
            stateWithStartedIndices(
                IndexMetadata.builder("heavy-index").indexWriteLoadForecast(8.0),
                IndexMetadata.builder("light-index-1").indexWriteLoadForecast(1.0),
                IndexMetadata.builder("light-index-2").indexWriteLoadForecast(2.0),
                IndexMetadata.builder("light-index-3").indexWriteLoadForecast(3.0),
                IndexMetadata.builder("zero-write-load-index").indexWriteLoadForecast(0.0),
                IndexMetadata.builder("no-write-load-index")
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
                    it -> TEST_WRITE_LOAD_FORECASTER.getForecastedWriteLoad(clusterState.metadata().index(it.index())).orElse(0.0)
                )
            ).values(),
            everyItem(lessThanOrEqualTo(8.0))
        );
    }

    public void testBalanceByForecastDiskUsage() {

        var allocationService = createAllocationService(
            Settings.builder()
                // enable disk based balancing
                .put(BalancedShardsAllocator.DISK_USAGE_BALANCE_FACTOR_SETTING.getKey(), "1e-9")
                .build()
        );

        var clusterState = applyStartedShardsUntilNoChange(
            stateWithStartedIndices(
                IndexMetadata.builder("heavy-index").shardSizeInBytesForecast(ByteSizeValue.ofGb(8).getBytes()),
                IndexMetadata.builder("light-index-1").shardSizeInBytesForecast(ByteSizeValue.ofGb(1).getBytes()),
                IndexMetadata.builder("light-index-2").shardSizeInBytesForecast(ByteSizeValue.ofGb(2).getBytes()),
                IndexMetadata.builder("light-index-3").shardSizeInBytesForecast(ByteSizeValue.ofGb(3).getBytes()),
                IndexMetadata.builder("zero-disk-usage-index").shardSizeInBytesForecast(0L),
                IndexMetadata.builder("no-disk-usage-index")
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
                summingLong(it -> clusterState.metadata().index(it.index()).getForecastedShardSizeInBytes().orElse(0L))
            ).values(),
            everyItem(lessThanOrEqualTo(ByteSizeValue.ofGb(8).getBytes()))
        );
    }

    public void testBalanceByActualDiskUsage() {

        var allocationService = createAllocationService(
            Settings.builder()
                // enable disk based balancing
                .put(BalancedShardsAllocator.DISK_USAGE_BALANCE_FACTOR_SETTING.getKey(), "1e-9")
                .build(),
            createClusterInfoService(
                Map.ofEntries(
                    Map.entry("heavy-index", ByteSizeValue.ofGb(8).getBytes()),
                    Map.entry("light-index-1", ByteSizeValue.ofGb(1).getBytes()),
                    Map.entry("light-index-2", ByteSizeValue.ofGb(2).getBytes()),
                    Map.entry("light-index-3", ByteSizeValue.ofGb(3).getBytes()),
                    Map.entry("zero-disk-usage-index", 0L)
                    // no-disk-usage-index is intentionally not present in cluster info
                )
            )
        );

        var clusterState = applyStartedShardsUntilNoChange(
            stateWithStartedIndices(
                IndexMetadata.builder("heavy-index"),
                IndexMetadata.builder("light-index-1"),
                IndexMetadata.builder("light-index-2"),
                IndexMetadata.builder("light-index-3"),
                IndexMetadata.builder("zero-disk-usage-index"),
                IndexMetadata.builder("no-disk-usage-index")
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
                summingLong(it -> clusterState.metadata().index(it.index()).getForecastedShardSizeInBytes().orElse(0L))
            ).values(),
            everyItem(lessThanOrEqualTo(ByteSizeValue.ofGb(8).getBytes()))
        );
    }

    public void testBalanceByActualAndForecastDiskUsage() {

        var allocationService = createAllocationService(
            Settings.builder()
                // enable disk based balancing
                .put(BalancedShardsAllocator.DISK_USAGE_BALANCE_FACTOR_SETTING.getKey(), "1e-9")
                .build(),
            createClusterInfoService(Map.of("heavy-index", ByteSizeValue.ofGb(8).getBytes()))
        );

        var clusterState = applyStartedShardsUntilNoChange(
            stateWithStartedIndices(
                IndexMetadata.builder("heavy-index"),// size is set in cluster info
                IndexMetadata.builder("light-index-1").shardSizeInBytesForecast(ByteSizeValue.ofGb(1).getBytes()),
                IndexMetadata.builder("light-index-2").shardSizeInBytesForecast(ByteSizeValue.ofGb(2).getBytes()),
                IndexMetadata.builder("light-index-3").shardSizeInBytesForecast(ByteSizeValue.ofGb(3).getBytes()),
                IndexMetadata.builder("zero-disk-usage-index").shardSizeInBytesForecast(0L),
                IndexMetadata.builder("no-disk-usage-index")
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
                summingLong(it -> clusterState.metadata().index(it.index()).getForecastedShardSizeInBytes().orElse(0L))
            ).values(),
            everyItem(lessThanOrEqualTo(ByteSizeValue.ofGb(8).getBytes()))
        );
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
                IndexMetadata.builder("index").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0).build()
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
                randomBoolean() ? ClusterInfo.EMPTY : new ClusterInfo(Map.of(), Map.of(), shardSizes, Map.of(), Map.of(), Map.of()),
                IndexMetadata.builder("index")
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(1)
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
                new ClusterInfo(Map.of(), Map.of(), shardSizes, Map.of(), Map.of(), Map.of()),
                IndexMetadata.builder("index").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1).build()
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

            final var indexMetadata = IndexMetadata.builder("index")
                .settings(settings(Version.CURRENT))
                .numberOfShards(4)
                .numberOfReplicas(1)
                .build();

            final var indexDiskUsageInBytes = getIndexDiskUsageInBytes(
                new ClusterInfo(Map.of(), Map.of(), shardSizes, Map.of(), Map.of(), Map.of()),
                indexMetadata
            );

            final var numberOfCopies = indexMetadata.getNumberOfShards() * (1 + indexMetadata.getNumberOfReplicas());
            assertThat(indexDiskUsageInBytes, is(equalTo(averageShardSize * numberOfCopies)));
        }
    }

    public void testThresholdLimit() {
        final var badValue = (float) randomDoubleBetween(0.0, Math.nextDown(1.0f), true);
        assertEquals(
            1.0f,
            new BalancedShardsAllocator(Settings.builder().put(BalancedShardsAllocator.THRESHOLD_SETTING.getKey(), badValue).build())
                .getThreshold(),
            0.0f
        );
        assertCriticalWarnings("ignoring value [" + badValue + """
            ] for [cluster.routing.allocation.balance.threshold] since it is smaller than 1.0; setting \
            [cluster.routing.allocation.balance.threshold] to a value smaller than 1.0 will be forbidden in a future release""");

        final var goodValue = (float) randomDoubleBetween(1.0, 10.0, true);
        assertEquals(
            goodValue,
            new BalancedShardsAllocator(Settings.builder().put(BalancedShardsAllocator.THRESHOLD_SETTING.getKey(), goodValue).build())
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
            RoutingNodes.mutable(clusterState.routingTable(), clusterState.nodes()),
            clusterState,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        );
    }

    private static ClusterInfoService createClusterInfoService(Map<String, Long> indexSizes) {
        return () -> new ClusterInfo(
            Map.of(),
            Map.of(),
            indexSizes.entrySet()
                .stream()
                .collect(
                    toMap(
                        entry -> ClusterInfo.shardIdentifierFromRouting(new ShardId(entry.getKey(), "_na_", 0), true),
                        Map.Entry::getValue
                    )
                ),
            Map.of(),
            Map.of(),
            Map.of()
        );
    }

    private static ClusterState stateWithStartedIndices(IndexMetadata.Builder... indices) {
        var metadataBuilder = Metadata.builder();
        var routingTableBuilder = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);
        for (var index : indices) {
            var build = index.settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0).build();
            metadataBuilder.put(build, false);
            routingTableBuilder.addAsNew(build);
        }

        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(newNode("node-1")).add(newNode("node-2")))
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
        var indexMetadataBuilder = IndexMetadata.builder(name)
            .settings(settings(Version.CURRENT))
            .numberOfShards(numberOfShards)
            .numberOfReplicas(0);

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
                    TestShardRouting.newShardRouting(
                        new ShardId(indexId, shardId),
                        assignment.getKey(),
                        null,
                        true,
                        ShardRoutingState.STARTED,
                        AllocationId.newInitializing(inSyncIds.get(shardId))
                    )
                );
                shardId++;
            }
        }
        routingTableBuilder.add(indexRoutingTableBuilder);
    }
}
