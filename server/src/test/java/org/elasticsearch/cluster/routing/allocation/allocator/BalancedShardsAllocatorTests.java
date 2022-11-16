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
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
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
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
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
        RoutingTable initialRoutingTable = RoutingTable.builder(clusterState.routingTable()).addAsNew(metadata.index(index)).build();
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

    public void testBalanceByShardLoad() {

        var smallIndices = IntStream.range(1, 5)
            .mapToObj(i -> IndexMetadata.builder("small-index-" + i))
            .map(builder -> builder.settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
            .map(builder -> builder.indexWriteLoadForecast(randomIngestLoad(1.5)));

        var heavyIndex = IndexMetadata.builder("heavy-index")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .indexWriteLoadForecast(8.0);

        var clusterState = stateWithStartedIndices(Stream.concat(smallIndices, Stream.of(heavyIndex)).toList());

        var testWriteLoadForecaster = new WriteLoadForecaster() {
            @Override
            public Metadata.Builder withWriteLoadForecastForWriteIndex(String dataStreamName, Metadata.Builder metadata) {
                throw new UnsupportedOperationException("Not required for test");
            }

            @Override
            @SuppressForbidden(reason = "This is required to test balancing by ingest load")
            public OptionalDouble getForecastedWriteLoad(IndexMetadata indexMetadata) {
                return indexMetadata.getForecastedWriteLoad();
            }
        };

        var settings = Settings.EMPTY;
        var allocator = new BalancedShardsAllocator(
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            testWriteLoadForecaster,
            EmptyClusterInfoService.INSTANCE
        );
        var allocation = createRoutingAllocation(clusterState);
        allocator.allocate(allocation);

        assertThat(allocation.metadata().getTotalNumberOfShards(), allOf(greaterThanOrEqualTo(3), lessThanOrEqualTo(5)));
        for (RoutingNode routingNode : allocation.routingNodes()) {
            var nodeIngestLoad = 0.0;
            for (ShardRouting shardRouting : routingNode) {
                if (shardRouting.started() || shardRouting.initializing()) { // count load from the target node when relocating
                    var indexMetadata = clusterState.metadata().index(shardRouting.index());
                    nodeIngestLoad += testWriteLoadForecaster.getForecastedWriteLoad(indexMetadata).orElse(0.0);
                }
            }
            assertThat(nodeIngestLoad, lessThanOrEqualTo(8.0 + 1.5));
        }
    }

    private Double randomIngestLoad(double max) {
        return switch (randomInt(3)) {
            case 0 -> null;
            case 1 -> 0.0;
            default -> randomDoubleBetween(1.0, max, true);
        };
    }

    public void testBalanceByDiskUsage() {

        var smallIndices = IntStream.range(1, 5)
            .mapToObj(i -> IndexMetadata.builder("small-index-" + i))
            .map(builder -> builder.settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
            .map(builder -> builder.shardSizeInBytesForecast(ByteSizeValue.ofGb(1).getBytes()));

        var heavyIndex = IndexMetadata.builder("heavy-index")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .shardSizeInBytesForecast(ByteSizeValue.ofGb(8).getBytes());

        var clusterState = stateWithStartedIndices(Stream.concat(smallIndices, Stream.of(heavyIndex)).toList());

        var allocator = new BalancedShardsAllocator(
            Settings.builder().put("cluster.routing.allocation.balance.disk_usage", "1e-9").build()
        );
        var allocation = createRoutingAllocation(clusterState);
        allocator.allocate(allocation);

        assertThat(allocation.metadata().getTotalNumberOfShards(), allOf(greaterThanOrEqualTo(3), lessThanOrEqualTo(5)));
        for (RoutingNode routingNode : allocation.routingNodes()) {
            var nodeDiskUsage = 0L;
            for (ShardRouting shardRouting : routingNode) {
                if (shardRouting.started() || shardRouting.initializing()) { // count load from the target node when relocating
                    var indexMetadata = clusterState.metadata().index(shardRouting.index());
                    nodeDiskUsage += indexMetadata.getForecastedShardSizeInBytes().orElse(0L);
                }
            }
            assertThat(nodeDiskUsage, lessThanOrEqualTo(ByteSizeValue.ofGb(8 + 1).getBytes()));
        }
    }

    /**
     * {@see https://github.com/elastic/elasticsearch/issues/88384}
     */
    public void testRebalanceImprovesTheBalanceOfTheShards() {
        var discoveryNodesBuilder = DiscoveryNodes.builder();
        for (int node = 0; node < 3; node++) {
            discoveryNodesBuilder.add(createNode("node-" + node));
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

    private static ClusterState stateWithStartedIndices(List<IndexMetadata.Builder> indices) {
        var metadataBuilder = Metadata.builder();
        var routingTableBuilder = RoutingTable.builder();
        for (var index : indices) {
            var inSyncId = UUIDs.randomBase64UUID(random());
            var build = index.putInSyncAllocationIds(0, Set.of(inSyncId)).build();
            metadataBuilder.put(build, false);
            routingTableBuilder.add(
                IndexRoutingTable.builder(build.getIndex())
                    .addShard(
                        TestShardRouting.newShardRouting(
                            new ShardId(build.getIndex(), 0),
                            randomFrom("node-1", "node-2"),
                            null,
                            true,
                            ShardRoutingState.STARTED,
                            AllocationId.newInitializing(inSyncId)
                        )
                    )
            );
        }

        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(createNode("node-1")).add(createNode("node-2")))
            .metadata(metadataBuilder)
            .routingTable(routingTableBuilder)
            .build();
    }

    private static DiscoveryNode createNode(String nodeId) {
        return new DiscoveryNode(
            nodeId,
            nodeId,
            UUIDs.randomBase64UUID(random()),
            buildNewFakeTransportAddress(),
            Map.of(),
            DiscoveryNodeRole.roles(),
            Version.CURRENT
        );
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
            .settings(
                Settings.builder()
                    .put("index.number_of_shards", numberOfShards)
                    .put("index.number_of_replicas", 0)
                    .put("index.version.created", Version.CURRENT)
                    .build()
            );

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
