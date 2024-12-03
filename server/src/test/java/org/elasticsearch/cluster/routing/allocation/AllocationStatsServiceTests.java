/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalance;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardAssignment;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.test.ClusterServiceUtils;

import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasEntry;

public class AllocationStatsServiceTests extends ESAllocationTestCase {

    public void testShardStats() {

        var ingestLoadForecast = randomDoubleBetween(0, 10, true);
        var shardSizeForecast = randomNonNegativeLong();
        var currentShardSize = randomNonNegativeLong();

        var indexMetadata = IndexMetadata.builder("my-index")
            .settings(indexSettings(IndexVersion.current(), 1, 0))
            .indexWriteLoadForecast(ingestLoadForecast)
            .shardSizeInBytesForecast(shardSizeForecast)
            .build();
        var shardId = new ShardId(indexMetadata.getIndex(), 0);

        var state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(newNode("node-1")))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(
                RoutingTable.builder()
                    .add(
                        IndexRoutingTable.builder(indexMetadata.getIndex())
                            .addShard(newShardRouting(shardId, "node-1", true, ShardRoutingState.STARTED))
                            .build()
                    )
            )
            .build();

        var clusterInfo = new ClusterInfo(
            Map.of(),
            Map.of(),
            Map.of(ClusterInfo.shardIdentifierFromRouting(shardId, true), currentShardSize),
            Map.of(),
            Map.of(),
            Map.of()
        );

        var queue = new DeterministicTaskQueue();
        try (var clusterService = ClusterServiceUtils.createClusterService(state, queue.getThreadPool())) {
            var service = new AllocationStatsService(
                clusterService,
                () -> clusterInfo,
                createShardAllocator(),
                new NodeAllocationStatsProvider(TEST_WRITE_LOAD_FORECASTER, ClusterSettings.createBuiltInClusterSettings())
            );
            assertThat(
                service.stats(),
                allOf(
                    aMapWithSize(1),
                    hasEntry(
                        "node-1",
                        new NodeAllocationStats(1, -1, ingestLoadForecast, Math.max(shardSizeForecast, currentShardSize), currentShardSize)
                    )
                )
            );
        }
    }

    public void testRelocatingShardIsOnlyCountedOnceOnTargetNode() {

        var indexMetadata = IndexMetadata.builder("my-index").settings(indexSettings(IndexVersion.current(), 1, 0)).build();
        var state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(newNode("node-1")).add(newNode("node-2")))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(
                RoutingTable.builder()
                    .add(
                        IndexRoutingTable.builder(indexMetadata.getIndex())
                            .addShard(
                                shardRoutingBuilder(new ShardId(indexMetadata.getIndex(), 0), "node-1", true, ShardRoutingState.RELOCATING)
                                    .withRelocatingNodeId("node-2")
                                    .build()
                            )
                            .build()
                    )
            )
            .build();

        var queue = new DeterministicTaskQueue();
        try (var clusterService = ClusterServiceUtils.createClusterService(state, queue.getThreadPool())) {
            var service = new AllocationStatsService(
                clusterService,
                EmptyClusterInfoService.INSTANCE,
                createShardAllocator(),
                new NodeAllocationStatsProvider(TEST_WRITE_LOAD_FORECASTER, ClusterSettings.createBuiltInClusterSettings())
            );
            assertThat(
                service.stats(),
                allOf(
                    aMapWithSize(2),
                    hasEntry("node-1", new NodeAllocationStats(0, -1, 0, 0, 0)),
                    hasEntry("node-2", new NodeAllocationStats(1, -1, 0, 0, 0))
                )
            );
        }
    }

    public void testUndesiredShardCount() {

        var indexMetadata = IndexMetadata.builder("my-index").settings(indexSettings(IndexVersion.current(), 2, 0)).build();

        var state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(newNode("node-1")).add(newNode("node-2")).add(newNode("node-3")))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(
                RoutingTable.builder()
                    .add(
                        IndexRoutingTable.builder(indexMetadata.getIndex())
                            .addShard(newShardRouting(new ShardId(indexMetadata.getIndex(), 0), "node-1", true, ShardRoutingState.STARTED))
                            .addShard(newShardRouting(new ShardId(indexMetadata.getIndex(), 1), "node-3", true, ShardRoutingState.STARTED))
                            .build()
                    )
            )
            .build();

        var queue = new DeterministicTaskQueue();
        var threadPool = queue.getThreadPool();
        try (var clusterService = ClusterServiceUtils.createClusterService(state, threadPool)) {
            var service = new AllocationStatsService(
                clusterService,
                EmptyClusterInfoService.INSTANCE,
                new DesiredBalanceShardsAllocator(
                    ClusterSettings.createBuiltInClusterSettings(),
                    createShardAllocator(),
                    threadPool,
                    clusterService,
                    (innerState, strategy) -> innerState,
                    TelemetryProvider.NOOP,
                    EMPTY_NODE_ALLOCATION_STATS
                ) {
                    @Override
                    public DesiredBalance getDesiredBalance() {
                        return new DesiredBalance(
                            1,
                            Map.ofEntries(
                                Map.entry(new ShardId(indexMetadata.getIndex(), 0), new ShardAssignment(Set.of("node-1"), 1, 0, 0)),
                                Map.entry(new ShardId(indexMetadata.getIndex(), 1), new ShardAssignment(Set.of("node-2"), 1, 0, 0))
                            )
                        );
                    }
                },
                new NodeAllocationStatsProvider(TEST_WRITE_LOAD_FORECASTER, ClusterSettings.createBuiltInClusterSettings())
            );
            assertThat(
                service.stats(),
                allOf(
                    aMapWithSize(3),
                    hasEntry("node-1", new NodeAllocationStats(1, 0, 0, 0, 0)),
                    hasEntry("node-2", new NodeAllocationStats(0, 0, 0, 0, 0)),
                    hasEntry("node-3", new NodeAllocationStats(1, 1, 0, 0, 0)) // [my-index][1] should be allocated to [node-2]
                )
            );
        }
    }

    private ShardsAllocator createShardAllocator() {
        return new ShardsAllocator() {
            @Override
            public void allocate(RoutingAllocation allocation) {

            }

            @Override
            public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                return null;
            }
        };
    }
}
