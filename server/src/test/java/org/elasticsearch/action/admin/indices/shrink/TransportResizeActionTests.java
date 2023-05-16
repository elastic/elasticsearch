/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.shrink;

import org.apache.lucene.index.IndexWriter;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.TestDiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.util.Collections;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;

public class TransportResizeActionTests extends ESTestCase {

    private ClusterState createClusterState(String name, int numShards, int numReplicas, Settings settings) {
        return createClusterState(name, numShards, numReplicas, numShards, settings);
    }

    private ClusterState createClusterState(String name, int numShards, int numReplicas, int numRoutingShards, Settings settings) {
        Metadata.Builder metaBuilder = Metadata.builder();
        IndexMetadata indexMetadata = IndexMetadata.builder(name)
            .settings(settings(Version.CURRENT).put(settings))
            .numberOfShards(numShards)
            .numberOfReplicas(numReplicas)
            .setRoutingNumShards(numRoutingShards)
            .build();
        metaBuilder.put(indexMetadata, false);
        Metadata metadata = metaBuilder.build();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);
        routingTableBuilder.addAsNew(metadata.index(name));

        RoutingTable routingTable = routingTableBuilder.build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTable)
            .blocks(ClusterBlocks.builder().addBlocks(indexMetadata))
            .build();
        return clusterState;
    }

    public void testErrorCondition() {
        IndexMetadata state = createClusterState(
            "source",
            randomIntBetween(2, 42),
            randomIntBetween(0, 10),
            Settings.builder().put("index.blocks.write", true).build()
        ).metadata().index("source");
        assertTrue(
            expectThrows(
                IllegalStateException.class,
                () -> TransportResizeAction.prepareCreateIndexRequest(
                    new ResizeRequest("target", "source"),
                    state,
                    "target",
                    new ResizeNumberOfShardsCalculator.ShrinkShardsCalculator(
                        new StoreStats(between(1, 100), between(0, 100), between(1, 100)),
                        (i) -> new DocsStats(Integer.MAX_VALUE, between(1, 1000), between(1, 100))
                    )
                )
            ).getMessage().startsWith("Can't merge index with more than [2147483519] docs - too many documents in shards ")
        );

        assertTrue(expectThrows(IllegalStateException.class, () -> {
            ResizeRequest req = new ResizeRequest("target", "source");
            req.getTargetIndexRequest().settings(Settings.builder().put("index.number_of_shards", 4));
            TransportResizeAction.prepareCreateIndexRequest(
                req,
                createClusterState("source", 8, 1, Settings.builder().put("index.blocks.write", true).build()).metadata().index("source"),
                "target",
                new ResizeNumberOfShardsCalculator.ShrinkShardsCalculator(
                    new StoreStats(between(1, 100), between(0, 100), between(1, 100)),
                    (i) -> i == 2 || i == 3 ? new DocsStats(Integer.MAX_VALUE / 2, between(1, 1000), between(1, 10000)) : null
                )
            );
        }).getMessage().startsWith("Can't merge index with more than [2147483519] docs - too many documents in shards "));

        IllegalArgumentException softDeletesError = expectThrows(IllegalArgumentException.class, () -> {
            ResizeRequest req = new ResizeRequest("target", "source");
            req.getTargetIndexRequest().settings(Settings.builder().put("index.soft_deletes.enabled", false));
            TransportResizeAction.prepareCreateIndexRequest(
                req,
                createClusterState(
                    "source",
                    8,
                    1,
                    Settings.builder().put("index.blocks.write", true).put("index.soft_deletes.enabled", true).build()
                ).metadata().index("source"),
                "target",
                new ResizeNumberOfShardsCalculator.ShrinkShardsCalculator(
                    new StoreStats(between(1, 100), between(0, 100), between(1, 100)),
                    (i) -> new DocsStats(between(10, 1000), between(1, 10), between(1, 10000))
                )
            );
        });
        assertThat(softDeletesError.getMessage(), equalTo("Can't disable [index.soft_deletes.enabled] setting on resize"));

        // create one that won't fail
        ClusterState clusterState = ClusterState.builder(
            createClusterState("source", randomIntBetween(2, 10), 0, Settings.builder().put("index.blocks.write", true).build())
        ).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        AllocationService service = new AllocationService(
            new AllocationDeciders(Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );

        RoutingTable routingTable = service.reroute(clusterState, "reroute", ActionListener.noop()).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        // now we start the shard
        routingTable = ESAllocationTestCase.startInitializingShardsAndReroute(service, clusterState, "source").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        TransportResizeAction.prepareCreateIndexRequest(
            new ResizeRequest("target", "source"),
            clusterState.metadata().index("source"),
            "target",
            new ResizeNumberOfShardsCalculator.ShrinkShardsCalculator(
                new StoreStats(between(1, 100), between(0, 100), between(1, 100)),
                (i) -> new DocsStats(between(1, 1000), between(1, 1000), between(0, 10000))
            )
        );
    }

    public void testPassNumRoutingShards() {
        ClusterState clusterState = ClusterState.builder(
            createClusterState("source", 1, 0, Settings.builder().put("index.blocks.write", true).build())
        ).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        AllocationService service = new AllocationService(
            new AllocationDeciders(Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );

        RoutingTable routingTable = service.reroute(clusterState, "reroute", ActionListener.noop()).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        // now we start the shard
        routingTable = ESAllocationTestCase.startInitializingShardsAndReroute(service, clusterState, "source").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        ResizeRequest resizeRequest = new ResizeRequest("target", "source");
        resizeRequest.setResizeType(ResizeType.SPLIT);
        resizeRequest.getTargetIndexRequest().settings(Settings.builder().put("index.number_of_shards", 2).build());
        IndexMetadata indexMetadata = clusterState.metadata().index("source");
        TransportResizeAction.prepareCreateIndexRequest(
            resizeRequest,
            indexMetadata,
            "target",
            new ResizeNumberOfShardsCalculator.SplitShardsCalculator()
        );

        resizeRequest.getTargetIndexRequest()
            .settings(
                Settings.builder().put("index.number_of_routing_shards", randomIntBetween(2, 10)).put("index.number_of_shards", 2).build()
            );
        TransportResizeAction.prepareCreateIndexRequest(
            resizeRequest,
            indexMetadata,
            "target",
            new ResizeNumberOfShardsCalculator.SplitShardsCalculator()
        );
    }

    public void testPassNumRoutingShardsAndFail() {
        int numShards = randomIntBetween(2, 100);
        ClusterState clusterState = ClusterState.builder(
            createClusterState("source", numShards, 0, numShards * 4, Settings.builder().put("index.blocks.write", true).build())
        ).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        AllocationService service = new AllocationService(
            new AllocationDeciders(Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );

        RoutingTable routingTable = service.reroute(clusterState, "reroute", ActionListener.noop()).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        // now we start the shard
        routingTable = ESAllocationTestCase.startInitializingShardsAndReroute(service, clusterState, "source").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        ResizeRequest resizeRequest = new ResizeRequest("target", "source");
        resizeRequest.setResizeType(ResizeType.SPLIT);
        resizeRequest.getTargetIndexRequest().settings(Settings.builder().put("index.number_of_shards", numShards * 2).build());
        TransportResizeAction.prepareCreateIndexRequest(
            resizeRequest,
            clusterState.metadata().index("source"),
            "target",
            new ResizeNumberOfShardsCalculator.SplitShardsCalculator()
        );

        resizeRequest.getTargetIndexRequest()
            .settings(
                Settings.builder().put("index.number_of_shards", numShards * 2).put("index.number_of_routing_shards", numShards * 2).build()
            );
        ClusterState finalState = clusterState;
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> TransportResizeAction.prepareCreateIndexRequest(
                resizeRequest,
                finalState.metadata().index("source"),
                "target",
                new ResizeNumberOfShardsCalculator.SplitShardsCalculator()
            )
        );
        assertEquals("cannot provide index.number_of_routing_shards on resize", iae.getMessage());
    }

    public void testShrinkIndexSettings() {
        String indexName = randomAlphaOfLength(10);
        // create one that won't fail
        ClusterState clusterState = ClusterState.builder(
            createClusterState(indexName, randomIntBetween(2, 10), 0, Settings.builder().put("index.blocks.write", true).build())
        ).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        AllocationService service = new AllocationService(
            new AllocationDeciders(Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );

        RoutingTable routingTable = service.reroute(clusterState, "reroute", ActionListener.noop()).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        // now we start the shard
        routingTable = ESAllocationTestCase.startInitializingShardsAndReroute(service, clusterState, indexName).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        int numSourceShards = clusterState.metadata().index(indexName).getNumberOfShards();
        DocsStats stats = new DocsStats(between(0, (IndexWriter.MAX_DOCS) / numSourceShards), between(1, 1000), between(1, 10000));
        ResizeRequest target = new ResizeRequest("target", indexName);
        final ActiveShardCount activeShardCount = randomBoolean() ? ActiveShardCount.ALL : ActiveShardCount.ONE;
        target.setWaitForActiveShards(activeShardCount);
        CreateIndexClusterStateUpdateRequest request = TransportResizeAction.prepareCreateIndexRequest(
            target,
            clusterState.metadata().index(indexName),
            "target",
            new ResizeNumberOfShardsCalculator.ShrinkShardsCalculator(
                new StoreStats(between(1, 100), between(0, 100), between(1, 100)),
                (i) -> stats
            )
        );
        assertNotNull(request.recoverFrom());
        assertEquals(indexName, request.recoverFrom().getName());
        assertEquals("1", request.settings().get("index.number_of_shards"));
        assertEquals("shrink_index", request.cause());
        assertEquals(request.waitForActiveShards(), activeShardCount);
    }

    public void testShrinkWithMaxPrimaryShardSize() {
        int sourceIndexShardsNum = randomIntBetween(2, 42);
        IndexMetadata state = createClusterState(
            "source",
            sourceIndexShardsNum,
            randomIntBetween(0, 10),
            Settings.builder().put("index.blocks.write", true).build()
        ).metadata().index("source");
        ResizeRequest resizeRequest = new ResizeRequest("target", "source");
        resizeRequest.setMaxPrimaryShardSize(ByteSizeValue.ofBytes(10));
        resizeRequest.getTargetIndexRequest().settings(Settings.builder().put("index.number_of_shards", 2).build());
        assertTrue(
            expectThrows(
                IllegalArgumentException.class,
                () -> TransportResizeAction.prepareCreateIndexRequest(
                    resizeRequest,
                    state,
                    "target",
                    new ResizeNumberOfShardsCalculator.ShrinkShardsCalculator(
                        new StoreStats(between(1, 100), between(0, 100), between(1, 100)),
                        (i) -> new DocsStats(Integer.MAX_VALUE, between(1, 1000), between(1, 100))
                    )
                )
            ).getMessage().startsWith("Cannot set both index.number_of_shards and max_primary_shard_size for the target index")
        );

        // create one that won't fail
        ClusterState clusterState = ClusterState.builder(
            createClusterState("source", 10, 0, Settings.builder().put("index.blocks.write", true).build())
        ).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        AllocationService service = new AllocationService(
            new AllocationDeciders(Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );

        RoutingTable routingTable = service.reroute(clusterState, "reroute", ActionListener.noop()).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        // now we start the shard
        routingTable = ESAllocationTestCase.startInitializingShardsAndReroute(service, clusterState, "source").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        int numSourceShards = clusterState.metadata().index("source").getNumberOfShards();
        DocsStats stats = new DocsStats(between(0, (IndexWriter.MAX_DOCS) / numSourceShards), between(1, 1000), between(1, 10000));

        // each shard's storage will not be greater than the `max_primary_shard_size`
        ResizeRequest target1 = new ResizeRequest("target", "source");
        target1.setMaxPrimaryShardSize(ByteSizeValue.ofBytes(2));
        StoreStats storeStats = new StoreStats(10, between(0, 100), between(1, 100));
        final int targetIndexShardsNum1 = 5;
        final ActiveShardCount activeShardCount1 = ActiveShardCount.from(targetIndexShardsNum1);
        target1.setWaitForActiveShards(targetIndexShardsNum1);

        CreateIndexClusterStateUpdateRequest request1 = TransportResizeAction.prepareCreateIndexRequest(
            target1,
            clusterState.metadata().index("source"),
            "target",
            new ResizeNumberOfShardsCalculator.ShrinkShardsCalculator(storeStats, (i) -> stats)
        );
        assertNotNull(request1.recoverFrom());
        assertEquals("source", request1.recoverFrom().getName());
        assertEquals(String.valueOf(targetIndexShardsNum1), request1.settings().get("index.number_of_shards"));
        assertEquals("shrink_index", request1.cause());
        assertEquals(request1.waitForActiveShards(), activeShardCount1);

        // if `max_primary_shard_size` is less than the single shard size of the source index,
        // the shards number of the target index will be equal to the source index's shards number
        ResizeRequest target2 = new ResizeRequest("target2", "source");
        target2.setMaxPrimaryShardSize(ByteSizeValue.ofBytes(1));
        StoreStats storeStats2 = new StoreStats(100, between(0, 100), between(1, 100));
        final int targetIndexShardsNum2 = 10;
        final ActiveShardCount activeShardCount2 = ActiveShardCount.from(targetIndexShardsNum2);
        target2.setWaitForActiveShards(activeShardCount2);

        CreateIndexClusterStateUpdateRequest request2 = TransportResizeAction.prepareCreateIndexRequest(
            target2,
            clusterState.metadata().index("source"),
            "target",
            new ResizeNumberOfShardsCalculator.ShrinkShardsCalculator(storeStats2, (i) -> stats)
        );
        assertNotNull(request2.recoverFrom());
        assertEquals("source", request2.recoverFrom().getName());
        assertEquals(String.valueOf(targetIndexShardsNum2), request2.settings().get("index.number_of_shards"));
        assertEquals("shrink_index", request2.cause());
        assertEquals(request2.waitForActiveShards(), activeShardCount2);
    }

    private DiscoveryNode newNode(String nodeId) {
        return TestDiscoveryNode.create(
            nodeId,
            buildNewFakeTransportAddress(),
            emptyMap(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE)
        );
    }

}
