/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource.EmptyStoreRecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.LocalShardsRecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.PeerRecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDeciderTests.DevNullClusterInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static java.util.Collections.emptySet;
import static org.elasticsearch.cluster.ClusterInfo.shardIdentifierFromRouting;
import static org.elasticsearch.cluster.routing.ExpectedShardSizeEstimator.getExpectedShardSize;
import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_STORE_TYPE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Unit tests for the DiskThresholdDecider
 */
public class DiskThresholdDeciderUnitTests extends ESAllocationTestCase {

    public void testCanAllocateUsesMaxAvailableSpace() {
        ClusterSettings nss = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        DiskThresholdDecider decider = new DiskThresholdDecider(Settings.EMPTY, nss);

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .build();

        final Index index = metadata.getProject().index("test").getIndex();

        ShardRouting test_0 = ShardRouting.newUnassigned(
            new ShardId(index, 0),
            true,
            EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"),
            ShardRouting.Role.DEFAULT
        );
        DiscoveryNode node_0 = DiscoveryNodeUtils.builder("node_0").roles(new HashSet<>(DiscoveryNodeRole.roles())).build();
        DiscoveryNode node_1 = DiscoveryNodeUtils.builder("node_1").roles(new HashSet<>(DiscoveryNodeRole.roles())).build();

        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(node_0).add(node_1)).build();

        // actual test -- after all that bloat :)
        Map<String, DiskUsage> leastAvailableUsages = new HashMap<>();
        leastAvailableUsages.put("node_0", new DiskUsage("node_0", "node_0", "_na_", 100, 0)); // all full
        leastAvailableUsages.put("node_1", new DiskUsage("node_1", "node_1", "_na_", 100, 0)); // all full

        Map<String, DiskUsage> mostAvailableUsage = new HashMap<>();
        // 20 - 99 percent since after allocation there must be at least 10% left and shard is 10byte
        mostAvailableUsage.put("node_0", new DiskUsage("node_0", "node_0", "_na_", 100, randomIntBetween(20, 100)));
        // this is weird and smells like a bug! it should be up to 20%?
        mostAvailableUsage.put("node_1", new DiskUsage("node_1", "node_1", "_na_", 100, randomIntBetween(0, 10)));

        final ClusterInfo clusterInfo = new ClusterInfo(
            leastAvailableUsages,
            mostAvailableUsage,
            Map.of("[test][0][p]", 10L), // 10 bytes,
            Map.of(),
            Map.of(),
            Map.of()
        );
        RoutingAllocation allocation = new RoutingAllocation(
            new AllocationDeciders(Collections.singleton(decider)),
            clusterState,
            clusterInfo,
            null,
            System.nanoTime()
        );
        allocation.debugDecision(true);
        Decision decision = decider.canAllocate(test_0, RoutingNodesHelper.routingNode("node_0", node_0), allocation);
        assertEquals(mostAvailableUsage.toString(), Decision.Type.YES, decision.type());
        assertThat(((Decision.Single) decision).getExplanation(), containsString("enough disk for shard on node"));
        decision = decider.canAllocate(test_0, RoutingNodesHelper.routingNode("node_1", node_1), allocation);
        assertEquals(mostAvailableUsage.toString(), Decision.Type.NO, decision.type());
        assertThat(
            ((Decision.Single) decision).getExplanation(),
            containsString(
                "the node is above the high watermark cluster setting [cluster.routing.allocation.disk.watermark.high=90%], "
                    + "having less than the minimum required"
            )
        );
    }

    private void doTestCannotAllocateDueToLackOfDiskResources(boolean testMaxHeadroom) {
        ClusterSettings nss = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        DiskThresholdDecider decider = new DiskThresholdDecider(Settings.EMPTY, nss);

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .build();

        final Index index = metadata.getProject().index("test").getIndex();

        ShardRouting test_0 = ShardRouting.newUnassigned(
            new ShardId(index, 0),
            true,
            EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"),
            ShardRouting.Role.DEFAULT
        );
        DiscoveryNode node_0 = DiscoveryNodeUtils.builder("node_0").roles(new HashSet<>(DiscoveryNodeRole.roles())).build();
        DiscoveryNode node_1 = DiscoveryNodeUtils.builder("node_1").roles(new HashSet<>(DiscoveryNodeRole.roles())).build();

        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(node_0).add(node_1)).build();

        // actual test -- after all that bloat :)

        final long totalBytes = testMaxHeadroom ? ByteSizeValue.ofGb(10000).getBytes() : 100;

        Map<String, DiskUsage> leastAvailableUsages = Map.of("node_0", new DiskUsage("node_0", "node_0", "_na_", totalBytes, 0)); // all
                                                                                                                                  // full
        final long freeBytes = testMaxHeadroom
            ? ByteSizeValue.ofGb(randomIntBetween(500, 10000)).getBytes()
            : randomLongBetween(50, totalBytes);
        Map<String, DiskUsage> mostAvailableUsage = Map.of("node_0", new DiskUsage("node_0", "node_0", "_na_", totalBytes, freeBytes));

        // way bigger than available space
        final long shardSize = randomLongBetween(totalBytes + 10, totalBytes * 10);
        ClusterInfo clusterInfo = new ClusterInfo(
            leastAvailableUsages,
            mostAvailableUsage,
            Map.of("[test][0][p]", shardSize),
            Map.of(),
            Map.of(),
            Map.of()
        );
        RoutingAllocation allocation = new RoutingAllocation(
            new AllocationDeciders(Collections.singleton(decider)),
            clusterState,
            clusterInfo,
            null,
            System.nanoTime()
        );
        allocation.debugDecision(true);
        Decision decision = decider.canAllocate(test_0, RoutingNodesHelper.routingNode("node_0", node_0), allocation);
        assertEquals(Decision.Type.NO, decision.type());

        double usedPercentage = 100.0 - (100.0 * freeBytes / totalBytes);

        assertThat(
            decision.getExplanation(),
            containsString(
                testMaxHeadroom
                    ? "allocating the shard to this node will bring the node above the high watermark cluster setting "
                        + "[cluster.routing.allocation.disk.watermark.high.max_headroom=150gb] "
                        + "and cause it to have less than the minimum required [150gb] of free space "
                        + "(free: ["
                        + ByteSizeValue.ofBytes(freeBytes)
                        + "], used: ["
                        + Strings.format1Decimals(usedPercentage, "%")
                        + "], estimated shard size: ["
                        + ByteSizeValue.ofBytes(shardSize)
                        + "])"
                    : "allocating the shard to this node will bring the node above the high watermark cluster setting "
                        + "[cluster.routing.allocation.disk.watermark.high=90%] "
                        + "and cause it to have less than the minimum required [10b] of free space "
                        + "(free: ["
                        + freeBytes
                        + "b], used: ["
                        + Strings.format1Decimals(usedPercentage, "%")
                        + "], estimated shard size: ["
                        + shardSize
                        + "b])"
            )
        );
    }

    public void testCannotAllocateDueToLackOfDiskResourcesWithPercentages() {
        doTestCannotAllocateDueToLackOfDiskResources(false);
    }

    public void testCannotAllocateDueToLackOfDiskResourcesWithMaxHeadroom() {
        doTestCannotAllocateDueToLackOfDiskResources(true);
    }

    private void doTestCanRemainUsesLeastAvailableSpace(boolean testMaxHeadroom) {
        ClusterSettings nss = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        DiskThresholdDecider decider = new DiskThresholdDecider(Settings.EMPTY, nss);
        Map<ClusterInfo.NodeAndShard, String> shardRoutingMap = new HashMap<>();

        DiscoveryNode node_0 = DiscoveryNodeUtils.builder("node_0").roles(new HashSet<>(DiscoveryNodeRole.roles())).build();
        DiscoveryNode node_1 = DiscoveryNodeUtils.builder("node_1").roles(new HashSet<>(DiscoveryNodeRole.roles())).build();

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .build();
        final IndexMetadata indexMetadata = metadata.getProject().index("test");

        ShardRouting test_0 = ShardRouting.newUnassigned(
            new ShardId(indexMetadata.getIndex(), 0),
            true,
            EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"),
            ShardRouting.Role.DEFAULT
        );
        test_0 = ShardRoutingHelper.initialize(test_0, node_0.getId());
        test_0 = ShardRoutingHelper.moveToStarted(test_0);
        shardRoutingMap.put(ClusterInfo.NodeAndShard.from(test_0), "/node0/least");

        ShardRouting test_1 = ShardRouting.newUnassigned(
            new ShardId(indexMetadata.getIndex(), 1),
            true,
            EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"),
            ShardRouting.Role.DEFAULT
        );
        test_1 = ShardRoutingHelper.initialize(test_1, node_1.getId());
        test_1 = ShardRoutingHelper.moveToStarted(test_1);
        shardRoutingMap.put(ClusterInfo.NodeAndShard.from(test_1), "/node1/least");

        ShardRouting test_2 = ShardRouting.newUnassigned(
            new ShardId(indexMetadata.getIndex(), 2),
            true,
            EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"),
            ShardRouting.Role.DEFAULT
        );
        test_2 = ShardRoutingHelper.initialize(test_2, node_1.getId());
        test_2 = ShardRoutingHelper.moveToStarted(test_2);
        shardRoutingMap.put(ClusterInfo.NodeAndShard.from(test_2), "/node1/most");

        ShardRouting test_3 = ShardRouting.newUnassigned(
            new ShardId(indexMetadata.getIndex(), 3),
            true,
            EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"),
            ShardRouting.Role.DEFAULT
        );
        test_3 = ShardRoutingHelper.initialize(test_3, node_1.getId());
        test_3 = ShardRoutingHelper.moveToStarted(test_3);
        // Intentionally not in the shardRoutingMap. We want to test what happens when we don't know where it is.

        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(indexMetadata).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();

        logger.info("--> adding two nodes");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(node_0).add(node_1)).build();

        // actual test -- after all that bloat :)

        final long totalBytes = testMaxHeadroom ? ByteSizeValue.ofGb(10000).getBytes() : 100;
        final long exactFreeSpaceForHighWatermark = testMaxHeadroom ? ByteSizeValue.ofGb(150).getBytes() : 10;
        final long exactFreeSpaceForBelowHighWatermark = exactFreeSpaceForHighWatermark - 1;
        final double exactUsedSpaceForBelowHighWatermark = 100.0 * (totalBytes - exactFreeSpaceForBelowHighWatermark) / totalBytes;
        final long ninetyPercentFreeSpace = (long) (totalBytes * 0.9);

        Map<String, DiskUsage> leastAvailableUsages = new HashMap<>();
        leastAvailableUsages.put("node_0", new DiskUsage("node_0", "node_0", "/node0/least", totalBytes, exactFreeSpaceForHighWatermark));
        leastAvailableUsages.put(
            "node_1",
            new DiskUsage("node_1", "node_1", "/node1/least", totalBytes, exactFreeSpaceForBelowHighWatermark)
        );

        Map<String, DiskUsage> mostAvailableUsage = new HashMap<>();
        mostAvailableUsage.put("node_0", new DiskUsage("node_0", "node_0", "/node0/most", totalBytes, ninetyPercentFreeSpace));
        mostAvailableUsage.put("node_1", new DiskUsage("node_1", "node_1", "/node1/most", totalBytes, ninetyPercentFreeSpace));

        Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", exactFreeSpaceForHighWatermark);
        shardSizes.put("[test][1][p]", exactFreeSpaceForHighWatermark);
        shardSizes.put("[test][2][p]", exactFreeSpaceForHighWatermark);

        final ClusterInfo clusterInfo = new ClusterInfo(
            leastAvailableUsages,
            mostAvailableUsage,
            shardSizes,
            Map.of(),
            shardRoutingMap,
            Map.of()
        );
        RoutingAllocation allocation = new RoutingAllocation(
            new AllocationDeciders(Collections.singleton(decider)),
            clusterState,
            clusterInfo,
            null,
            System.nanoTime()
        );
        allocation.debugDecision(true);
        Decision decision = decider.canRemain(indexMetadata, test_0, RoutingNodesHelper.routingNode("node_0", node_0), allocation);
        assertEquals(Decision.Type.YES, decision.type());
        assertThat(
            ((Decision.Single) decision).getExplanation(),
            containsString(
                "there is enough disk on this node for the shard to remain, free: ["
                    + ByteSizeValue.ofBytes(exactFreeSpaceForHighWatermark)
                    + "]"
            )
        );
        decision = decider.canRemain(indexMetadata, test_1, RoutingNodesHelper.routingNode("node_1", node_1), allocation);
        assertEquals(Decision.Type.NO, decision.type());
        assertThat(
            ((Decision.Single) decision).getExplanation(),
            containsString(
                "the shard cannot remain on this node because it is above the high watermark cluster setting "
                    + "[cluster.routing.allocation.disk.watermark.high"
                    + (testMaxHeadroom ? ".max_headroom=150gb" : "=90%")
                    + "] and there is less than the required ["
                    + ByteSizeValue.ofBytes(exactFreeSpaceForHighWatermark)
                    + "] free space on "
                    + "node, actual free: ["
                    + ByteSizeValue.ofBytes(exactFreeSpaceForBelowHighWatermark)
                    + "], actual used: ["
                    + Strings.format1Decimals(exactUsedSpaceForBelowHighWatermark, "%")
                    + "]"
            )
        );
        try {
            decider.canRemain(indexMetadata, test_0, RoutingNodesHelper.routingNode("node_1", node_1), allocation);
            fail("not allocated on this node");
        } catch (IllegalArgumentException ex) {
            // not allocated on that node
        }
        try {
            decider.canRemain(indexMetadata, test_1, RoutingNodesHelper.routingNode("node_0", node_0), allocation);
            fail("not allocated on this node");
        } catch (IllegalArgumentException ex) {
            // not allocated on that node
        }

        decision = decider.canRemain(indexMetadata, test_2, RoutingNodesHelper.routingNode("node_1", node_1), allocation);
        assertEquals("can stay since allocated on a different path with enough space", Decision.Type.YES, decision.type());
        assertThat(
            ((Decision.Single) decision).getExplanation(),
            containsString("this shard is not allocated on the most utilized disk and can remain")
        );

        decision = decider.canRemain(indexMetadata, test_2, RoutingNodesHelper.routingNode("node_1", node_1), allocation);
        assertEquals("can stay since we don't have information about this shard", Decision.Type.YES, decision.type());
        assertThat(
            ((Decision.Single) decision).getExplanation(),
            containsString("this shard is not allocated on the most utilized disk and can remain")
        );
    }

    public void testCanRemainUsesLeastAvailableSpaceWithPercentages() {
        doTestCanRemainUsesLeastAvailableSpace(false);
    }

    public void testCanRemainUsesLeastAvailableSpaceWithMaxHeadroom() {
        doTestCanRemainUsesLeastAvailableSpace(true);
    }

    public void testShardSizeAndRelocatingSize() {
        Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][r]", 10L);
        shardSizes.put("[test][1][r]", 100L);
        shardSizes.put("[test][2][r]", 1000L);
        shardSizes.put("[other][0][p]", 10000L);
        ClusterInfo info = new DevNullClusterInfo(Map.of(), Map.of(), shardSizes);
        Metadata.Builder metaBuilder = Metadata.builder();
        metaBuilder.put(
            IndexMetadata.builder("test")
                .settings(settings(IndexVersion.current()).put("index.uuid", "1234"))
                .numberOfShards(3)
                .numberOfReplicas(1)
        );
        metaBuilder.put(
            IndexMetadata.builder("other")
                .settings(settings(IndexVersion.current()).put("index.uuid", "5678"))
                .numberOfShards(1)
                .numberOfReplicas(1)
        );
        Metadata metadata = metaBuilder.build();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);
        routingTableBuilder.addAsNew(metadata.getProject().index("test"));
        routingTableBuilder.addAsNew(metadata.getProject().index("other"));
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTableBuilder.build())
            .build();
        RoutingAllocation allocation = new RoutingAllocation(null, clusterState, info, null, 0);

        final Index index = new Index("test", "1234");
        ShardRouting test_0 = ShardRouting.newUnassigned(
            new ShardId(index, 0),
            false,
            PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"),
            ShardRouting.Role.DEFAULT
        );
        test_0 = ShardRoutingHelper.initialize(test_0, "node1");
        test_0 = ShardRoutingHelper.moveToStarted(test_0);
        test_0 = ShardRoutingHelper.relocate(test_0, "node2");

        ShardRouting test_1 = ShardRouting.newUnassigned(
            new ShardId(index, 1),
            false,
            PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"),
            ShardRouting.Role.DEFAULT
        );
        test_1 = ShardRoutingHelper.initialize(test_1, "node2");
        test_1 = ShardRoutingHelper.moveToStarted(test_1);
        test_1 = ShardRoutingHelper.relocate(test_1, "node1");

        ShardRouting test_2 = ShardRouting.newUnassigned(
            new ShardId(index, 2),
            false,
            PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"),
            ShardRouting.Role.DEFAULT
        );
        test_2 = ShardRoutingHelper.initialize(test_2, "node1");
        test_2 = ShardRoutingHelper.moveToStarted(test_2);

        assertEquals(1000L, getExpectedShardSize(test_2, 0L, allocation));
        assertEquals(100L, getExpectedShardSize(test_1, 0L, allocation));
        assertEquals(10L, getExpectedShardSize(test_0, 0L, allocation));

        RoutingNode node = RoutingNodesHelper.routingNode(
            "node1",
            DiscoveryNodeUtils.builder("node1").roles(emptySet()).build(),
            test_0,
            test_1.getTargetRelocatingShard(),
            test_2
        );
        assertEquals(100L, sizeOfUnaccountedShards(allocation, node, false, "/dev/null"));
        assertEquals(90L, sizeOfUnaccountedShards(allocation, node, true, "/dev/null"));
        assertEquals(0L, sizeOfUnaccountedShards(allocation, node, true, "/dev/some/other/dev"));
        assertEquals(0L, sizeOfUnaccountedShards(allocation, node, true, "/dev/some/other/dev"));

        ShardRouting test_3 = ShardRouting.newUnassigned(
            new ShardId(index, 3),
            false,
            PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"),
            ShardRouting.Role.DEFAULT
        );
        test_3 = ShardRoutingHelper.initialize(test_3, "node1");
        test_3 = ShardRoutingHelper.moveToStarted(test_3);
        assertEquals(0L, getExpectedShardSize(test_3, 0L, allocation));

        boolean primary = randomBoolean();
        ShardRouting other_0 = ShardRouting.newUnassigned(
            new ShardId("other", "5678", 0),
            primary,
            primary ? EmptyStoreRecoverySource.INSTANCE : PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"),
            ShardRouting.Role.DEFAULT
        );
        other_0 = ShardRoutingHelper.initialize(other_0, "node2");
        other_0 = ShardRoutingHelper.moveToStarted(other_0);
        other_0 = ShardRoutingHelper.relocate(other_0, "node1");

        node = RoutingNodesHelper.routingNode(
            "node1",
            DiscoveryNodeUtils.builder("node1").roles(emptySet()).build(),
            test_0,
            test_1.getTargetRelocatingShard(),
            test_2,
            other_0.getTargetRelocatingShard()
        );
        assertEquals(10100L, sizeOfUnaccountedShards(allocation, node, false, "/dev/null"));
        assertEquals(10090L, sizeOfUnaccountedShards(allocation, node, true, "/dev/null"));
    }

    public void testTakesIntoAccountExpectedSizeForInitializingSearchableSnapshots() {

        var searchableSnapshotIndex = IndexMetadata.builder("searchable_snapshot")
            .settings(indexSettings(IndexVersion.current(), 3, 0).put(INDEX_STORE_TYPE_SETTING.getKey(), SEARCHABLE_SNAPSHOT_STORE_TYPE))
            .build();
        var regularIndex = IndexMetadata.builder("regular_index").settings(indexSettings(IndexVersion.current(), 1, 0)).build();

        String nodeId = "node1";
        var knownShardSizes = new HashMap<String, Long>();
        long unaccountedSearchableSnapshotSizes = 0;
        long relocatingShardsSizes = 0;

        var searchableSnapshotIndexRoutingTableBuilder = IndexRoutingTable.builder(searchableSnapshotIndex.getIndex());
        for (int i = 0; i < searchableSnapshotIndex.getNumberOfShards(); i++) {
            long expectedSize = randomLongBetween(10, 50);
            // a searchable snapshot shard without corresponding entry in cluster info
            ShardRouting startedShardWithExpectedSize = shardRoutingBuilder(
                new ShardId(searchableSnapshotIndex.getIndex(), i),
                nodeId,
                true,
                ShardRoutingState.STARTED
            ).withExpectedShardSize(expectedSize).build();
            searchableSnapshotIndexRoutingTableBuilder.addShard(startedShardWithExpectedSize);
            unaccountedSearchableSnapshotSizes += expectedSize;
        }
        var regularIndexRoutingTableBuilder = IndexRoutingTable.builder(regularIndex.getIndex());
        for (int i = 0; i < searchableSnapshotIndex.getNumberOfShards(); i++) {
            var shardSize = randomLongBetween(10, 50);
            // a shard relocating to this node
            ShardRouting initializingShard = shardRoutingBuilder(
                new ShardId(regularIndex.getIndex(), i),
                nodeId,
                true,
                ShardRoutingState.INITIALIZING
            ).withRecoverySource(PeerRecoverySource.INSTANCE).build();
            regularIndexRoutingTableBuilder.addShard(initializingShard);
            knownShardSizes.put(shardIdentifierFromRouting(initializingShard), shardSize);
            relocatingShardsSizes += shardSize;
        }

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(searchableSnapshotIndex, false).put(regularIndex, false))
            .routingTable(RoutingTable.builder().add(searchableSnapshotIndexRoutingTableBuilder).add(regularIndexRoutingTableBuilder))
            .nodes(DiscoveryNodes.builder().add(newNode(nodeId)).build())
            .build();

        RoutingAllocation allocation = new RoutingAllocation(
            null,
            clusterState,
            new DevNullClusterInfo(Map.of(), Map.of(), knownShardSizes),
            null,
            0
        );
        assertEquals(
            unaccountedSearchableSnapshotSizes + relocatingShardsSizes,
            sizeOfUnaccountedShards(allocation, clusterState.getRoutingNodes().node(nodeId), false, "/dev/null")
        );
    }

    private ShardRouting createShard(Index index, String nodeId, int i, int expectedSize) {
        var unassigned = ShardRouting.newUnassigned(
            new ShardId(index, i),
            true,
            EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"),
            ShardRouting.Role.DEFAULT
        );
        var initialized = ShardRoutingHelper.initialize(unassigned, nodeId, expectedSize);
        var started = ShardRoutingHelper.moveToStarted(initialized, expectedSize);
        return started;
    }

    public long sizeOfUnaccountedShards(RoutingAllocation allocation, RoutingNode node, boolean subtractShardsMovingAway, String dataPath) {
        return DiskThresholdDecider.sizeOfUnaccountedShards(
            node,
            subtractShardsMovingAway,
            dataPath,
            allocation.clusterInfo(),
            allocation.snapshotShardSizeInfo(),
            allocation.metadata(),
            allocation.globalRoutingTable(),
            allocation.unaccountedSearchableSnapshotSize(node)
        );
    }

    public void testSizeShrinkIndex() {
        Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", 10L);
        shardSizes.put("[test][1][p]", 100L);
        shardSizes.put("[test][2][p]", 500L);
        shardSizes.put("[test][3][p]", 500L);

        ClusterInfo info = new DevNullClusterInfo(Map.of(), Map.of(), shardSizes);
        Metadata.Builder metaBuilder = Metadata.builder();
        metaBuilder.put(
            IndexMetadata.builder("test")
                .settings(settings(IndexVersion.current()).put("index.uuid", "1234"))
                .numberOfShards(4)
                .numberOfReplicas(0)
        );
        metaBuilder.put(
            IndexMetadata.builder("target")
                .settings(
                    settings(IndexVersion.current()).put("index.uuid", "5678")
                        .put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME_KEY, "test")
                        .put(IndexMetadata.INDEX_RESIZE_SOURCE_UUID_KEY, "1234")
                )
                .numberOfShards(1)
                .numberOfReplicas(0)
        );
        metaBuilder.put(
            IndexMetadata.builder("target2")
                .settings(
                    settings(IndexVersion.current()).put("index.uuid", "9101112")
                        .put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME_KEY, "test")
                        .put(IndexMetadata.INDEX_RESIZE_SOURCE_UUID_KEY, "1234")
                )
                .numberOfShards(2)
                .numberOfReplicas(0)
        );
        Metadata metadata = metaBuilder.build();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);
        routingTableBuilder.addAsNew(metadata.getProject().index("test"));
        routingTableBuilder.addAsNew(metadata.getProject().index("target"));
        routingTableBuilder.addAsNew(metadata.getProject().index("target2"));
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTableBuilder.build())
            .build();

        AllocationService allocationService = createAllocationService();
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        clusterState = allocationService.reroute(clusterState, "foo", ActionListener.noop());

        clusterState = startShardsAndReroute(
            allocationService,
            clusterState,
            clusterState.getRoutingTable().index("test").shardsWithState(ShardRoutingState.UNASSIGNED)
        );

        RoutingAllocation allocation = new RoutingAllocation(null, clusterState, info, null, 0);

        final Index index = new Index("test", "1234");
        ShardRouting test_0 = ShardRouting.newUnassigned(
            new ShardId(index, 0),
            true,
            LocalShardsRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"),
            ShardRouting.Role.DEFAULT
        );
        test_0 = ShardRoutingHelper.initialize(test_0, "node1");
        test_0 = ShardRoutingHelper.moveToStarted(test_0);

        ShardRouting test_1 = ShardRouting.newUnassigned(
            new ShardId(index, 1),
            true,
            LocalShardsRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"),
            ShardRouting.Role.DEFAULT
        );
        test_1 = ShardRoutingHelper.initialize(test_1, "node2");
        test_1 = ShardRoutingHelper.moveToStarted(test_1);

        ShardRouting test_2 = ShardRouting.newUnassigned(
            new ShardId(index, 2),
            true,
            LocalShardsRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"),
            ShardRouting.Role.DEFAULT
        );
        test_2 = ShardRoutingHelper.initialize(test_2, "node1");

        ShardRouting test_3 = ShardRouting.newUnassigned(
            new ShardId(index, 3),
            true,
            LocalShardsRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"),
            ShardRouting.Role.DEFAULT
        );
        test_3 = ShardRoutingHelper.initialize(test_3, "node1");
        assertEquals(500L, getExpectedShardSize(test_3, 0L, allocation));
        assertEquals(500L, getExpectedShardSize(test_2, 0L, allocation));
        assertEquals(100L, getExpectedShardSize(test_1, 0L, allocation));
        assertEquals(10L, getExpectedShardSize(test_0, 0L, allocation));

        ShardRouting target = ShardRouting.newUnassigned(
            new ShardId(new Index("target", "5678"), 0),
            true,
            LocalShardsRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"),
            ShardRouting.Role.DEFAULT
        );
        assertEquals(1110L, getExpectedShardSize(target, 0L, allocation));

        ShardRouting target2 = ShardRouting.newUnassigned(
            new ShardId(new Index("target2", "9101112"), 0),
            true,
            LocalShardsRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"),
            ShardRouting.Role.DEFAULT
        );
        assertEquals(110L, getExpectedShardSize(target2, 0L, allocation));

        target2 = ShardRouting.newUnassigned(
            new ShardId(new Index("target2", "9101112"), 1),
            true,
            LocalShardsRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"),
            ShardRouting.Role.DEFAULT
        );
        assertEquals(1000L, getExpectedShardSize(target2, 0L, allocation));

        // check that the DiskThresholdDecider still works even if the source index has been deleted
        ClusterState clusterStateWithMissingSourceIndex = ClusterState.builder(clusterState)
            .metadata(Metadata.builder(metadata).remove("test"))
            .routingTable(RoutingTable.builder(clusterState.routingTable()).remove("test").build())
            .build();

        allocationService.reroute(clusterState, "foo", ActionListener.noop());
        RoutingAllocation allocationWithMissingSourceIndex = new RoutingAllocation(null, clusterStateWithMissingSourceIndex, info, null, 0);
        assertEquals(42L, getExpectedShardSize(target, 42L, allocationWithMissingSourceIndex));
        assertEquals(42L, getExpectedShardSize(target2, 42L, allocationWithMissingSourceIndex));
    }

    public void testDiskUsageWithRelocations() {
        assertThat(
            new DiskThresholdDecider.DiskUsageWithRelocations(new DiskUsage("n", "n", "/dev/null", 1000L, 1000L), 0).getFreeBytes(),
            equalTo(1000L)
        );
        assertThat(
            new DiskThresholdDecider.DiskUsageWithRelocations(new DiskUsage("n", "n", "/dev/null", 1000L, 1000L), 9).getFreeBytes(),
            equalTo(991L)
        );
        assertThat(
            new DiskThresholdDecider.DiskUsageWithRelocations(new DiskUsage("n", "n", "/dev/null", 1000L, 1000L), -9).getFreeBytes(),
            equalTo(1009L)
        );

        assertThat(
            new DiskThresholdDecider.DiskUsageWithRelocations(new DiskUsage("n", "n", "/dev/null", 1000L, 1000L), 0)
                .getFreeDiskAsPercentage(),
            equalTo(100.0)
        );
        assertThat(
            new DiskThresholdDecider.DiskUsageWithRelocations(new DiskUsage("n", "n", "/dev/null", 1000L, 500L), 0)
                .getFreeDiskAsPercentage(),
            equalTo(50.0)
        );
        assertThat(
            new DiskThresholdDecider.DiskUsageWithRelocations(new DiskUsage("n", "n", "/dev/null", 1000L, 500L), 100)
                .getFreeDiskAsPercentage(),
            equalTo(40.0)
        );

        assertThat(
            new DiskThresholdDecider.DiskUsageWithRelocations(new DiskUsage("n", "n", "/dev/null", 1000L, 1000L), 0)
                .getUsedDiskAsPercentage(),
            equalTo(0.0)
        );
        assertThat(
            new DiskThresholdDecider.DiskUsageWithRelocations(new DiskUsage("n", "n", "/dev/null", 1000L, 500L), 0)
                .getUsedDiskAsPercentage(),
            equalTo(50.0)
        );
        assertThat(
            new DiskThresholdDecider.DiskUsageWithRelocations(new DiskUsage("n", "n", "/dev/null", 1000L, 500L), 100)
                .getUsedDiskAsPercentage(),
            equalTo(60.0)
        );

        assertThat(
            new DiskThresholdDecider.DiskUsageWithRelocations(new DiskUsage("n", "n", "/dev/null", Long.MAX_VALUE, Long.MAX_VALUE), 0)
                .getFreeBytes(),
            equalTo(Long.MAX_VALUE)
        );
        assertThat(
            new DiskThresholdDecider.DiskUsageWithRelocations(new DiskUsage("n", "n", "/dev/null", Long.MAX_VALUE, Long.MAX_VALUE), 10)
                .getFreeBytes(),
            equalTo(Long.MAX_VALUE - 10)
        );
        assertThat(
            new DiskThresholdDecider.DiskUsageWithRelocations(new DiskUsage("n", "n", "/dev/null", Long.MAX_VALUE, Long.MAX_VALUE), -10)
                .getFreeBytes(),
            equalTo(Long.MAX_VALUE)
        );
    }

    public void testDecidesYesIfWatermarksIgnored() {
        ClusterSettings nss = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        DiskThresholdDecider decider = new DiskThresholdDecider(Settings.EMPTY, nss);

        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(IndexVersion.current()).put(DiskThresholdDecider.SETTING_IGNORE_DISK_WATERMARKS.getKey(), true))
                    .numberOfShards(1)
                    .numberOfReplicas(1)
            )
            .build();

        final Index index = metadata.getProject().index("test").getIndex();

        ShardRouting test_0 = ShardRouting.newUnassigned(
            new ShardId(index, 0),
            true,
            EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"),
            ShardRouting.Role.DEFAULT
        );
        DiscoveryNode node_0 = DiscoveryNodeUtils.builder("node_0").roles(new HashSet<>(DiscoveryNodeRole.roles())).build();
        DiscoveryNode node_1 = DiscoveryNodeUtils.builder("node_1").roles(new HashSet<>(DiscoveryNodeRole.roles())).build();

        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(node_0).add(node_1)).build();

        // actual test -- after all that bloat :)
        Map<String, DiskUsage> allFullUsages = new HashMap<>();
        allFullUsages.put("node_0", new DiskUsage("node_0", "node_0", "_na_", 100, 0)); // all full
        allFullUsages.put("node_1", new DiskUsage("node_1", "node_1", "_na_", 100, 0)); // all full

        final ClusterInfo clusterInfo = new ClusterInfo(
            allFullUsages,
            allFullUsages,
            Map.of("[test][0][p]", 10L),
            Map.of(),
            Map.of(),
            Map.of()
        );
        RoutingAllocation allocation = new RoutingAllocation(
            new AllocationDeciders(Collections.singleton(decider)),
            clusterState,
            clusterInfo,
            null,
            System.nanoTime()
        );
        allocation.debugDecision(true);
        final RoutingNode routingNode = RoutingNodesHelper.routingNode("node_0", node_0);
        Decision decision = decider.canAllocate(test_0, routingNode, allocation);
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(decision.getExplanation(), containsString("disk watermarks are ignored on this index"));

        decision = decider.canRemain(
            metadata.getProject().getIndexSafe(test_0.index()),
            test_0.initialize(node_0.getId(), null, 0L).moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE),
            routingNode,
            allocation
        );
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(decision.getExplanation(), containsString("disk watermarks are ignored on this index"));
    }

    public void testCannotForceAllocateOver100PercentUsage() {
        ClusterSettings nss = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        DiskThresholdDecider decider = new DiskThresholdDecider(Settings.EMPTY, nss);

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .build();

        final Index index = metadata.getProject().index("test").getIndex();

        ShardRouting test_0 = ShardRouting.newUnassigned(
            new ShardId(index, 0),
            true,
            EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"),
            ShardRouting.Role.DEFAULT
        );
        DiscoveryNode node_0 = DiscoveryNodeUtils.builder("node_0").roles(new HashSet<>(DiscoveryNodeRole.roles())).build();
        DiscoveryNode node_1 = DiscoveryNodeUtils.builder("node_1").roles(new HashSet<>(DiscoveryNodeRole.roles())).build();

        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(node_0).add(node_1)).build();

        // actual test -- after all that bloat :)
        Map<String, DiskUsage> leastAvailableUsages = Map.of("node_0", new DiskUsage("node_0", "node_0", "_na_", 100, 0)); // all full
        Map<String, DiskUsage> mostAvailableUsage = Map.of("node_0", new DiskUsage("node_0", "node_0", "_na_", 100, 0)); // all full

        Map<String, Long> shardSizes = new HashMap<>();
        // bigger than available space
        final long shardSize = randomIntBetween(1, 10);
        shardSizes.put("[test][0][p]", shardSize);
        ClusterInfo clusterInfo = new ClusterInfo(leastAvailableUsages, mostAvailableUsage, shardSizes, Map.of(), Map.of(), Map.of());
        RoutingAllocation allocation = new RoutingAllocation(
            new AllocationDeciders(Collections.singleton(decider)),
            clusterState,
            clusterInfo,
            null,
            System.nanoTime()
        );
        allocation.debugDecision(true);
        Decision decision = decider.canForceAllocateDuringReplace(test_0, RoutingNodesHelper.routingNode("node_0", node_0), allocation);
        assertEquals(Decision.Type.NO, decision.type());

        assertThat(
            decision.getExplanation(),
            containsString(
                "unable to force allocate shard to [node_0] during replacement, "
                    + "as allocating to this node would cause disk usage to exceed 100% (["
                    + shardSize
                    + "] bytes above available disk space)"
            )
        );
    }
}
