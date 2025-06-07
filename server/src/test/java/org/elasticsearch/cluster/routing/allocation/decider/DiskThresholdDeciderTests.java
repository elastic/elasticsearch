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
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.GlobalRoutingTableTestHelper;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.elasticsearch.cluster.routing.UnassignedInfo.Reason;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.snapshots.InternalSnapshotsInfoService.SnapshotShard;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.snapshots.SnapshotsInfoService;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.routing.RoutingNodesHelper.assignedShardsIn;
import static org.elasticsearch.cluster.routing.RoutingNodesHelper.numberOfShardsWithState;
import static org.elasticsearch.cluster.routing.RoutingNodesHelper.shardsWithState;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING;
import static org.elasticsearch.common.settings.ClusterSettings.createBuiltInClusterSettings;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;
import static org.hamcrest.Matchers.sameInstance;

public class DiskThresholdDeciderTests extends ESAllocationTestCase {

    private void doTestDiskThreshold(boolean testMaxHeadroom) {
        Settings.Builder diskSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), 0.7)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), 0.8);
        if (testMaxHeadroom) {
            diskSettings = diskSettings.put(
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING.getKey(),
                ByteSizeValue.ofGb(200).toString()
            )
                .put(
                    DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING.getKey(),
                    ByteSizeValue.ofGb(150).toString()
                );
        }

        Map<String, DiskUsage> usages = new HashMap<>();
        final long totalBytes = testMaxHeadroom ? ByteSizeValue.ofGb(10000).getBytes() : 100;
        final long exactFreeSpaceForHighWatermark = testMaxHeadroom ? ByteSizeValue.ofGb(150).getBytes() : 10;
        usages.put("node1", new DiskUsage("node1", "node1", "/dev/null", totalBytes, exactFreeSpaceForHighWatermark));
        usages.put(
            "node2",
            new DiskUsage("node2", "node2", "/dev/null", totalBytes, testMaxHeadroom ? ByteSizeValue.ofGb(350).getBytes() : 35)
        );
        usages.put(
            "node3",
            new DiskUsage("node3", "node3", "/dev/null", totalBytes, testMaxHeadroom ? ByteSizeValue.ofGb(600).getBytes() : 60)
        );
        usages.put(
            "node4",
            new DiskUsage("node4", "node4", "/dev/null", totalBytes, testMaxHeadroom ? ByteSizeValue.ofGb(800).getBytes() : 80)
        );

        Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", exactFreeSpaceForHighWatermark);
        shardSizes.put("[test][0][r]", exactFreeSpaceForHighWatermark);
        final ClusterInfo clusterInfo = new DevNullClusterInfo(usages, usages, shardSizes);

        AllocationService strategy = createAllocationService(clusterInfo, createDiskThresholdDecider(diskSettings.build()));

        var indexMetadata = IndexMetadata.builder("test")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata, false).build())
            .routingTable(RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(indexMetadata).build())
            .build();

        logger.info("--> adding two nodes");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        logShardStates(clusterState);

        // Primary shard should be initializing, replica should not
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(1));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logShardStates(clusterState);
        // Assert that we're able to start the primary
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(1));
        // Assert that node1 didn't get any shards because its disk usage is too high
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(0));

        logger.info("--> start the shards (replicas)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logShardStates(clusterState);
        // Assert that the replica couldn't be started since node1 doesn't have enough space
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(1));

        logger.info("--> adding node3");

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        logShardStates(clusterState);
        // Assert that the replica is initialized now that node3 is available with enough space
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(1));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.INITIALIZING).size(), equalTo(1));

        logger.info("--> start the shards (replicas)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logShardStates(clusterState);
        // Assert that the replica couldn't be started since node1 doesn't have enough space
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(0));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));

        logger.info("--> changing decider settings");

        if (testMaxHeadroom) {
            // Set the low max headroom to 250GB
            // Set the high max headroom to 150GB
            // node2 (with 200GB free space) now should not have new shards allocated to it, but shards can remain
            diskSettings = Settings.builder()
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING.getKey(), ByteSizeValue.ofGb(250))
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING.getKey(), ByteSizeValue.ofGb(150));
        } else {
            // Set the low threshold to 60 instead of 70
            // Set the high threshold to 70 instead of 80
            // node2 (with 75% used space) now should not have new shards allocated to it, but shards can remain
            diskSettings = Settings.builder()
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "60%")
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), 0.7);
        }
        strategy = createAllocationService(clusterInfo, createDiskThresholdDecider(diskSettings.build()));

        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        logShardStates(clusterState);

        // Shards remain started
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(0));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));

        logger.info("--> changing settings again");

        if (testMaxHeadroom) {
            // Set the low max headroom to 500GB
            // Set the high max headroom to 400GB
            // node2 (with 200GB free space) now should not have new shards allocated to it, and shards cannot remain
            // Note that node3 (with 500GB free space) should not receive the shard so it does not get over the high threshold
            diskSettings = Settings.builder()
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING.getKey(), ByteSizeValue.ofGb(500))
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING.getKey(), ByteSizeValue.ofGb(400));
        } else {
            // Set the low threshold to 50 instead of 60
            // Set the high threshold to 60 instead of 70
            // node2 (with 75 used) now should not have new shards allocated to it, and shards cannot remain
            // Note that node3 (with 50% used space) should not receive the shard so it does not get over the high threshold
            diskSettings = Settings.builder()
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), 0.5)
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), 0.6);
        }
        strategy = createAllocationService(clusterInfo, createDiskThresholdDecider(diskSettings.build()));

        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        logShardStates(clusterState);
        // Shards remain started
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(0));
        // Shard hasn't been moved off of node2 yet because there's nowhere for it to go
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));

        logger.info("--> adding node4");

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node4"))).build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        logShardStates(clusterState);
        // Shards remain started
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(1));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(1));

        logger.info("--> apply INITIALIZING shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logShardStates(clusterState);
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(0));
        // Node4 is available now, so the shard is moved off of node2
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node4").size(), equalTo(1));
    }

    public void testDiskThresholdWithPercentages() {
        doTestDiskThreshold(false);
    }

    public void testDiskThresholdWithMaxHeadroom() {
        doTestDiskThreshold(true);
    }

    public void testDiskThresholdWithAbsoluteSizes() {
        Settings diskSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "30b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "9b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "5b")
            .build();

        Map<String, DiskUsage> usages = new HashMap<>();
        usages.put("node1", new DiskUsage("node1", "n1", "/dev/null", 100, 10)); // 90% used
        usages.put("node2", new DiskUsage("node2", "n2", "/dev/null", 100, 10)); // 90% used
        usages.put("node3", new DiskUsage("node3", "n3", "/dev/null", 100, 60)); // 40% used
        usages.put("node4", new DiskUsage("node4", "n4", "/dev/null", 100, 80)); // 20% used
        usages.put("node5", new DiskUsage("node5", "n5", "/dev/null", 100, 85)); // 15% used

        Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", 10L); // 10 bytes
        shardSizes.put("[test][0][r]", 10L);
        final ClusterInfo clusterInfo = new DevNullClusterInfo(usages, usages, shardSizes);

        AllocationService strategy = createAllocationService(clusterInfo, createDiskThresholdDecider(diskSettings));

        var indexMetadata = IndexMetadata.builder("test")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(2)
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata, false).build())
            .routingTable(RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(indexMetadata).build())
            .build();

        logger.info("--> adding node1 and node2 node");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();

        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        logShardStates(clusterState);

        // Primary should initialize, even though both nodes are over the limit initialize
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(1));

        String nodeWithPrimary, nodeWithoutPrimary;
        if (clusterState.getRoutingNodes().node("node1").size() == 1) {
            nodeWithPrimary = "node1";
            nodeWithoutPrimary = "node2";
        } else {
            nodeWithPrimary = "node2";
            nodeWithoutPrimary = "node1";
        }
        logger.info("--> nodeWithPrimary: {}", nodeWithPrimary);
        logger.info("--> nodeWithoutPrimary: {}", nodeWithoutPrimary);

        // Make node without the primary now habitable to replicas
        usages = new HashMap<>(usages);
        usages.put(nodeWithoutPrimary, new DiskUsage(nodeWithoutPrimary, "", "/dev/null", 100, 35)); // 65% used
        final ClusterInfo clusterInfo2 = new DevNullClusterInfo(usages, usages, shardSizes);

        strategy = createAllocationService(clusterInfo2, createDiskThresholdDecider(diskSettings));

        logShardStates(clusterState);

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        // Now the replica should be able to initialize
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(1));

        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logShardStates(clusterState);
        // Assert that we're able to start the replica
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(2));
        // Assert that node1 got a single shard (the primary), even though its disk usage is too high
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        // Assert that node2 got a single shard (a replica)
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));

        // Assert that one replica is still unassigned
        // assertThat(clusterState.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(1));

        logger.info("--> adding node3");

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        logShardStates(clusterState);
        // Assert that the replica is initialized now that node3 is available with enough space
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(2));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.INITIALIZING).size(), equalTo(1));

        logger.info("--> start the shards (replicas)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logShardStates(clusterState);
        // Assert that all replicas could be started
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(3));
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));

        logger.info("--> changing decider settings");

        // Set the low threshold to 60 instead of 70
        // Set the high threshold to 70 instead of 80
        // node2 now should not have new shards allocated to it, but shards can remain
        diskSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "40b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "30b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "20b")
            .build();
        strategy = createAllocationService(clusterInfo2, createDiskThresholdDecider(diskSettings));

        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        logShardStates(clusterState);

        // Shards remain started
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(3));
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));

        logger.info("--> changing settings again");

        // Set the low threshold to 50 instead of 60
        // Set the high threshold to 60 instead of 70
        // node2 now should not have new shards allocated to it, and shards cannot remain
        diskSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "50b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "40b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "30b")
            .build();
        strategy = createAllocationService(clusterInfo2, createDiskThresholdDecider(diskSettings));

        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        logShardStates(clusterState);
        // Shards remain started
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(3));
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        // Shard hasn't been moved off of node2 yet because there's nowhere for it to go
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));

        logger.info("--> adding node4");

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node4"))).build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        logShardStates(clusterState);
        // Shards remain started
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(2));
        // One shard is relocating off of node1
        assertThat(shardsWithState(clusterState.getRoutingNodes(), RELOCATING).size(), equalTo(1));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(1));

        logger.info("--> apply INITIALIZING shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logShardStates(clusterState);
        assertThat(
            clusterState.getRoutingNodes().node(nodeWithPrimary).size() + clusterState.getRoutingNodes().node(nodeWithoutPrimary).size(),
            equalTo(1)
        );
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node4").size(), equalTo(1));

        logger.info("--> adding node5");

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node5"))).build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        logShardStates(clusterState);
        // Shards remain started on node3 and node4
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(2));
        // One shard is relocating off of node2 now
        assertThat(shardsWithState(clusterState.getRoutingNodes(), RELOCATING).size(), equalTo(1));
        // Initializing on node5
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(1));

        logger.info("--> apply INITIALIZING shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> final cluster state:");
        logShardStates(clusterState);
        // Node1 still has no shards because it has no space for them
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(0));
        // Node5 is available now, so the shard is moved off of node2
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(0));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node4").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node5").size(), equalTo(1));
    }

    private void doTestDiskThresholdWithShardSizes(boolean testMaxHeadroom) {
        Settings.Builder diskSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), 0.7)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "71%");
        if (testMaxHeadroom) {
            diskSettings = diskSettings.put(
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING.getKey(),
                ByteSizeValue.ofGb(200).toString()
            )
                .put(
                    DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING.getKey(),
                    ByteSizeValue.ofGb(199).toString()
                );
        }

        Map<String, DiskUsage> usages = new HashMap<>();
        final long totalBytes = testMaxHeadroom ? ByteSizeValue.ofGb(10000).getBytes() : 100;
        // below but close to low watermark
        usages.put(
            "node1",
            new DiskUsage("node1", "n1", "/dev/null", totalBytes, testMaxHeadroom ? ByteSizeValue.ofGb(201).getBytes() : 31)
        );
        // almost fully used
        usages.put("node2", new DiskUsage("node2", "n2", "/dev/null", totalBytes, testMaxHeadroom ? ByteSizeValue.ofGb(1).getBytes() : 1));

        final ClusterInfo clusterInfo = new DevNullClusterInfo(
            usages,
            usages,
            Map.of("[test][0][p]", testMaxHeadroom ? ByteSizeValue.ofGb(10).getBytes() : 10L)
        );

        AllocationService strategy = createAllocationService(clusterInfo, createDiskThresholdDecider(diskSettings.build()));

        var indexMetadata = IndexMetadata.builder("test")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata, false).build())
            .routingTable(RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(indexMetadata).build())
            .build();
        logger.info("--> adding node1");
        // node2 is added because DiskThresholdDecider automatically ignore single-node clusters
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        logShardStates(clusterState);

        // Shard can't be allocated to node1 (or node2) because it would cause too much usage
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(0));
        // No shards are started, no nodes have enough disk for allocation
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(0));
    }

    public void testDiskThresholdWithShardSizesWithPercentages() {
        doTestDiskThresholdWithShardSizes(false);
    }

    public void testDiskThresholdWithShardSizesWithMaxHeadroom() {
        doTestDiskThresholdWithShardSizes(true);
    }

    public void testUnknownDiskUsage() {
        Settings diskSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), 0.7)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), 0.85)
            .build();

        Map<String, DiskUsage> usages = new HashMap<>();
        usages.put("node2", new DiskUsage("node2", "node2", "/dev/null", 100, 50)); // 50% used
        usages.put("node3", new DiskUsage("node3", "node3", "/dev/null", 100, 0));  // 100% used

        Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", 10L); // 10 bytes
        shardSizes.put("[test][0][r]", 10L); // 10 bytes
        final ClusterInfo clusterInfo = new DevNullClusterInfo(usages, usages, shardSizes);

        AllocationService strategy = createAllocationService(clusterInfo, createDiskThresholdDecider(diskSettings));

        var indexMetadata = IndexMetadata.builder("test")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata, false).build())
            .routingTable(RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(indexMetadata).build())
            .build();
        logger.info("--> adding node1");
        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node3")) // node3 is added because DiskThresholdDecider
                                                                                     // automatically ignore single-node clusters
            )
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        // Shard can be allocated to node1, even though it only has 25% free,
        // because it's a primary that's never been allocated before
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(1));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        logShardStates(clusterState);

        // A single shard is started on node1, even though it normally would not
        // be allowed, because it's a primary that hasn't been allocated, and node1
        // is still below the high watermark (unlike node3)
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
    }

    public void testAverageUsage() {
        RoutingNode rn = RoutingNodesHelper.routingNode("node1", newNode("node1"));

        Map<String, DiskUsage> usages = new HashMap<>();
        usages.put("node2", new DiskUsage("node2", "n2", "/dev/null", 100, 50)); // 50% used
        usages.put("node3", new DiskUsage("node3", "n3", "/dev/null", 100, 0));  // 100% used

        DiskUsage node1Usage = DiskThresholdDecider.averageUsage(rn, usages);
        assertThat(node1Usage.totalBytes(), equalTo(100L));
        assertThat(node1Usage.freeBytes(), equalTo(25L));
    }

    private void doTestShardRelocationsTakenIntoAccount(boolean testMaxHeadroom, boolean multipleProjects) {
        Settings.Builder diskSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), 0.7)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), 0.8);
        if (testMaxHeadroom) {
            diskSettings = diskSettings.put(
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING.getKey(),
                ByteSizeValue.ofGb(150).toString()
            )
                .put(
                    DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING.getKey(),
                    ByteSizeValue.ofGb(110).toString()
                );
        }

        Map<String, DiskUsage> usages = new HashMap<>();
        final long totalBytes = testMaxHeadroom ? ByteSizeValue.ofGb(10000).getBytes() : 100;
        usages.put(
            "node1",
            new DiskUsage("node1", "n1", "/dev/null", totalBytes, testMaxHeadroom ? ByteSizeValue.ofGb(160).getBytes() : 40)
        );
        usages.put(
            "node2",
            new DiskUsage("node2", "n2", "/dev/null", totalBytes, testMaxHeadroom ? ByteSizeValue.ofGb(160).getBytes() : 40)
        );
        usages.put(
            "node3",
            new DiskUsage("node3", "n3", "/dev/null", totalBytes, testMaxHeadroom ? ByteSizeValue.ofGb(160).getBytes() : 40)
        );

        Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", testMaxHeadroom ? ByteSizeValue.ofGb(14).getBytes() : 14L);
        shardSizes.put("[test][0][r]", testMaxHeadroom ? ByteSizeValue.ofGb(14).getBytes() : 14L);
        shardSizes.put("[test2][0][p]", testMaxHeadroom ? ByteSizeValue.ofGb(1).getBytes() : 1L);
        shardSizes.put("[test2][0][r]", testMaxHeadroom ? ByteSizeValue.ofGb(1).getBytes() : 1L);
        final ClusterInfo clusterInfo = new DevNullClusterInfo(usages, usages, shardSizes);

        final AtomicReference<ClusterInfo> clusterInfoReference = new AtomicReference<>(clusterInfo);

        AllocationService strategy = createAllocationService(
            clusterInfoReference::get,
            EmptySnapshotsInfoService.INSTANCE,
            createEnableAllocationDecider(Settings.builder().put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none").build()),
            createDiskThresholdDecider(diskSettings.build())
        );

        var indexMetadata1 = IndexMetadata.builder("test")
            .settings(settings(IndexVersion.current()).put(IndexMetadata.SETTING_INDEX_UUID, randomUUID()))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        var indexMetadata2 = IndexMetadata.builder("test2")
            .settings(settings(IndexVersion.current()).put(IndexMetadata.SETTING_INDEX_UUID, randomUUID()))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        var metadataBuilder = Metadata.builder();
        final ProjectId projectId1, projectId2;
        if (multipleProjects) {
            projectId1 = randomUniqueProjectId();
            projectId2 = randomUniqueProjectId();
            metadataBuilder.put(ProjectMetadata.builder(projectId1).put(indexMetadata1, false));
            metadataBuilder.put(ProjectMetadata.builder(projectId2).put(indexMetadata2, false));
        } else {
            projectId1 = projectId2 = Metadata.DEFAULT_PROJECT_ID;
            metadataBuilder.put(ProjectMetadata.builder(projectId2).put(indexMetadata1, false).put(indexMetadata2, false));
        }
        var metadata = metadataBuilder.build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(GlobalRoutingTableTestHelper.buildRoutingTable(metadata, RoutingTable.Builder::addAsNew))
            .build();

        logger.info("--> adding two nodes");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        logShardStates(clusterState);

        // primaries should be initializing
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(2));

        logger.info("--> start the shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        // replicas should now be initializing
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(2));

        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logShardStates(clusterState);
        // Assert that we're able to start the primary and replicas
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(4));

        logger.info("--> adding node3");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();

        {
            AllocationCommand moveAllocationCommand = new MoveAllocationCommand("test", 0, "node2", "node3", projectId1);
            AllocationCommands cmds = new AllocationCommands(moveAllocationCommand);

            clusterState = strategy.reroute(clusterState, cmds, false, false, false, ActionListener.noop()).clusterState();
            logShardStates(clusterState);
        }

        Map<String, DiskUsage> overfullUsages = new HashMap<>();
        overfullUsages.put(
            "node1",
            new DiskUsage("node1", "n1", "/dev/null", totalBytes, testMaxHeadroom ? ByteSizeValue.ofGb(160).getBytes() : 40)
        );
        overfullUsages.put(
            "node2",
            new DiskUsage("node2", "n2", "/dev/null", totalBytes, testMaxHeadroom ? ByteSizeValue.ofGb(160).getBytes() : 40)
        );
        overfullUsages.put("node3", new DiskUsage("node3", "n3", "/dev/null", totalBytes, 0));  // 100% used

        Map<String, Long> largerShardSizes = new HashMap<>();
        largerShardSizes.put("[test][0][p]", testMaxHeadroom ? ByteSizeValue.ofGb(14).getBytes() : 14L);
        largerShardSizes.put("[test][0][r]", testMaxHeadroom ? ByteSizeValue.ofGb(14).getBytes() : 14L);
        largerShardSizes.put("[test2][0][p]", testMaxHeadroom ? ByteSizeValue.ofGb(2).getBytes() : 2L);
        largerShardSizes.put("[test2][0][r]", testMaxHeadroom ? ByteSizeValue.ofGb(2).getBytes() : 2L);

        final ClusterInfo overfullClusterInfo = new DevNullClusterInfo(overfullUsages, overfullUsages, largerShardSizes);

        {
            AllocationCommand moveAllocationCommand = new MoveAllocationCommand("test2", 0, "node2", "node3", projectId2);
            AllocationCommands cmds = new AllocationCommands(moveAllocationCommand);

            final ClusterState clusterStateThatRejectsCommands = clusterState;

            assertThat(
                expectThrows(
                    IllegalArgumentException.class,
                    () -> strategy.reroute(clusterStateThatRejectsCommands, cmds, false, false, false, ActionListener.noop())
                ).getMessage(),
                containsString(
                    testMaxHeadroom
                        ? "the node is above the low watermark cluster setting "
                            + "[cluster.routing.allocation.disk.watermark.low.max_headroom=150gb], "
                            + "having less than the minimum required [150gb] free space, actual free: [146gb], actual used: [98.5%]"
                        : "the node is above the low watermark cluster setting [cluster.routing.allocation.disk.watermark.low=70%], "
                            + "having less than the minimum required [30b] free space, actual free: [26b], actual used: [74%]"
                )
            );

            clusterInfoReference.set(overfullClusterInfo);

            assertThat(
                expectThrows(
                    IllegalArgumentException.class,
                    () -> strategy.reroute(clusterStateThatRejectsCommands, cmds, false, false, false, ActionListener.noop())
                ).getMessage(),
                containsString("the node has fewer free bytes remaining than the total size of all incoming shards")
            );

            clusterInfoReference.set(clusterInfo);
        }

        {
            AllocationCommand moveAllocationCommand = new MoveAllocationCommand("test2", 0, "node2", "node3", projectId2);
            AllocationCommands cmds = new AllocationCommands(moveAllocationCommand);

            clusterState = startInitializingShardsAndReroute(strategy, clusterState);
            clusterState = strategy.reroute(clusterState, cmds, false, false, false, ActionListener.noop()).clusterState();
            logShardStates(clusterState);

            clusterInfoReference.set(overfullClusterInfo);

            strategy.reroute(clusterState, "foo", ActionListener.noop()); // ensure reroute doesn't fail even though there is negative free
                                                                          // space
        }

        {
            clusterInfoReference.set(overfullClusterInfo);
            clusterState = applyStartedShardsUntilNoChange(clusterState, strategy);
            final List<ShardRouting> startedShardsWithOverfullDisk = shardsWithState(clusterState.getRoutingNodes(), STARTED);
            assertThat(startedShardsWithOverfullDisk.size(), equalTo(4));
            for (ShardRouting shardRouting : startedShardsWithOverfullDisk) {
                // no shards on node3 since it has no free space
                assertThat(shardRouting.toString(), shardRouting.currentNodeId(), oneOf("node1", "node2"));
            }

            // reset free space on node 3 and reserve space on node1
            clusterInfoReference.set(
                new DevNullClusterInfo(
                    usages,
                    usages,
                    shardSizes,
                    Map.of(
                        new ClusterInfo.NodeAndPath("node1", "/dev/null"),
                        new ClusterInfo.ReservedSpace.Builder().add(
                            new ShardId("", "", 0),
                            testMaxHeadroom ? ByteSizeValue.ofGb(between(200, 250)).getBytes() : between(51, 200)
                        ).build()
                    )
                )
            );
            clusterState = applyStartedShardsUntilNoChange(clusterState, strategy);
            final List<ShardRouting> startedShardsWithReservedSpace = shardsWithState(clusterState.getRoutingNodes(), STARTED);
            assertThat(startedShardsWithReservedSpace.size(), equalTo(4));
            for (ShardRouting shardRouting : startedShardsWithReservedSpace) {
                // no shards on node1 since all its free space is reserved
                assertThat(shardRouting.toString(), shardRouting.currentNodeId(), oneOf("node2", "node3"));
            }
        }
    }

    public void testShardRelocationsTakenIntoAccountWithPercentages() {
        doTestShardRelocationsTakenIntoAccount(false, false);
    }

    public void testShardRelocationsTakenIntoAccountWithMaxHeadroom() {
        doTestShardRelocationsTakenIntoAccount(true, false);
    }

    public void testShardRelocationsTakenIntoAccountWithPercentagesOnMultipleProjects() {
        doTestShardRelocationsTakenIntoAccount(false, true);
    }

    public void testShardRelocationsTakenIntoAccountWithMaxHeadroomOnMultipleProjects() {
        doTestShardRelocationsTakenIntoAccount(true, true);
    }

    private void doTestCanRemainWithShardRelocatingAway(boolean testMaxHeadroom) {
        Settings.Builder diskSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "60%")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "70%");
        if (testMaxHeadroom) {
            diskSettings = diskSettings.put(
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING.getKey(),
                ByteSizeValue.ofGb(150).toString()
            )
                .put(
                    DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING.getKey(),
                    ByteSizeValue.ofGb(110).toString()
                );
        }

        Map<String, DiskUsage> usages = new HashMap<>();
        final long totalBytes = testMaxHeadroom ? ByteSizeValue.ofGb(10000).getBytes() : 100;
        usages.put(
            "node1",
            new DiskUsage("node1", "n1", "/dev/null", totalBytes, testMaxHeadroom ? ByteSizeValue.ofGb(40).getBytes() : 20)
        );
        usages.put(
            "node2",
            new DiskUsage("node2", "n2", "/dev/null", totalBytes, testMaxHeadroom ? ByteSizeValue.ofGb(10000).getBytes() : 100)
        );

        Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", testMaxHeadroom ? ByteSizeValue.ofGb(4980).getBytes() : 40L);
        shardSizes.put("[test][1][p]", testMaxHeadroom ? ByteSizeValue.ofGb(4980).getBytes() : 40L);
        shardSizes.put("[foo][0][p]", testMaxHeadroom ? ByteSizeValue.ofGb(10).getBytes() : 10L);

        final ClusterInfo clusterInfo = new DevNullClusterInfo(usages, usages, shardSizes);

        DiskThresholdDecider diskThresholdDecider = createDiskThresholdDecider(diskSettings.build());

        DiscoveryNode discoveryNode1 = newNode("node1");
        DiscoveryNode discoveryNode2 = newNode("node2");

        var testMetadata = IndexMetadata.builder("test")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(2)
            .numberOfReplicas(0)
            .build();
        var fooMetadata = IndexMetadata.builder("foo")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        ClusterState baseClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(testMetadata, false).put(fooMetadata, false).build())
            .routingTable(
                RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(testMetadata).addAsNew(fooMetadata).build()
            )
            .nodes(DiscoveryNodes.builder().add(discoveryNode1).add(discoveryNode2).build())
            .build();

        // Two shards consuming each 80% of disk space while 70% is allowed, so shard 0 isn't allowed here
        ShardRouting firstRouting = TestShardRouting.newShardRouting("test", 0, "node1", null, true, ShardRoutingState.STARTED);
        ShardRouting secondRouting = TestShardRouting.newShardRouting("test", 1, "node1", null, true, ShardRoutingState.STARTED);
        RoutingNode firstRoutingNode = RoutingNodesHelper.routingNode("node1", discoveryNode1, firstRouting, secondRouting);
        RoutingTable.Builder builder = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(firstRouting.index())
                    .addIndexShard(IndexShardRoutingTable.builder(firstRouting.shardId()).addShard(firstRouting))
                    .addIndexShard(IndexShardRoutingTable.builder(secondRouting.shardId()).addShard(secondRouting))
            );
        ClusterState clusterState = ClusterState.builder(baseClusterState).routingTable(builder.build()).build();
        RoutingAllocation routingAllocation = new RoutingAllocation(
            null,
            RoutingNodes.immutable(clusterState.globalRoutingTable(), clusterState.nodes()),
            clusterState,
            clusterInfo,
            null,
            System.nanoTime()
        );
        routingAllocation.debugDecision(true);
        Decision decision = diskThresholdDecider.canRemain(
            routingAllocation.metadata().getProject().getIndexSafe(firstRouting.index()),
            firstRouting,
            firstRoutingNode,
            routingAllocation
        );
        assertThat(decision.type(), equalTo(Decision.Type.NO));
        assertThat(
            decision.getExplanation(),
            containsString(
                testMaxHeadroom
                    ? "the shard cannot remain on this node because it is above the high watermark cluster setting "
                        + "[cluster.routing.allocation.disk.watermark.high.max_headroom=110gb] and there is less than the required [110gb] "
                        + "free space on node, actual free: [40gb], actual used: [99.6%]"
                    : "the shard cannot remain on this node because it is above the high watermark cluster setting "
                        + "[cluster.routing.allocation.disk.watermark.high=70%] and there is less than the required [30b] free space "
                        + "on node, actual free: [20b], actual used: [80%]"
            )
        );

        // Two shards consuming each 80% of disk space while 70% is allowed, but one is relocating, so shard 0 can stay
        firstRouting = TestShardRouting.newShardRouting("test", 0, "node1", null, true, ShardRoutingState.STARTED);
        secondRouting = TestShardRouting.newShardRouting("test", 1, "node1", "node2", true, ShardRoutingState.RELOCATING);
        ShardRouting fooRouting = TestShardRouting.newShardRouting("foo", 0, null, true, ShardRoutingState.UNASSIGNED);
        fooRouting = fooRouting.updateUnassigned(
            new UnassignedInfo(
                fooRouting.unassignedInfo().reason(),
                fooRouting.unassignedInfo().message(),
                fooRouting.unassignedInfo().failure(),
                fooRouting.unassignedInfo().failedAllocations(),
                fooRouting.unassignedInfo().unassignedTimeNanos(),
                fooRouting.unassignedInfo().unassignedTimeMillis(),
                false,
                fooRouting.recoverySource().getType() == RecoverySource.Type.EMPTY_STORE
                    ? AllocationStatus.DECIDERS_NO
                    : AllocationStatus.NO_VALID_SHARD_COPY,
                fooRouting.unassignedInfo().failedNodeIds(),
                fooRouting.unassignedInfo().lastAllocatedNodeId()
            ),
            fooRouting.recoverySource()
        );
        firstRoutingNode = RoutingNodesHelper.routingNode("node1", discoveryNode1, firstRouting, secondRouting);
        builder = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(firstRouting.index())
                    .addIndexShard(new IndexShardRoutingTable.Builder(firstRouting.shardId()).addShard(firstRouting))
                    .addIndexShard(new IndexShardRoutingTable.Builder(secondRouting.shardId()).addShard(secondRouting))
            )
            .add(
                IndexRoutingTable.builder(fooRouting.index())
                    .addIndexShard(new IndexShardRoutingTable.Builder(fooRouting.shardId()).addShard(fooRouting))
                    .build()
            );
        clusterState = ClusterState.builder(baseClusterState).routingTable(builder.build()).build();
        routingAllocation = new RoutingAllocation(
            null,
            RoutingNodes.immutable(clusterState.globalRoutingTable(), clusterState.nodes()),
            clusterState,
            clusterInfo,
            null,
            System.nanoTime()
        );
        routingAllocation.debugDecision(true);
        decision = diskThresholdDecider.canRemain(
            routingAllocation.metadata().getProject().getIndexSafe(firstRouting.index()),
            firstRouting,
            firstRoutingNode,
            routingAllocation
        );
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertEquals(
            testMaxHeadroom
                ? "there is enough disk on this node for the shard to remain, free: [4.9tb]"
                : "there is enough disk on this node for the shard to remain, free: [60b]",
            decision.getExplanation()
        );

        clusterState = ClusterState.builder(clusterState)
            .routingTable(GlobalRoutingTableTestHelper.updateRoutingTable(clusterState, (builder1, indexMetadata) -> {
                builder1.addAsNew(indexMetadata);
            }, (ignore1, ignore2) -> {}))
            .build();
        routingAllocation = new RoutingAllocation(
            null,
            RoutingNodes.immutable(clusterState.globalRoutingTable(), clusterState.nodes()),
            clusterState,
            clusterInfo,
            null,
            System.nanoTime()
        );
        routingAllocation.debugDecision(true);

        decision = diskThresholdDecider.canAllocate(fooRouting, firstRoutingNode, routingAllocation);
        assertThat(decision.type(), equalTo(Decision.Type.NO));
        if (fooRouting.recoverySource().getType() == RecoverySource.Type.EMPTY_STORE) {
            assertThat(
                decision.getExplanation(),
                containsString(
                    testMaxHeadroom
                        ? "the node is above the high watermark cluster setting [cluster.routing.allocation.disk.watermark"
                            + ".high.max_headroom=110gb], having less than the minimum required [110gb] free space, actual free: "
                            + "[40gb], actual used: [99.6%]"
                        : "the node is above the high watermark cluster setting [cluster.routing.allocation.disk.watermark.high=70%], "
                            + "having less than the minimum required [30b] free space, actual free: [20b], actual used: [80%]"
                )
            );
        } else {
            assertThat(
                decision.getExplanation(),
                containsString(
                    testMaxHeadroom
                        ? "the node is above the low watermark cluster setting [cluster.routing.allocation.disk.watermark.low"
                            + ".max_headroom=150gb], having less than the minimum required [150gb] free space, actual free: [40gb], actual "
                            + "used: [99.6%]"
                        : "the node is above the low watermark cluster setting [cluster.routing.allocation.disk.watermark.low=60%], "
                            + "having less than the minimum required [40b] free space, actual free: [20b], actual used: [80%]"
                )
            );
        }

        // Creating AllocationService instance and the services it depends on...
        AllocationService strategy = createAllocationService(
            clusterInfo,
            diskThresholdDecider,
            // fake allocation decider to block allocation of the `foo` shard
            new AllocationDecider() {
                @Override
                public Decision canAllocate(IndexMetadata indexMetadata, RoutingNode node, RoutingAllocation allocation) {
                    return cannotAllocateFooShards(indexMetadata.getIndex());
                }

                @Override
                public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                    return cannotAllocateFooShards(shardRouting.index());
                }

                @Override
                public Decision canForceAllocatePrimary(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                    return cannotAllocateFooShards(shardRouting.index());
                }

                private Decision cannotAllocateFooShards(Index index) {
                    return index.getName().equals("foo") ? Decision.NO : Decision.YES;
                }
            }
        );

        // Populate the in-sync allocation IDs so that the overall cluster state is valid enough to run reroute()
        final var metadataBuilder = Metadata.builder(clusterState.metadata());
        metadataBuilder.put(
            clusterState.metadata().getProject().index(fooRouting.index()).withInSyncAllocationIds(fooRouting.id(), Set.of(randomUUID())),
            false
        );
        metadataBuilder.put(
            clusterState.metadata()
                .getProject()
                .index(firstRouting.index())
                .withInSyncAllocationIds(firstRouting.id(), Set.of(firstRouting.allocationId().getId()))
                .withInSyncAllocationIds(secondRouting.id(), Set.of(secondRouting.allocationId().getId())),
            false
        );
        final var clusterStateWithInSyncIds = ClusterState.builder(clusterState).metadata(metadataBuilder).build();

        // Ensure that the reroute call doesn't alter the routing table, since the first primary is relocating away
        // and therefore we will have sufficient disk space on node1.
        ClusterState result = strategy.reroute(clusterStateWithInSyncIds, "reroute", ActionListener.noop());
        assertThat(result, sameInstance(clusterStateWithInSyncIds));
        assertThat(result.routingTable().index("test").shard(0).primaryShard().state(), equalTo(STARTED));
        assertThat(result.routingTable().index("test").shard(0).primaryShard().currentNodeId(), equalTo("node1"));
        assertThat(result.routingTable().index("test").shard(0).primaryShard().relocatingNodeId(), nullValue());
        assertThat(result.routingTable().index("test").shard(1).primaryShard().state(), equalTo(RELOCATING));
        assertThat(result.routingTable().index("test").shard(1).primaryShard().currentNodeId(), equalTo("node1"));
        assertThat(result.routingTable().index("test").shard(1).primaryShard().relocatingNodeId(), equalTo("node2"));
    }

    public void testCanRemainWithShardRelocatingAwayWithPercentages() {
        doTestCanRemainWithShardRelocatingAway(false);
    }

    public void testCanRemainWithShardRelocatingAwayWithMaxHeadroom() {
        doTestCanRemainWithShardRelocatingAway(true);
    }

    private void doTestWatermarksEnabledForSingleDataNode(boolean testMaxHeadroom) {
        Settings.Builder builder = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "60%")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "70%");
        if (testMaxHeadroom) {
            builder = builder.put(
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING.getKey(),
                ByteSizeValue.ofGb(150).toString()
            )
                .put(
                    DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING.getKey(),
                    ByteSizeValue.ofGb(110).toString()
                );
        }
        Settings diskSettings = builder.build();

        final long totalBytes = testMaxHeadroom ? ByteSizeValue.ofGb(10000).getBytes() : 100;
        Map<String, DiskUsage> usages = Map.of(
            "data",
            new DiskUsage("data", "data", "/dev/null", totalBytes, testMaxHeadroom ? ByteSizeValue.ofGb(40).getBytes() : 20)
        );

        // We have an index with 1 primary shard, taking more bytes than the free space of the single data node.
        Map<String, Long> shardSizes = Map.of("[test][0][p]", testMaxHeadroom ? ByteSizeValue.ofGb(60).getBytes() : 40L);
        final ClusterInfo clusterInfo = new DevNullClusterInfo(usages, usages, shardSizes);

        DiskThresholdDecider diskThresholdDecider = createDiskThresholdDecider(diskSettings);

        var discoveryNodesBuilder = DiscoveryNodes.builder().add(newNode("data", "data", Set.of(DiscoveryNodeRole.DATA_ROLE)));
        if (randomBoolean()) {
            discoveryNodesBuilder.add(newNode("master", "master", Set.of(DiscoveryNodeRole.MASTER_ROLE)));
        }

        var testMetadata = IndexMetadata.builder("test")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .nodes(discoveryNodesBuilder.build())
            .metadata(Metadata.builder().put(testMetadata, false).build())
            .routingTable(RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(testMetadata).build())
            .build();

        // validate that the shard cannot be allocated
        AllocationService strategy = createAllocationService(clusterInfo, diskThresholdDecider);
        ClusterState result = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        ShardRouting shardRouting = result.routingTable().index("test").shard(0).primaryShard();
        assertThat(shardRouting.state(), equalTo(UNASSIGNED));
        assertThat(shardRouting.currentNodeId(), nullValue());
        assertThat(shardRouting.relocatingNodeId(), nullValue());

        // force assign shard and validate that it cannot remain.
        ShardId shardId = shardRouting.shardId();
        ShardRouting startedShard = shardRouting.initialize("data", null, 40L).moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        RoutingTable forceAssignedRoutingTable = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(shardId.getIndex())
                    .addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(startedShard))
            )
            .build();
        clusterState = ClusterState.builder(clusterState).routingTable(forceAssignedRoutingTable).build();

        RoutingAllocation routingAllocation = new RoutingAllocation(
            null,
            RoutingNodes.immutable(clusterState.globalRoutingTable(), clusterState.nodes()),
            clusterState,
            clusterInfo,
            null,
            System.nanoTime()
        );
        routingAllocation.debugDecision(true);
        Decision decision = diskThresholdDecider.canRemain(
            routingAllocation.metadata().getProject().getIndexSafe(startedShard.index()),
            startedShard,
            clusterState.getRoutingNodes().node("data"),
            routingAllocation
        );
        assertThat(decision.type(), equalTo(Decision.Type.NO));
        assertThat(
            decision.getExplanation(),
            containsString(
                testMaxHeadroom
                    ? "the shard cannot remain on this node because it is above the high watermark cluster setting [cluster"
                        + ".routing.allocation.disk.watermark.high.max_headroom=110gb] and there is less than the required [110gb] free "
                        + "space on node, actual free: [40gb], actual used: [99.6%]"
                    : "the shard cannot remain on this node because it is above the high watermark cluster setting"
                        + " [cluster.routing.allocation.disk.watermark.high=70%] and there is less than the required [30b] free space "
                        + "on node, actual free: [20b], actual used: [80%]"
            )
        );
    }

    public void testWatermarksEnabledForSingleDataNodeWithPercentages() {
        doTestWatermarksEnabledForSingleDataNode(false);
    }

    public void testWatermarksEnabledForSingleDataNodeWithMaxHeadroom() {
        doTestWatermarksEnabledForSingleDataNode(true);
    }

    private void doTestDiskThresholdWithSnapshotShardSizes(boolean testMaxHeadroom) {
        final long shardSizeInBytes = randomBoolean()
            ? (testMaxHeadroom ? ByteSizeValue.ofGb(99).getBytes() : 10L) // fits free space of node1
            : (testMaxHeadroom ? ByteSizeValue.ofGb(350).getBytes() : 50L); // does not fit free space of node1
        logger.info("--> using shard size [{}]", shardSizeInBytes);

        Settings.Builder diskSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "90%")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "95%");
        if (testMaxHeadroom) {
            diskSettings = diskSettings.put(
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING.getKey(),
                ByteSizeValue.ofGb(150).toString()
            )
                .put(
                    DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING.getKey(),
                    ByteSizeValue.ofGb(110).toString()
                );
        }

        Map<String, DiskUsage> usages = new HashMap<>();
        final long totalBytes = testMaxHeadroom ? ByteSizeValue.ofGb(10000).getBytes() : 100;
        usages.put(
            "node1",
            new DiskUsage("node1", "n1", "/dev/null", totalBytes, testMaxHeadroom ? ByteSizeValue.ofGb(210).getBytes() : 21)
        );
        usages.put("node2", new DiskUsage("node2", "n2", "/dev/null", totalBytes, testMaxHeadroom ? ByteSizeValue.ofGb(1).getBytes() : 1));

        final Snapshot snapshot = new Snapshot("_repository", new SnapshotId("_snapshot_name", UUIDs.randomBase64UUID(random())));
        final IndexId indexId = new IndexId("_indexid_name", UUIDs.randomBase64UUID(random()));
        final ShardId shardId = new ShardId(new Index("test", IndexMetadata.INDEX_UUID_NA_VALUE), 0);

        var indexMetadata = IndexMetadata.builder("test")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putInSyncAllocationIds(0, Set.of(AllocationId.newInitializing().getId()))
            .build();
        ClusterState clusterState = ClusterState.builder(new ClusterName(getTestName()))
            .metadata(Metadata.builder().put(indexMetadata, false).build())
            .routingTable(
                RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
                    .addAsNewRestore(
                        indexMetadata,
                        new RecoverySource.SnapshotRecoverySource("_restore_uuid", snapshot, IndexVersion.current(), indexId),
                        new HashSet<>()
                    )
                    .build()
            )
            .putCustom(
                RestoreInProgress.TYPE,
                new RestoreInProgress.Builder().add(
                    new RestoreInProgress.Entry(
                        "_restore_uuid",
                        snapshot,
                        RestoreInProgress.State.INIT,
                        false,
                        List.of("test"),
                        Map.of(shardId, new RestoreInProgress.ShardRestoreStatus("node1"))
                    )
                ).build()
            )
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")) // node2 is added because DiskThresholdDecider
            // automatically ignore single-node clusters
            )
            .build();

        assertThat(
            shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).stream()
                .map(ShardRouting::unassignedInfo)
                .allMatch(unassignedInfo -> Reason.NEW_INDEX_RESTORED.equals(unassignedInfo.reason())),
            is(true)
        );
        assertThat(
            shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).stream()
                .map(ShardRouting::unassignedInfo)
                .allMatch(unassignedInfo -> AllocationStatus.NO_ATTEMPT.equals(unassignedInfo.lastAllocationStatus())),
            is(true)
        );
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).size(), equalTo(1));

        final AtomicReference<SnapshotShardSizeInfo> snapshotShardSizeInfoRef = new AtomicReference<>(SnapshotShardSizeInfo.EMPTY);

        final AllocationService strategy = createAllocationService(
            () -> new DevNullClusterInfo(usages, usages, Map.of()),
            snapshotShardSizeInfoRef::get,
            new RestoreInProgressAllocationDecider(),
            createDiskThresholdDecider(diskSettings.build())
        );

        // reroute triggers snapshot shard size fetching
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        logShardStates(clusterState);

        // shard cannot be assigned yet as the snapshot shard size is unknown
        assertThat(
            shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).stream()
                .map(ShardRouting::unassignedInfo)
                .allMatch(unassignedInfo -> AllocationStatus.FETCHING_SHARD_DATA.equals(unassignedInfo.lastAllocationStatus())),
            is(true)
        );
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).size(), equalTo(1));

        final SnapshotShard snapshotShard = new SnapshotShard(snapshot, indexId, shardId);
        final Map<SnapshotShard, Long> snapshotShardSizes = new HashMap<>();

        final boolean shouldAllocate;
        if (randomBoolean()) {
            logger.info("--> simulating snapshot shards size retrieval success");
            snapshotShardSizes.put(snapshotShard, shardSizeInBytes);
            logger.info("--> shard allocation depends on its size");
            DiskUsage usage = usages.get("node1");
            shouldAllocate = shardSizeInBytes < usage.freeBytes();
        } else {
            logger.info("--> simulating snapshot shards size retrieval failure");
            snapshotShardSizes.put(snapshotShard, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
            logger.info("--> shard is always allocated when its size could not be retrieved");
            shouldAllocate = true;
        }
        snapshotShardSizeInfoRef.set(new SnapshotShardSizeInfo(snapshotShardSizes));

        // reroute uses the previous snapshot shard size
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        logShardStates(clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).size(), equalTo(shouldAllocate ? 0 : 1));
        assertThat(
            shardsWithState(clusterState.getRoutingNodes(), "test", INITIALIZING).size() //
                + shardsWithState(clusterState.getRoutingNodes(), "test", STARTED).size(),
            equalTo(shouldAllocate ? 1 : 0)
        );
    }

    public void testDiskThresholdWithSnapshotShardSizesWithPercentages() {
        doTestDiskThresholdWithSnapshotShardSizes(false);
    }

    public void testDiskThresholdWithSnapshotShardSizesWithMaxHeadroom() {
        doTestDiskThresholdWithSnapshotShardSizes(true);
    }

    public void logShardStates(ClusterState state) {
        RoutingNodes rn = state.getRoutingNodes();
        logger.info(
            "--> counts: total: {}, unassigned: {}, initializing: {}, relocating: {}, started: {}",
            assignedShardsIn(rn).count(),
            rn.unassigned().size(),
            numberOfShardsWithState(rn, INITIALIZING),
            numberOfShardsWithState(rn, RELOCATING),
            numberOfShardsWithState(rn, STARTED)
        );
        logger.info(
            "--> unassigned: {}, initializing: {}, relocating: {}, started: {}",
            shardsWithState(rn, UNASSIGNED),
            shardsWithState(rn, INITIALIZING),
            shardsWithState(rn, RELOCATING),
            shardsWithState(rn, STARTED)
        );
    }

    private AllocationService createAllocationService(ClusterInfo clusterInfo, AllocationDecider... allocationDeciders) {
        return createAllocationService(() -> clusterInfo, EmptySnapshotsInfoService.INSTANCE, allocationDeciders);
    }

    private AllocationService createAllocationService(
        ClusterInfoService clusterInfoService,
        SnapshotsInfoService snapshotShardSizeInfoService,
        AllocationDecider... allocationDeciders
    ) {
        return new AllocationService(
            new AllocationDeciders(
                Stream.concat(
                    Stream.of(createSameShardAllocationDecider(Settings.EMPTY), new ReplicaAfterPrimaryActiveAllocationDecider()),
                    Stream.of(allocationDeciders)
                ).toList()
            ),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            clusterInfoService,
            snapshotShardSizeInfoService,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );
    }

    private DiskThresholdDecider createDiskThresholdDecider(Settings settings) {
        return new DiskThresholdDecider(settings, createBuiltInClusterSettings(settings));
    }

    private SameShardAllocationDecider createSameShardAllocationDecider(Settings settings) {
        return new SameShardAllocationDecider(createBuiltInClusterSettings(settings));
    }

    private EnableAllocationDecider createEnableAllocationDecider(Settings settings) {
        return new EnableAllocationDecider(createBuiltInClusterSettings(settings));
    }

    /**
     * ClusterInfo that always reports /dev/null for the shards' data paths.
     */
    static class DevNullClusterInfo extends ClusterInfo {
        DevNullClusterInfo(
            Map<String, DiskUsage> leastAvailableSpaceUsage,
            Map<String, DiskUsage> mostAvailableSpaceUsage,
            Map<String, Long> shardSizes
        ) {
            this(leastAvailableSpaceUsage, mostAvailableSpaceUsage, shardSizes, Map.of());
        }

        DevNullClusterInfo(
            Map<String, DiskUsage> leastAvailableSpaceUsage,
            Map<String, DiskUsage> mostAvailableSpaceUsage,
            Map<String, Long> shardSizes,
            Map<NodeAndPath, ReservedSpace> reservedSpace
        ) {
            super(leastAvailableSpaceUsage, mostAvailableSpaceUsage, shardSizes, Map.of(), Map.of(), reservedSpace, Map.of());
        }

        @Override
        public String getDataPath(ShardRouting shardRouting) {
            return "/dev/null";
        }
    }
}
