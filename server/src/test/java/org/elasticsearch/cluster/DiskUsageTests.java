/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.RecoverySource.PeerRecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;

public class DiskUsageTests extends ESTestCase {
    public void testDiskUsageCalc() {
        DiskUsage du = new DiskUsage("node1", "n1", "random", 100, 40);
        assertThat(du.getFreeDiskAsPercentage(), equalTo(40.0));
        assertThat(du.getUsedDiskAsPercentage(), equalTo(100.0 - 40.0));
        assertThat(du.getFreeBytes(), equalTo(40L));
        assertThat(du.getUsedBytes(), equalTo(60L));
        assertThat(du.getTotalBytes(), equalTo(100L));

        DiskUsage du2 = new DiskUsage("node1", "n1", "random", 100, 55);
        assertThat(du2.getFreeDiskAsPercentage(), equalTo(55.0));
        assertThat(du2.getUsedDiskAsPercentage(), equalTo(45.0));
        assertThat(du2.getFreeBytes(), equalTo(55L));
        assertThat(du2.getUsedBytes(), equalTo(45L));
        assertThat(du2.getTotalBytes(), equalTo(100L));

        // Test that DiskUsage handles invalid numbers, as reported by some
        // filesystems (ZFS & NTFS)
        DiskUsage du3 = new DiskUsage("node1", "n1", "random", 100, 101);
        assertThat(du3.getFreeDiskAsPercentage(), equalTo(101.0));
        assertThat(du3.getFreeBytes(), equalTo(101L));
        assertThat(du3.getUsedBytes(), equalTo(-1L));
        assertThat(du3.getTotalBytes(), equalTo(100L));

        DiskUsage du4 = new DiskUsage("node1", "n1", "random", -1, -1);
        assertThat(du4.getFreeDiskAsPercentage(), equalTo(100.0));
        assertThat(du4.getFreeBytes(), equalTo(-1L));
        assertThat(du4.getUsedBytes(), equalTo(0L));
        assertThat(du4.getTotalBytes(), equalTo(-1L));

        DiskUsage du5 = new DiskUsage("node1", "n1", "random", 0, 0);
        assertThat(du5.getFreeDiskAsPercentage(), equalTo(100.0));
        assertThat(du5.getFreeBytes(), equalTo(0L));
        assertThat(du5.getUsedBytes(), equalTo(0L));
        assertThat(du5.getTotalBytes(), equalTo(0L));
    }

    public void testRandomDiskUsage() {
        int iters = scaledRandomIntBetween(1000, 10000);
        for (int i = 1; i < iters; i++) {
            long total = between(Integer.MIN_VALUE, Integer.MAX_VALUE);
            long free = between(Integer.MIN_VALUE, Integer.MAX_VALUE);
            DiskUsage du = new DiskUsage("random", "random", "random", total, free);
            if (total == 0) {
                assertThat(du.getFreeBytes(), equalTo(free));
                assertThat(du.getTotalBytes(), equalTo(0L));
                assertThat(du.getUsedBytes(), equalTo(-free));
                assertThat(du.getFreeDiskAsPercentage(), equalTo(100.0));
                assertThat(du.getUsedDiskAsPercentage(), equalTo(0.0));
            } else {
                assertThat(du.getFreeBytes(), equalTo(free));
                assertThat(du.getTotalBytes(), equalTo(total));
                assertThat(du.getUsedBytes(), equalTo(total - free));
                assertThat(du.getFreeDiskAsPercentage(), equalTo(100.0 * free / total));
                assertThat(du.getUsedDiskAsPercentage(), equalTo(100.0 - (100.0 * free / total)));
            }
        }
    }

    public void testFillShardLevelInfo() {
        final Index index = new Index("test", "0xdeadbeef");
        ShardRouting test_0 = ShardRouting.newUnassigned(
            new ShardId(index, 0),
            false,
            PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"),
            ShardRouting.Role.DEFAULT
        );
        test_0 = ShardRoutingHelper.initialize(test_0, "node1");
        test_0 = ShardRoutingHelper.moveToStarted(test_0);
        Path test0Path = createTempDir().resolve("indices").resolve(index.getUUID()).resolve("0");
        CommonStats commonStats0 = new CommonStats();
        commonStats0.store = new StoreStats(100, 101, 0L);
        ShardRouting test_1 = ShardRouting.newUnassigned(
            new ShardId(index, 1),
            false,
            PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"),
            ShardRouting.Role.DEFAULT
        );
        test_1 = ShardRoutingHelper.initialize(test_1, "node2");
        test_1 = ShardRoutingHelper.moveToStarted(test_1);
        Path test1Path = createTempDir().resolve("indices").resolve(index.getUUID()).resolve("1");
        CommonStats commonStats1 = new CommonStats();
        commonStats1.store = new StoreStats(1000, 1001, 0L);
        CommonStats commonStats2 = new CommonStats();
        commonStats2.store = new StoreStats(1000, 999, 0L);
        ShardStats[] stats = new ShardStats[] {
            new ShardStats(test_0, new ShardPath(false, test0Path, test0Path, test_0.shardId()), commonStats0, null, null, null, false, 0),
            new ShardStats(test_1, new ShardPath(false, test1Path, test1Path, test_1.shardId()), commonStats1, null, null, null, false, 0),
            new ShardStats(
                test_1,
                new ShardPath(false, test1Path, test1Path, test_1.shardId()),
                commonStats2,
                null,
                null,
                null,
                false,
                0
            ) };
        Map<String, Long> shardSizes = new HashMap<>();
        Map<ShardId, Long> shardDataSetSizes = new HashMap<>();
        Map<ClusterInfo.NodeAndShard, String> routingToPath = new HashMap<>();
        InternalClusterInfoService.buildShardLevelInfo(
            RoutingTable.EMPTY_ROUTING_TABLE,
            stats,
            shardSizes,
            shardDataSetSizes,
            routingToPath,
            new HashMap<>()
        );

        assertThat(
            shardSizes,
            allOf(
                aMapWithSize(2),
                hasEntry(ClusterInfo.shardIdentifierFromRouting(test_0), 100L),
                hasEntry(ClusterInfo.shardIdentifierFromRouting(test_1), 1000L)
            )
        );

        assertThat(shardDataSetSizes, allOf(aMapWithSize(2), hasEntry(test_0.shardId(), 101L), hasEntry(test_1.shardId(), 1001L)));

        assertThat(
            routingToPath,
            allOf(
                aMapWithSize(2),
                hasEntry(ClusterInfo.NodeAndShard.from(test_0), test0Path.getParent().getParent().getParent().toAbsolutePath().toString()),
                hasEntry(ClusterInfo.NodeAndShard.from(test_1), test1Path.getParent().getParent().getParent().toAbsolutePath().toString())
            )
        );
    }

    public void testLeastAndMostAvailableDiskSpace() {
        {
            FsInfo.Path[] nodeFSInfo = new FsInfo.Path[] {
                new FsInfo.Path("/middle", "/dev/sda", 100, 90, 80),
                new FsInfo.Path("/least", "/dev/sdb", 200, 190, 70),
                new FsInfo.Path("/most", "/dev/sdc", 300, 290, 280), };
            NodeStats nodeStats = new NodeStats(
                DiscoveryNodeUtils.builder("node_1").roles(emptySet()).build(),
                0,
                null,
                null,
                null,
                null,
                null,
                new FsInfo(0, null, nodeFSInfo),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            );
            DiskUsage leastNode = DiskUsage.findLeastAvailablePath(nodeStats);
            DiskUsage mostNode = DiskUsage.findMostAvailable(nodeStats);
            assertDiskUsage(mostNode, nodeFSInfo[2]);
            assertDiskUsage(leastNode, nodeFSInfo[1]);
        }

        {
            FsInfo.Path[] nodeFSInfo = new FsInfo.Path[] { new FsInfo.Path("/least_most", "/dev/sda", 100, 90, 80), };
            NodeStats nodeStats = new NodeStats(
                DiscoveryNodeUtils.builder("node_2").roles(emptySet()).build(),
                0,
                null,
                null,
                null,
                null,
                null,
                new FsInfo(0, null, nodeFSInfo),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            );
            DiskUsage leastNode = DiskUsage.findLeastAvailablePath(nodeStats);
            DiskUsage mostNode = DiskUsage.findMostAvailable(nodeStats);
            assertDiskUsage(leastNode, nodeFSInfo[0]);
            assertDiskUsage(mostNode, nodeFSInfo[0]);
        }

        {
            FsInfo.Path[] nodeFSInfo = new FsInfo.Path[] {
                new FsInfo.Path("/least", "/dev/sda", 100, 90, 70),
                new FsInfo.Path("/most", "/dev/sda", 100, 90, 80), };
            NodeStats nodeStats = new NodeStats(
                DiscoveryNodeUtils.builder("node_3").roles(emptySet()).build(),
                0,
                null,
                null,
                null,
                null,
                null,
                new FsInfo(0, null, nodeFSInfo),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            );
            DiskUsage leastNode = DiskUsage.findLeastAvailablePath(nodeStats);
            DiskUsage mostNode = DiskUsage.findMostAvailable(nodeStats);
            assertDiskUsage(leastNode, nodeFSInfo[0]);
            assertDiskUsage(mostNode, nodeFSInfo[1]);
        }
    }

    public void testLeastAndMostAvailableDiskSpaceSomeInvalidValues() {
        {
            FsInfo.Path[] nodeFSInfo = new FsInfo.Path[] {
                new FsInfo.Path("/middle", "/dev/sda", 100, 90, 80),
                new FsInfo.Path("/least", "/dev/sdb", -1, -1, -1),
                new FsInfo.Path("/most", "/dev/sdc", 300, 290, 280), };

            NodeStats nodeStats = new NodeStats(
                DiscoveryNodeUtils.builder("node_1").roles(emptySet()).build(),
                0,
                null,
                null,
                null,
                null,
                null,
                new FsInfo(0, null, nodeFSInfo),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            );
            DiskUsage leastNode = DiskUsage.findLeastAvailablePath(nodeStats);
            DiskUsage mostNode = DiskUsage.findMostAvailable(nodeStats);
            assertNull("node_1 should have been skipped", leastNode);
            assertDiskUsage(mostNode, nodeFSInfo[2]);

        }

        {
            FsInfo.Path[] nodeFSInfo = new FsInfo.Path[] { new FsInfo.Path("/least_most", "/dev/sda", -1, -1, -1), };
            NodeStats nodeStats = new NodeStats(
                DiscoveryNodeUtils.builder("node_2").roles(emptySet()).build(),
                0,
                null,
                null,
                null,
                null,
                null,
                new FsInfo(0, null, nodeFSInfo),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            );
            DiskUsage leastNode = DiskUsage.findLeastAvailablePath(nodeStats);
            DiskUsage mostNode = DiskUsage.findMostAvailable(nodeStats);
            assertNull("node_2 should have been skipped", leastNode);
            assertNull("node_2 should have been skipped", mostNode);
        }

        {
            FsInfo.Path[] node3FSInfo = new FsInfo.Path[] {
                new FsInfo.Path("/most", "/dev/sda", 100, 90, 70),
                new FsInfo.Path("/least", "/dev/sda", 10, -1, 0), };
            NodeStats nodeStats = new NodeStats(
                DiscoveryNodeUtils.builder("node_3").roles(emptySet()).build(),
                0,
                null,
                null,
                null,
                null,
                null,
                new FsInfo(0, null, node3FSInfo),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            );

            DiskUsage leastNode = DiskUsage.findLeastAvailablePath(nodeStats);
            DiskUsage mostNode = DiskUsage.findMostAvailable(nodeStats);
            assertDiskUsage(leastNode, node3FSInfo[1]);
            assertDiskUsage(mostNode, node3FSInfo[0]);
        }
    }

    private void assertDiskUsage(DiskUsage usage, FsInfo.Path path) {
        assertNotNull(usage);
        assertNotNull(path);
        assertEquals(usage.toString(), usage.getPath(), path.getPath());
        assertEquals(usage.toString(), usage.getTotalBytes(), path.getTotal().getBytes());
        assertEquals(usage.toString(), usage.getFreeBytes(), path.getAvailable().getBytes());

    }
}
