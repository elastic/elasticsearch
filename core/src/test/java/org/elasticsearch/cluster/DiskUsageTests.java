/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource.PeerRecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;

public class DiskUsageTests extends ESTestCase {
    public void testDiskUsageCalc() {
        DiskUsage du = new DiskUsage("node1", "n1", "random", 100, 40);
        assertThat(du.getFreeDiskAsPercentage(), equalTo(40.0));
        assertThat(du.getUsedDiskAsPercentage(), equalTo(100.0 - 40.0));
        assertThat(du.getFreeBytes(), equalTo(40L));
        assertThat(du.getUsedBytes(), equalTo(60L));
        assertThat(du.getTotalBytes(), equalTo(100L));

        // Test that DiskUsage handles invalid numbers, as reported by some
        // filesystems (ZFS & NTFS)
        DiskUsage du2 = new DiskUsage("node1", "n1","random", 100, 101);
        assertThat(du2.getFreeDiskAsPercentage(), equalTo(101.0));
        assertThat(du2.getFreeBytes(), equalTo(101L));
        assertThat(du2.getUsedBytes(), equalTo(-1L));
        assertThat(du2.getTotalBytes(), equalTo(100L));

        DiskUsage du3 = new DiskUsage("node1", "n1", "random",-1, -1);
        assertThat(du3.getFreeDiskAsPercentage(), equalTo(100.0));
        assertThat(du3.getFreeBytes(), equalTo(-1L));
        assertThat(du3.getUsedBytes(), equalTo(0L));
        assertThat(du3.getTotalBytes(), equalTo(-1L));

        DiskUsage du4 = new DiskUsage("node1", "n1","random", 0, 0);
        assertThat(du4.getFreeDiskAsPercentage(), equalTo(100.0));
        assertThat(du4.getFreeBytes(), equalTo(0L));
        assertThat(du4.getUsedBytes(), equalTo(0L));
        assertThat(du4.getTotalBytes(), equalTo(0L));
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
                assertThat(du.getFreeDiskAsPercentage(), equalTo(100.0 * ((double) free / total)));
                assertThat(du.getUsedDiskAsPercentage(), equalTo(100.0 - (100.0 * ((double) free / total))));
            }
        }
    }

    public void testFillShardLevelInfo() {
        final Index index = new Index("test", "0xdeadbeef");
        ShardRouting test_0 = ShardRouting.newUnassigned(new ShardId(index, 0), false, PeerRecoverySource.INSTANCE, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"));
        test_0 = ShardRoutingHelper.initialize(test_0, "node1");
        test_0 = ShardRoutingHelper.moveToStarted(test_0);
        Path test0Path = createTempDir().resolve("indices").resolve(index.getUUID()).resolve("0");
        CommonStats commonStats0 = new CommonStats();
        commonStats0.store = new StoreStats(100);
        ShardRouting test_1 = ShardRouting.newUnassigned(new ShardId(index, 1), false, PeerRecoverySource.INSTANCE, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"));
        test_1 = ShardRoutingHelper.initialize(test_1, "node2");
        test_1 = ShardRoutingHelper.moveToStarted(test_1);
        Path test1Path = createTempDir().resolve("indices").resolve(index.getUUID()).resolve("1");
        CommonStats commonStats1 = new CommonStats();
        commonStats1.store = new StoreStats(1000);
        ShardStats[] stats  = new ShardStats[] {
                new ShardStats(test_0, new ShardPath(false, test0Path, test0Path, test_0.shardId()), commonStats0 , null, null),
                new ShardStats(test_1, new ShardPath(false, test1Path, test1Path, test_1.shardId()), commonStats1 , null, null)
        };
        ImmutableOpenMap.Builder<String, Long> shardSizes = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<ShardRouting, String> routingToPath = ImmutableOpenMap.builder();
        ClusterState state = ClusterState.builder(new ClusterName("blarg")).version(0).build();
        InternalClusterInfoService.buildShardLevelInfo(logger, stats, shardSizes, routingToPath, state);
        assertEquals(2, shardSizes.size());
        assertTrue(shardSizes.containsKey(ClusterInfo.shardIdentifierFromRouting(test_0)));
        assertTrue(shardSizes.containsKey(ClusterInfo.shardIdentifierFromRouting(test_1)));
        assertEquals(100L, shardSizes.get(ClusterInfo.shardIdentifierFromRouting(test_0)).longValue());
        assertEquals(1000L, shardSizes.get(ClusterInfo.shardIdentifierFromRouting(test_1)).longValue());

        assertEquals(2, routingToPath.size());
        assertTrue(routingToPath.containsKey(test_0));
        assertTrue(routingToPath.containsKey(test_1));
        assertEquals(test0Path.getParent().getParent().getParent().toAbsolutePath().toString(), routingToPath.get(test_0));
        assertEquals(test1Path.getParent().getParent().getParent().toAbsolutePath().toString(), routingToPath.get(test_1));
    }

    public void testFillDiskUsage() {
        ImmutableOpenMap.Builder<String, DiskUsage> newLeastAvaiableUsages = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, DiskUsage> newMostAvaiableUsages = ImmutableOpenMap.builder();
        FsInfo.Path[] node1FSInfo =  new FsInfo.Path[] {
                new FsInfo.Path("/middle", "/dev/sda", 100, 90, 80),
                new FsInfo.Path("/least", "/dev/sdb", 200, 190, 70),
                new FsInfo.Path("/most", "/dev/sdc", 300, 290, 280),
        };
        FsInfo.Path[] node2FSInfo = new FsInfo.Path[] {
                new FsInfo.Path("/least_most", "/dev/sda", 100, 90, 80),
        };

        FsInfo.Path[] node3FSInfo =  new FsInfo.Path[] {
                new FsInfo.Path("/least", "/dev/sda", 100, 90, 70),
                new FsInfo.Path("/most", "/dev/sda", 100, 90, 80),
        };
        List<NodeStats> nodeStats = Arrays.asList(
                new NodeStats(new DiscoveryNode("node_1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT), 0,
                        null,null,null,null,null,new FsInfo(0, null, node1FSInfo), null,null,null,null,null, null, null),
                new NodeStats(new DiscoveryNode("node_2", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT), 0,
                        null,null,null,null,null, new FsInfo(0, null, node2FSInfo), null,null,null,null,null, null, null),
                new NodeStats(new DiscoveryNode("node_3", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT), 0,
                        null,null,null,null,null, new FsInfo(0, null, node3FSInfo), null,null,null,null,null, null, null)
        );
        InternalClusterInfoService.fillDiskUsagePerNode(logger, nodeStats, newLeastAvaiableUsages, newMostAvaiableUsages);
        DiskUsage leastNode_1 = newLeastAvaiableUsages.get("node_1");
        DiskUsage mostNode_1 = newMostAvaiableUsages.get("node_1");
        assertDiskUsage(mostNode_1, node1FSInfo[2]);
        assertDiskUsage(leastNode_1, node1FSInfo[1]);

        DiskUsage leastNode_2 = newLeastAvaiableUsages.get("node_2");
        DiskUsage mostNode_2 = newMostAvaiableUsages.get("node_2");
        assertDiskUsage(leastNode_2, node2FSInfo[0]);
        assertDiskUsage(mostNode_2, node2FSInfo[0]);

        DiskUsage leastNode_3 = newLeastAvaiableUsages.get("node_3");
        DiskUsage mostNode_3 = newMostAvaiableUsages.get("node_3");
        assertDiskUsage(leastNode_3, node3FSInfo[0]);
        assertDiskUsage(mostNode_3, node3FSInfo[1]);
    }

    public void testFillDiskUsageSomeInvalidValues() {
        ImmutableOpenMap.Builder<String, DiskUsage> newLeastAvailableUsages = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, DiskUsage> newMostAvailableUsages = ImmutableOpenMap.builder();
        FsInfo.Path[] node1FSInfo =  new FsInfo.Path[] {
                new FsInfo.Path("/middle", "/dev/sda", 100, 90, 80),
                new FsInfo.Path("/least", "/dev/sdb", -1, -1, -1),
                new FsInfo.Path("/most", "/dev/sdc", 300, 290, 280),
        };
        FsInfo.Path[] node2FSInfo = new FsInfo.Path[] {
                new FsInfo.Path("/least_most", "/dev/sda", -2, -1, -1),
        };

        FsInfo.Path[] node3FSInfo =  new FsInfo.Path[] {
                new FsInfo.Path("/most", "/dev/sda", 100, 90, 70),
                new FsInfo.Path("/least", "/dev/sda", 10, -8, 0),
        };
        List<NodeStats> nodeStats = Arrays.asList(
                new NodeStats(new DiscoveryNode("node_1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT), 0,
                        null,null,null,null,null,new FsInfo(0, null, node1FSInfo), null,null,null,null,null, null, null),
                new NodeStats(new DiscoveryNode("node_2", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT), 0,
                        null,null,null,null,null, new FsInfo(0, null, node2FSInfo), null,null,null,null,null, null, null),
                new NodeStats(new DiscoveryNode("node_3", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT), 0,
                        null,null,null,null,null, new FsInfo(0, null, node3FSInfo), null,null,null,null,null, null, null)
        );
        InternalClusterInfoService.fillDiskUsagePerNode(logger, nodeStats, newLeastAvailableUsages, newMostAvailableUsages);
        DiskUsage leastNode_1 = newLeastAvailableUsages.get("node_1");
        DiskUsage mostNode_1 = newMostAvailableUsages.get("node_1");
        assertNull("node1 should have been skipped", leastNode_1);
        assertDiskUsage(mostNode_1, node1FSInfo[2]);

        DiskUsage leastNode_2 = newLeastAvailableUsages.get("node_2");
        DiskUsage mostNode_2 = newMostAvailableUsages.get("node_2");
        assertNull("node2 should have been skipped", leastNode_2);
        assertNull("node2 should have been skipped", mostNode_2);

        DiskUsage leastNode_3 = newLeastAvailableUsages.get("node_3");
        DiskUsage mostNode_3 = newMostAvailableUsages.get("node_3");
        assertDiskUsage(leastNode_3, node3FSInfo[1]);
        assertDiskUsage(mostNode_3, node3FSInfo[0]);
    }

    private void assertDiskUsage(DiskUsage usage, FsInfo.Path path) {
        assertNotNull(usage);
        assertNotNull(path);
        assertEquals(usage.toString(), usage.getPath(), path.getPath());
        assertEquals(usage.toString(), usage.getTotalBytes(), path.getTotal().getBytes());
        assertEquals(usage.toString(), usage.getFreeBytes(), path.getAvailable().getBytes());

    }
}
