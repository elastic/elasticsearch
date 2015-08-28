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
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class DiskUsageTests extends ESTestCase {

    @Test
    public void diskUsageCalcTest() {
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

    @Test
    public void randomDiskUsageTest() {
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

    public void testFillDiskUsage() {
        Map<String, DiskUsage> newLeastAvaiableUsages = new HashMap<>();
        Map<String, DiskUsage> newMostAvaiableUsages = new HashMap<>();
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
        NodeStats[] nodeStats = new NodeStats[] {
                new NodeStats(new DiscoveryNode("node_1", DummyTransportAddress.INSTANCE, Version.CURRENT), 0,
                        null,null,null,null,null,new FsInfo(0, node1FSInfo), null,null,null,null),
                new NodeStats(new DiscoveryNode("node_2", DummyTransportAddress.INSTANCE, Version.CURRENT), 0,
                        null,null,null,null,null, new FsInfo(0, node2FSInfo), null,null,null,null),
                new NodeStats(new DiscoveryNode("node_3", DummyTransportAddress.INSTANCE, Version.CURRENT), 0,
                        null,null,null,null,null, new FsInfo(0, node3FSInfo), null,null,null,null)
        };
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

    private void assertDiskUsage(DiskUsage usage, FsInfo.Path path) {
        assertEquals(usage.toString(), usage.getPath(), path.getPath());
        assertEquals(usage.toString(), usage.getTotalBytes(), path.getTotal().bytes());
        assertEquals(usage.toString(), usage.getFreeBytes(), path.getAvailable().bytes());

    }
}
