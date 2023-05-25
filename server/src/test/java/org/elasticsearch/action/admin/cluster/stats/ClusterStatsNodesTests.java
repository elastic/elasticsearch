/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStatsTests;
import org.elasticsearch.cluster.node.TestDiscoveryNode;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.stats.IndexingPressureStats;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.net.InetAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.hamcrest.Matchers.equalTo;

public class ClusterStatsNodesTests extends ESTestCase {

    /**
     * Test that empty transport/http types are not printed out as part
     * of the cluster stats xcontent output.
     */
    public void testNetworkTypesToXContent() throws Exception {
        ClusterStatsNodes.NetworkTypes stats = new ClusterStatsNodes.NetworkTypes(List.of());
        assertEquals("{\"transport_types\":{},\"http_types\":{}}", toXContent(stats, XContentType.JSON, randomBoolean()).utf8ToString());

        List<NodeInfo> nodeInfos = List.of(createNodeInfo("node_0", null, null));
        stats = new ClusterStatsNodes.NetworkTypes(nodeInfos);
        assertEquals("{\"transport_types\":{},\"http_types\":{}}", toXContent(stats, XContentType.JSON, randomBoolean()).utf8ToString());

        nodeInfos = List.of(
            createNodeInfo("node_1", "", ""),
            createNodeInfo("node_2", "custom", "custom"),
            createNodeInfo("node_3", null, "custom")
        );
        stats = new ClusterStatsNodes.NetworkTypes(nodeInfos);
        assertEquals(
            """
                {"transport_types":{"custom":1},"http_types":{"custom":2}}""",
            toXContent(stats, XContentType.JSON, randomBoolean()).utf8ToString()
        );
    }

    public void testIngestStats() throws Exception {
        NodeStats nodeStats = randomValueOtherThanMany(n -> n.getIngestStats() == null, NodeStatsTests::createNodeStats);
        SortedMap<String, long[]> processorStats = new TreeMap<>();
        nodeStats.getIngestStats().processorStats().values().forEach(stats -> {
            stats.forEach(stat -> {
                processorStats.compute(stat.type(), (key, value) -> {
                    if (value == null) {
                        return new long[] {
                            stat.stats().ingestCount(),
                            stat.stats().ingestFailedCount(),
                            stat.stats().ingestCurrent(),
                            stat.stats().ingestTimeInMillis() };
                    } else {
                        value[0] += stat.stats().ingestCount();
                        value[1] += stat.stats().ingestFailedCount();
                        value[2] += stat.stats().ingestCurrent();
                        value[3] += stat.stats().ingestTimeInMillis();
                        return value;
                    }
                });
            });
        });

        ClusterStatsNodes.IngestStats stats = new ClusterStatsNodes.IngestStats(List.of(nodeStats));
        assertThat(stats.pipelineCount, equalTo(nodeStats.getIngestStats().processorStats().size()));
        StringBuilder processorStatsString = new StringBuilder("{");
        Iterator<Map.Entry<String, long[]>> iter = processorStats.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, long[]> entry = iter.next();
            long[] statValues = entry.getValue();
            long count = statValues[0];
            long failedCount = statValues[1];
            long current = statValues[2];
            long timeInMillis = statValues[3];
            processorStatsString.append(Strings.format("""
                "%s":{"count":%s,"failed":%s,"current":%s,"time_in_millis":%s}\
                """, entry.getKey(), count, failedCount, current, timeInMillis));
            if (iter.hasNext()) {
                processorStatsString.append(",");
            }
        }
        processorStatsString.append("}");
        assertThat(toXContent(stats, XContentType.JSON, false).utf8ToString(), equalTo(Strings.format("""
            {"ingest":{"number_of_pipelines":%s,"processor_stats":%s}}\
            """, stats.pipelineCount, processorStatsString)));
    }

    public void testIndexPressureStats() throws Exception {
        List<NodeStats> nodeStats = List.of(
            randomValueOtherThanMany(n -> n.getIndexingPressureStats() == null, NodeStatsTests::createNodeStats),
            randomValueOtherThanMany(n -> n.getIndexingPressureStats() == null, NodeStatsTests::createNodeStats)
        );
        long[] expectedStats = new long[12];
        for (NodeStats nodeStat : nodeStats) {
            IndexingPressureStats indexingPressureStats = nodeStat.getIndexingPressureStats();
            if (indexingPressureStats != null) {
                expectedStats[0] += indexingPressureStats.getCurrentCombinedCoordinatingAndPrimaryBytes();
                expectedStats[1] += indexingPressureStats.getCurrentCoordinatingBytes();
                expectedStats[2] += indexingPressureStats.getCurrentPrimaryBytes();
                expectedStats[3] += indexingPressureStats.getCurrentReplicaBytes();

                expectedStats[4] += indexingPressureStats.getTotalCombinedCoordinatingAndPrimaryBytes();
                expectedStats[5] += indexingPressureStats.getTotalCoordinatingBytes();
                expectedStats[6] += indexingPressureStats.getTotalPrimaryBytes();
                expectedStats[7] += indexingPressureStats.getTotalReplicaBytes();

                expectedStats[8] += indexingPressureStats.getCoordinatingRejections();
                expectedStats[9] += indexingPressureStats.getPrimaryRejections();
                expectedStats[10] += indexingPressureStats.getReplicaRejections();

                expectedStats[11] += indexingPressureStats.getMemoryLimit();
            }
        }

        ClusterStatsNodes.IndexPressureStats indexPressureStats = new ClusterStatsNodes.IndexPressureStats(nodeStats);
        assertThat(
            toXContent(indexPressureStats, XContentType.JSON, false).utf8ToString(),
            equalTo(
                "{\"indexing_pressure\":{"
                    + "\"memory\":{"
                    + "\"current\":{"
                    + "\"combined_coordinating_and_primary_in_bytes\":"
                    + expectedStats[0]
                    + ","
                    + "\"coordinating_in_bytes\":"
                    + expectedStats[1]
                    + ","
                    + "\"primary_in_bytes\":"
                    + expectedStats[2]
                    + ","
                    + "\"replica_in_bytes\":"
                    + expectedStats[3]
                    + ","
                    + "\"all_in_bytes\":"
                    + (expectedStats[3] + expectedStats[0])
                    + "},"
                    + "\"total\":{"
                    + "\"combined_coordinating_and_primary_in_bytes\":"
                    + expectedStats[4]
                    + ","
                    + "\"coordinating_in_bytes\":"
                    + expectedStats[5]
                    + ","
                    + "\"primary_in_bytes\":"
                    + expectedStats[6]
                    + ","
                    + "\"replica_in_bytes\":"
                    + expectedStats[7]
                    + ","
                    + "\"all_in_bytes\":"
                    + (expectedStats[7] + expectedStats[4])
                    + ","
                    + "\"coordinating_rejections\":"
                    + expectedStats[8]
                    + ","
                    + "\"primary_rejections\":"
                    + expectedStats[9]
                    + ","
                    + "\"replica_rejections\":"
                    + expectedStats[10]
                    + "},"
                    + "\"limit_in_bytes\":"
                    + expectedStats[11]
                    + "}"
                    + "}}"
            )
        );
    }

    public void testClusterFsStatsDeduplicator() {
        {
            // single node, multiple data paths, different devices
            InetAddress address1 = InetAddresses.forString("192.168.0.1");
            FsInfo.Path path1 = new FsInfo.Path("/a", "/dev/sda", 3, 2, 1);
            FsInfo.Path path2 = new FsInfo.Path("/b", "/dev/sdb", 3, 2, 1);
            ClusterStatsNodes.ClusterFsStatsDeduplicator deduplicator = new ClusterStatsNodes.ClusterFsStatsDeduplicator(1);
            deduplicator.add(address1, newFsInfo(path1, path2));
            FsInfo.Path total = deduplicator.getTotal();

            // since they're different devices, they sum
            assertThat(total.getTotal().getBytes(), equalTo(6L));
            assertThat(total.getFree().getBytes(), equalTo(4L));
            assertThat(total.getAvailable().getBytes(), equalTo(2L));
        }

        {
            // single node, multiple data paths, same device
            InetAddress address1 = InetAddresses.forString("192.168.0.1");
            FsInfo.Path path1 = new FsInfo.Path("/data/a", "/dev/sda", 3, 2, 1);
            FsInfo.Path path2 = new FsInfo.Path("/data/b", "/dev/sda", 3, 2, 1);
            ClusterStatsNodes.ClusterFsStatsDeduplicator deduplicator = new ClusterStatsNodes.ClusterFsStatsDeduplicator(1);
            deduplicator.add(address1, newFsInfo(path1, path2));
            FsInfo.Path total = deduplicator.getTotal();

            // since it's the same device, they don't sum, we just see the one
            assertThat(total.getTotal().getBytes(), equalTo(3L));
            assertThat(total.getFree().getBytes(), equalTo(2L));
            assertThat(total.getAvailable().getBytes(), equalTo(1L));
        }

        {
            // two nodes, same ip address, but different data paths on different devices
            InetAddress address1 = InetAddresses.forString("192.168.0.1");
            FsInfo.Path path1 = new FsInfo.Path("/data/a", "/dev/sda", 3, 2, 1);
            InetAddress address2 = InetAddresses.forString("192.168.0.1");
            FsInfo.Path path2 = new FsInfo.Path("/data/b", "/dev/sdb", 3, 2, 1);
            ClusterStatsNodes.ClusterFsStatsDeduplicator deduplicator = new ClusterStatsNodes.ClusterFsStatsDeduplicator(1);
            deduplicator.add(address1, newFsInfo(path1));
            deduplicator.add(address2, newFsInfo(path2));
            FsInfo.Path total = deduplicator.getTotal();

            // since they're different devices, they sum
            assertThat(total.getTotal().getBytes(), equalTo(6L));
            assertThat(total.getFree().getBytes(), equalTo(4L));
            assertThat(total.getAvailable().getBytes(), equalTo(2L));
        }

        {
            // two nodes, different ip addresses, different data paths, same device
            InetAddress address1 = InetAddresses.forString("192.168.0.1");
            FsInfo.Path path1 = new FsInfo.Path("/data/a", "/dev/sda", 3, 2, 1);
            InetAddress address2 = InetAddresses.forString("192.168.0.2");
            FsInfo.Path path2 = new FsInfo.Path("/data/b", "/dev/sda", 3, 2, 1);
            ClusterStatsNodes.ClusterFsStatsDeduplicator deduplicator = new ClusterStatsNodes.ClusterFsStatsDeduplicator(1);
            deduplicator.add(address1, newFsInfo(path1));
            deduplicator.add(address2, newFsInfo(path2));
            FsInfo.Path total = deduplicator.getTotal();

            // it's the same device, yeah, but on entirely different machines, so they sum
            assertThat(total.getTotal().getBytes(), equalTo(6L));
            assertThat(total.getFree().getBytes(), equalTo(4L));
            assertThat(total.getAvailable().getBytes(), equalTo(2L));
        }

        {
            // two nodes, same ip address, same data path, same device
            InetAddress address1 = InetAddresses.forString("192.168.0.1");
            FsInfo.Path path1 = new FsInfo.Path("/app/data", "/app (/dev/mapper/lxc-data)", 3, 2, 1);
            InetAddress address2 = InetAddresses.forString("192.168.0.1");
            FsInfo.Path path2 = new FsInfo.Path("/app/data", "/app (/dev/mapper/lxc-data)", 3, 2, 1);
            ClusterStatsNodes.ClusterFsStatsDeduplicator deduplicator = new ClusterStatsNodes.ClusterFsStatsDeduplicator(1);
            deduplicator.add(address1, newFsInfo(path1));
            deduplicator.add(address2, newFsInfo(path2));
            FsInfo.Path total = deduplicator.getTotal();

            // wait a second, this is the super-special case -- you can't actually have two nodes doing this unless something
            // very interesting is happening, so they sum (i.e. we assume the operator is doing smart things)
            assertThat(total.getTotal().getBytes(), equalTo(6L));
            assertThat(total.getFree().getBytes(), equalTo(4L));
            assertThat(total.getAvailable().getBytes(), equalTo(2L));
        }

        {
            // two nodes, same ip address, different data paths, same device
            InetAddress address1 = InetAddresses.forString("192.168.0.1");
            FsInfo.Path path1 = new FsInfo.Path("/app/data1", "/dev/sda", 3, 2, 1);
            InetAddress address2 = InetAddresses.forString("192.168.0.1");
            FsInfo.Path path2 = new FsInfo.Path("/app/data2", "/dev/sda", 3, 2, 1);
            ClusterStatsNodes.ClusterFsStatsDeduplicator deduplicator = new ClusterStatsNodes.ClusterFsStatsDeduplicator(1);
            deduplicator.add(address1, newFsInfo(path1));
            deduplicator.add(address2, newFsInfo(path2));
            FsInfo.Path total = deduplicator.getTotal();

            // since the paths aren't the same, it doesn't trigger the special case -- it's just the same device and doesn't sum
            assertThat(total.getTotal().getBytes(), equalTo(3L));
            assertThat(total.getFree().getBytes(), equalTo(2L));
            assertThat(total.getAvailable().getBytes(), equalTo(1L));
        }

        {
            // two nodes, same ip address, same data path, different devices
            InetAddress address1 = InetAddresses.forString("192.168.0.1");
            FsInfo.Path path1 = new FsInfo.Path("/app/data", "/dev/sda", 3, 2, 1);
            InetAddress address2 = InetAddresses.forString("192.168.0.1");
            FsInfo.Path path2 = new FsInfo.Path("/app/data", "/dev/sdb", 3, 2, 1);
            ClusterStatsNodes.ClusterFsStatsDeduplicator deduplicator = new ClusterStatsNodes.ClusterFsStatsDeduplicator(1);
            deduplicator.add(address1, newFsInfo(path1));
            deduplicator.add(address2, newFsInfo(path2));
            FsInfo.Path total = deduplicator.getTotal();

            // having the same path isn't special in this case, it's just unique ip/mount pairs, so they sum
            assertThat(total.getTotal().getBytes(), equalTo(6L));
            assertThat(total.getFree().getBytes(), equalTo(4L));
            assertThat(total.getAvailable().getBytes(), equalTo(2L));
        }
    }

    private static FsInfo newFsInfo(FsInfo.Path... paths) {
        return new FsInfo(-1, null, paths);
    }

    private static NodeInfo createNodeInfo(String nodeId, String transportType, String httpType) {
        Settings.Builder settings = Settings.builder();
        if (transportType != null) {
            settings.put(randomFrom(NetworkModule.TRANSPORT_TYPE_KEY, NetworkModule.TRANSPORT_TYPE_DEFAULT_KEY), transportType);
        }
        if (httpType != null) {
            settings.put(randomFrom(NetworkModule.HTTP_TYPE_KEY, NetworkModule.HTTP_TYPE_DEFAULT_KEY), httpType);
        }
        return new NodeInfo(
            Version.CURRENT,
            TransportVersion.CURRENT,
            Build.CURRENT,
            TestDiscoveryNode.create(nodeId, buildNewFakeTransportAddress()),
            settings.build(),
            null,
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
    }
}
