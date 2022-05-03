/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStatsTests;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.stats.IndexingPressureStats;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.hamcrest.Matchers.equalTo;

public class ClusterStatsNodesTests extends ESTestCase {

    /**
     * Test that empty transport/http types are not printed out as part
     * of the cluster stats xcontent output.
     */
    public void testNetworkTypesToXContent() throws Exception {
        ClusterStatsNodes.NetworkTypes stats = new ClusterStatsNodes.NetworkTypes(emptyList());
        assertEquals("{\"transport_types\":{},\"http_types\":{}}", toXContent(stats, XContentType.JSON, randomBoolean()).utf8ToString());

        List<NodeInfo> nodeInfos = singletonList(createNodeInfo("node_0", null, null));
        stats = new ClusterStatsNodes.NetworkTypes(nodeInfos);
        assertEquals("{\"transport_types\":{},\"http_types\":{}}", toXContent(stats, XContentType.JSON, randomBoolean()).utf8ToString());

        nodeInfos = Arrays.asList(
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
        nodeStats.getIngestStats().getProcessorStats().values().forEach(stats -> {
            stats.forEach(stat -> {
                processorStats.compute(stat.getType(), (key, value) -> {
                    if (value == null) {
                        return new long[] {
                            stat.getStats().getIngestCount(),
                            stat.getStats().getIngestFailedCount(),
                            stat.getStats().getIngestCurrent(),
                            stat.getStats().getIngestTimeInMillis() };
                    } else {
                        value[0] += stat.getStats().getIngestCount();
                        value[1] += stat.getStats().getIngestFailedCount();
                        value[2] += stat.getStats().getIngestCurrent();
                        value[3] += stat.getStats().getIngestTimeInMillis();
                        return value;
                    }
                });
            });
        });

        ClusterStatsNodes.IngestStats stats = new ClusterStatsNodes.IngestStats(Collections.singletonList(nodeStats));
        assertThat(stats.pipelineCount, equalTo(nodeStats.getIngestStats().getProcessorStats().size()));
        StringBuilder processorStatsString = new StringBuilder("{");
        Iterator<Map.Entry<String, long[]>> iter = processorStats.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, long[]> entry = iter.next();
            long[] statValues = entry.getValue();
            long count = statValues[0];
            long failedCount = statValues[1];
            long current = statValues[2];
            long timeInMillis = statValues[3];
            processorStatsString.append("""
                "%s":{"count":%s,"failed":%s,"current":%s,"time_in_millis":%s}\
                """.formatted(entry.getKey(), count, failedCount, current, timeInMillis));
            if (iter.hasNext()) {
                processorStatsString.append(",");
            }
        }
        processorStatsString.append("}");
        assertThat(toXContent(stats, XContentType.JSON, false).utf8ToString(), equalTo("""
            {"ingest":{"number_of_pipelines":%s,"processor_stats":%s}}\
            """.formatted(stats.pipelineCount, processorStatsString)));
    }

    public void testIndexPressureStats() throws Exception {
        List<NodeStats> nodeStats = Arrays.asList(
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

    private static NodeInfo createNodeInfo(String nodeId, String transportType, String httpType) {
        Settings.Builder settings = Settings.builder();
        if (transportType != null) {
            settings.put(randomFrom(NetworkModule.TRANSPORT_TYPE_KEY, NetworkModule.TRANSPORT_TYPE_DEFAULT_KEY), transportType);
        }
        if (httpType != null) {
            settings.put(randomFrom(NetworkModule.HTTP_TYPE_KEY, NetworkModule.HTTP_TYPE_DEFAULT_KEY), httpType);
        }
        return new NodeInfo(
            null,
            null,
            new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), null),
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
            null
        );
    }
}
