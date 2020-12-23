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

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.Build;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStatsTests;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.monitor.process.ProcessInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportInfo;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
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
        assertEquals("{\"transport_types\":{},\"http_types\":{}}",
                toXContent(stats, XContentType.JSON, randomBoolean()).utf8ToString());

        List<NodeInfo> nodeInfos = singletonList(createNodeInfo("node_0", null, null));
        stats = new ClusterStatsNodes.NetworkTypes(nodeInfos);
        assertEquals("{\"transport_types\":{},\"http_types\":{}}",
                toXContent(stats, XContentType.JSON, randomBoolean()).utf8ToString());

        nodeInfos = Arrays.asList(createNodeInfo("node_1", "", ""),
                                  createNodeInfo("node_2", "custom", "custom"),
                                  createNodeInfo("node_3", null, "custom"));
        stats = new ClusterStatsNodes.NetworkTypes(nodeInfos);
        assertEquals("{"
                + "\"transport_types\":{\"custom\":1},"
                + "\"http_types\":{\"custom\":2}"
        + "}", toXContent(stats, XContentType.JSON, randomBoolean()).utf8ToString());
    }

    public void testIngestStats() throws Exception {
        NodeStats nodeStats = randomValueOtherThanMany(n -> n.getIngestStats() == null, NodeStatsTests::createNodeStats);
        SortedMap<String, long[]> processorStats = new TreeMap<>();
        nodeStats.getIngestStats().getProcessorStats().values().forEach(stats -> {
            stats.forEach(stat -> {
                processorStats.compute(stat.getType(), (key, value) -> {
                    if (value == null) {
                        return new long[] { stat.getStats().getIngestCount(), stat.getStats().getIngestFailedCount(),
                            stat.getStats().getIngestCurrent(), stat.getStats().getIngestTimeInMillis()};
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
        String processorStatsString = "{";
        Iterator<Map.Entry<String, long[]>> iter = processorStats.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, long[]> entry = iter.next();
            long[] statValues = entry.getValue();
            long count = statValues[0];
            long failedCount = statValues[1];
            long current = statValues[2];
            long timeInMillis = statValues[3];
            processorStatsString += "\"" + entry.getKey() + "\":{\"count\":" + count
                + ",\"failed\":" + failedCount
                + ",\"current\":" + current
                + ",\"time_in_millis\":" + timeInMillis
                + "}";
            if (iter.hasNext()) {
                processorStatsString += ",";
            }
        }
        processorStatsString += "}";
        assertThat(toXContent(stats, XContentType.JSON, false).utf8ToString(), equalTo(
            "{\"ingest\":{"
                + "\"number_of_pipelines\":" + stats.pipelineCount + ","
                + "\"processor_stats\":" + processorStatsString
                + "}}"));
    }

    public void testClusterFsTotal() throws Exception {
        List<ClusterStatsNodeResponse> statsNodeResponses = Arrays.asList(
            createNodeStatResponse("node_0", "192.168.0.10", 9300,
                new FsInfo.Path[]{
                    createFsInfoPath("/data00/dnode0/nodes/0", "/data00 (/dev/sdb)", 10737418240l, 50000l, 50000l)
                }),
            createNodeStatResponse("node_1", "192.168.0.10", 9310,
                new FsInfo.Path[]{
                    createFsInfoPath("/data01/dnode0/nodes/0", "/data01 (/dev/sdc)", 10737418240l, 50000l, 50000l)
                }),
            createNodeStatResponse("node_2", "192.168.0.11", 9300,
                new FsInfo.Path[]{
                    createFsInfoPath("/data01/dnode0/nodes/0", "/data00 (/dev/sdb)", 10737418240l, 50000l, 50000l)
                }),
            createNodeStatResponse("node_3", "192.168.0.11", 9310,
                new FsInfo.Path[]{
                    createFsInfoPath("/data01/dnode0/nodes/0", "/data01 (/dev/sdc)", 10737418240l, 50000l, 50000l)
                }),
            createNodeStatResponse("node_4", "192.168.0.11", 9320,
                new FsInfo.Path[]{
                    createFsInfoPath("/data01/dnode1/nodes/0", "/data01 (/dev/sdc)", 10737418240l, 50000l, 50000l)
                }),
            createNodeStatResponse("node_5", "192.168.0.12", 9300,
                new FsInfo.Path[]{
                    createFsInfoPath("/data00/dnode0/nodes/0", "/data00 (/dev/sdb)", 6442450944l, 50000l, 50000l),
                    createFsInfoPath("/data01/dnode0/nodes/0", "/data01 (/dev/sdc)", 6442450944l, 50000l, 50000l),
                    createFsInfoPath("/data01/dnode1/nodes/0", "/data01 (/dev/sdc)", 6442450944l, 50000l, 50000l)
                }));
        ClusterStatsNodes stats = new ClusterStatsNodes(statsNodeResponses);
        assertEquals(new ByteSizeValue(4*10737418240l + 2*6442450944l), stats.getFs().getTotal());

    }

    private static NodeInfo createNodeInfo(String nodeId, String transportType, String httpType) {
        Settings.Builder settings = Settings.builder();
        if (transportType != null) {
            settings.put(randomFrom(NetworkModule.TRANSPORT_TYPE_KEY,
                    NetworkModule.TRANSPORT_TYPE_DEFAULT_KEY), transportType);
        }
        if (httpType != null) {
            settings.put(randomFrom(NetworkModule.HTTP_TYPE_KEY,
                    NetworkModule.HTTP_TYPE_DEFAULT_KEY), httpType);
        }
        return new NodeInfo(null, null,
                new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), null),
                settings.build(), null, null, null, null, null, null, null, null, null, null);
    }

    private static ClusterStatsNodeResponse createNodeStatResponse(String nodeId, String hostname, Integer port, FsInfo.Path[] fsInfoPaths) throws Exception {

        DiscoveryNode discoveryNode = new DiscoveryNode(nodeId, new TransportAddress(new InetSocketAddress(hostname, port)), null);
        return new ClusterStatsNodeResponse(new DiscoveryNode(nodeId, new TransportAddress(new InetSocketAddress(hostname, port)), null), null,
            createNodeInfo2(nodeId, hostname, port),
            createNodeStats(discoveryNode, fsInfoPaths),
            null);
    }

    private static NodeInfo createNodeInfo2(String nodeId, String hostname, Integer port){
        return new NodeInfo(null, new Build(Build.Flavor.DEFAULT, Build.Type.TAR, "", "", false, ""),
            new DiscoveryNode(nodeId, new TransportAddress(new InetSocketAddress(hostname, port)), null),
            Settings.builder().build(), new OsInfo(1l, 1, 1, "", "", "", ""),
            new ProcessInfo(1l, true, 1l),
            JvmInfo.jvmInfo(), null,
            new TransportInfo(new BoundTransportAddress(new TransportAddress[]{
                new TransportAddress(new InetSocketAddress(hostname, port))
            }, new TransportAddress(new InetSocketAddress(hostname, port))), null), null,
            new PluginsAndModules(Collections.emptyList(), Collections.emptyList()),null, null, null);
    }

    private static NodeStats createNodeStats(DiscoveryNode discoveryNode, FsInfo.Path[] fsInfoPaths) throws UnknownHostException {

        long timestamp = 1608809284528l;
        return new NodeStats(discoveryNode, timestamp, null, null ,null, null,
            null, new FsInfo(timestamp, null, fsInfoPaths),
            null, null, null, null,null, null, null, null);
    }

    private static FsInfo.Path createFsInfoPath(String path, String mount, long total, long free, long available){
        return new FsInfo.Path(path, mount, total, free, available);
    }
}
