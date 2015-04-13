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

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.TransportIndicesStatsAction;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.monitor.fs.FsStats;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 0)
public class MockDiskUsagesTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                        // Use the mock internal cluster info service, which has fake-able disk usages
                .put(ClusterModule.CLUSTER_SERVICE_IMPL, MockInternalClusterInfoService.class.getName())
                        // Update more frequently
                .put(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL, "2s")
                .build();
    }

    @Test
    //@TestLogging("org.elasticsearch.cluster:TRACE,org.elasticsearch.cluster.routing.allocation.decider:TRACE")
    public void testRerouteOccursOnDiskpassingHighWatermark() throws Exception {
        List<String> nodes = internalCluster().startNodesAsync(3).get();

        // Wait for all 3 nodes to be up
        assertBusy(new Runnable() {
            @Override
            public void run() {
                NodesStatsResponse resp = client().admin().cluster().prepareNodesStats().get();
                assertThat(resp.getNodes().length, equalTo(3));
            }
        });

        // Start with all nodes at 50% usage
        final MockInternalClusterInfoService cis = (MockInternalClusterInfoService)
                internalCluster().getInstance(ClusterInfoService.class, internalCluster().getMasterName());
        cis.setN1Usage(nodes.get(0), new DiskUsage(nodes.get(0), "n1", 100, 50));
        cis.setN2Usage(nodes.get(1), new DiskUsage(nodes.get(1), "n2", 100, 50));
        cis.setN3Usage(nodes.get(2), new DiskUsage(nodes.get(2), "n3", 100, 50));

        client().admin().cluster().prepareUpdateSettings().setTransientSettings(settingsBuilder()
                .put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK, randomFrom("20b", "80%"))
                .put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK, randomFrom("10b", "90%"))
                .put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL, "1s")).get();

        // Create an index with 10 shards so we can check allocation for it
        prepareCreate("test").setSettings(settingsBuilder()
                .put("number_of_shards", 10)
                .put("number_of_replicas", 0)
                .put("index.routing.allocation.exclude._name", "")).get();
        ensureGreen("test");

        // Block until the "fake" cluster info is retrieved at least once
        assertBusy(new Runnable() {
            @Override
            public void run() {
                ClusterInfo info = cis.getClusterInfo();
                logger.info("--> got: {} nodes", info.getNodeDiskUsages().size());
                assertThat(info.getNodeDiskUsages().size(), greaterThan(0));
            }
        });

        List<String> realNodeNames = newArrayList();
        ClusterStateResponse resp = client().admin().cluster().prepareState().get();
        Iterator<RoutingNode> iter = resp.getState().getRoutingNodes().iterator();
        while (iter.hasNext()) {
            RoutingNode node = iter.next();
            realNodeNames.add(node.nodeId());
            logger.info("--> node {} has {} shards",
                    node.nodeId(), resp.getState().getRoutingNodes().node(node.nodeId()).numberOfOwningShards());
        }

        // Update the disk usages so one node has now passed the high watermark
        cis.setN1Usage(realNodeNames.get(0), new DiskUsage(nodes.get(0), "n1", 100, 50));
        cis.setN2Usage(realNodeNames.get(1), new DiskUsage(nodes.get(1), "n2", 100, 50));
        cis.setN3Usage(realNodeNames.get(2), new DiskUsage(nodes.get(2), "n3", 100, 0)); // nothing free on node3

        // Cluster info gathering interval is 2 seconds, give reroute 2 seconds to kick in
        Thread.sleep(4000);

        // Retrieve the count of shards on each node
        resp = client().admin().cluster().prepareState().get();
        iter = resp.getState().getRoutingNodes().iterator();
        Map<String, Integer> nodesToShardCount = newHashMap();
        while (iter.hasNext()) {
            RoutingNode node = iter.next();
            logger.info("--> node {} has {} shards",
                    node.nodeId(), resp.getState().getRoutingNodes().node(node.nodeId()).numberOfOwningShards());
            nodesToShardCount.put(node.nodeId(), resp.getState().getRoutingNodes().node(node.nodeId()).numberOfOwningShards());
        }
        assertThat("node1 has 5 shards", nodesToShardCount.get(realNodeNames.get(0)), equalTo(5));
        assertThat("node2 has 5 shards", nodesToShardCount.get(realNodeNames.get(1)), equalTo(5));
        assertThat("node3 has 0 shards", nodesToShardCount.get(realNodeNames.get(2)), equalTo(0));
    }

    /** Create a fake NodeStats for the given node and usage */
    public static NodeStats makeStats(String nodeName, DiskUsage usage) {
        FsStats.Info[] infos = new FsStats.Info[1];
        FsStats.Info info = new FsStats.Info("/path.data", null, null,
                usage.getTotalBytes(), usage.getFreeBytes(), usage.getFreeBytes(), -1, -1, -1, -1, -1, -1);
        infos[0] = info;
        FsStats fsStats = new FsStats(System.currentTimeMillis(), infos);
        return new NodeStats(new DiscoveryNode(nodeName, null, Version.V_2_0_0),
                System.currentTimeMillis(),
                null, null, null, null, null, null,
                fsStats,
                null, null, null);
    }

    /**
     * Fake ClusterInfoService class that allows updating the nodes stats disk
     * usage with fake values
     */
    public static class MockInternalClusterInfoService extends InternalClusterInfoService {

        private final ClusterName clusterName;
        private volatile NodeStats[] stats = new NodeStats[3];

        @Inject
        public MockInternalClusterInfoService(Settings settings, NodeSettingsService nodeSettingsService,
                                              TransportNodesStatsAction transportNodesStatsAction,
                                              TransportIndicesStatsAction transportIndicesStatsAction,
                                              ClusterService clusterService, ThreadPool threadPool) {
            super(settings, nodeSettingsService, transportNodesStatsAction, transportIndicesStatsAction, clusterService, threadPool);
            this.clusterName = ClusterName.clusterNameFromSettings(settings);
            stats[0] = makeStats("node_t1", new DiskUsage("node_t1", "n1", 100, 100));
            stats[1] = makeStats("node_t2", new DiskUsage("node_t2", "n2", 100, 100));
            stats[2] = makeStats("node_t3", new DiskUsage("node_t3", "n3", 100, 100));
        }

        public void setN1Usage(String nodeName, DiskUsage newUsage) {
            stats[0] = makeStats(nodeName, newUsage);
        }

        public void setN2Usage(String nodeName, DiskUsage newUsage) {
            stats[1] = makeStats(nodeName, newUsage);
        }

        public void setN3Usage(String nodeName, DiskUsage newUsage) {
            stats[2] = makeStats(nodeName, newUsage);
        }

        @Override
        public CountDownLatch updateNodeStats(final ActionListener<NodesStatsResponse> listener) {
            NodesStatsResponse response = new NodesStatsResponse(clusterName, stats);
            listener.onResponse(response);
            return new CountDownLatch(0);
        }

        @Override
        public CountDownLatch updateIndicesStats(final ActionListener<IndicesStatsResponse> listener) {
            // Not used, so noop
            return new CountDownLatch(0);
        }
    }
}
