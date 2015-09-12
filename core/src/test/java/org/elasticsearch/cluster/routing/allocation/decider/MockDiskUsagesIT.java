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

import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.MockInternalClusterInfoService;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class MockDiskUsagesIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                        // Update more frequently
                .put(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL, "1s")
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // Use the mock internal cluster info service, which has fake-able disk usages
        return pluginList(MockInternalClusterInfoService.TestPlugin.class);
    }

    @Test
    //@TestLogging("org.elasticsearch.cluster:TRACE,org.elasticsearch.cluster.routing.allocation.decider:TRACE")
    public void testRerouteOccursOnDiskPassingHighWatermark() throws Exception {
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
        cis.setN1Usage(nodes.get(0), new DiskUsage(nodes.get(0), "n1", "/dev/null", 100, 50));
        cis.setN2Usage(nodes.get(1), new DiskUsage(nodes.get(1), "n2", "/dev/null", 100, 50));
        cis.setN3Usage(nodes.get(2), new DiskUsage(nodes.get(2), "n3", "/dev/null", 100, 50));

        client().admin().cluster().prepareUpdateSettings().setTransientSettings(settingsBuilder()
                .put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK, randomFrom("20b", "80%"))
                .put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK, randomFrom("10b", "90%"))
                .put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL, "1ms")).get();

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
                logger.info("--> got: {} nodes", info.getNodeLeastAvailableDiskUsages().size());
                assertThat(info.getNodeLeastAvailableDiskUsages().size(), greaterThan(0));
            }
        });

        final List<String> realNodeNames = new ArrayList<>();
        ClusterStateResponse resp = client().admin().cluster().prepareState().get();
        Iterator<RoutingNode> iter = resp.getState().getRoutingNodes().iterator();
        while (iter.hasNext()) {
            RoutingNode node = iter.next();
            realNodeNames.add(node.nodeId());
            logger.info("--> node {} has {} shards",
                    node.nodeId(), resp.getState().getRoutingNodes().node(node.nodeId()).numberOfOwningShards());
        }

        // Update the disk usages so one node has now passed the high watermark
        cis.setN1Usage(realNodeNames.get(0), new DiskUsage(nodes.get(0), "n1", "_na_", 100, 50));
        cis.setN2Usage(realNodeNames.get(1), new DiskUsage(nodes.get(1), "n2", "_na_", 100, 50));
        cis.setN3Usage(realNodeNames.get(2), new DiskUsage(nodes.get(2), "n3", "_na_", 100, 0)); // nothing free on node3

        // Retrieve the count of shards on each node
        final Map<String, Integer> nodesToShardCount = new HashMap<>();

        assertBusy(new Runnable() {
            @Override
            public void run() {
                ClusterStateResponse resp = client().admin().cluster().prepareState().get();
                Iterator<RoutingNode> iter = resp.getState().getRoutingNodes().iterator();
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
        });

        // Update the disk usages so one node is now back under the high watermark
        cis.setN1Usage(realNodeNames.get(0), new DiskUsage(nodes.get(0), "n1", "_na_", 100, 50));
        cis.setN2Usage(realNodeNames.get(1), new DiskUsage(nodes.get(1), "n2", "_na_", 100, 50));
        cis.setN3Usage(realNodeNames.get(2), new DiskUsage(nodes.get(2), "n3", "_na_", 100, 50)); // node3 has free space now

        // Retrieve the count of shards on each node
        nodesToShardCount.clear();

        assertBusy(new Runnable() {
            @Override
            public void run() {
                ClusterStateResponse resp = client().admin().cluster().prepareState().get();
                Iterator<RoutingNode> iter = resp.getState().getRoutingNodes().iterator();
                while (iter.hasNext()) {
                    RoutingNode node = iter.next();
                    logger.info("--> node {} has {} shards",
                            node.nodeId(), resp.getState().getRoutingNodes().node(node.nodeId()).numberOfOwningShards());
                    nodesToShardCount.put(node.nodeId(), resp.getState().getRoutingNodes().node(node.nodeId()).numberOfOwningShards());
                }
                assertThat("node1 has at least 3 shards", nodesToShardCount.get(realNodeNames.get(0)), greaterThanOrEqualTo(3));
                assertThat("node2 has at least 3 shards", nodesToShardCount.get(realNodeNames.get(1)), greaterThanOrEqualTo(3));
                assertThat("node3 has at least 3 shards", nodesToShardCount.get(realNodeNames.get(2)), greaterThanOrEqualTo(3));
            }
        });
    }
}
