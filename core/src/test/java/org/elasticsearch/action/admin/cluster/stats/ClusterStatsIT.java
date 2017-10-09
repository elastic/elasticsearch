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

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class ClusterStatsIT extends ESIntegTestCase {

    private void assertCounts(ClusterStatsNodes.Counts counts, int total, Map<String, Integer> roles) {
        assertThat(counts.getTotal(), equalTo(total));
        assertThat(counts.getRoles(), equalTo(roles));
    }

    private void waitForNodes(int numNodes) {
        ClusterHealthResponse actionGet = client().admin().cluster()
                .health(Requests.clusterHealthRequest().waitForEvents(Priority.LANGUID).waitForNodes(Integer.toString(numNodes))).actionGet();
        assertThat(actionGet.isTimedOut(), is(false));
    }

    public void testNodeCounts() {
        int total = 1;
        internalCluster().startNode();
        Map<String, Integer> expectedCounts = new HashMap<>();
        expectedCounts.put(DiscoveryNode.Role.DATA.getRoleName(), 1);
        expectedCounts.put(DiscoveryNode.Role.MASTER.getRoleName(), 1);
        expectedCounts.put(DiscoveryNode.Role.INGEST.getRoleName(), 1);
        expectedCounts.put(ClusterStatsNodes.Counts.COORDINATING_ONLY, 0);
        int numNodes = randomIntBetween(1, 5);

        ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().get();
        assertCounts(response.getNodesStats().getCounts(), total, expectedCounts);

        for (int i = 0; i < numNodes; i++) {
            boolean isDataNode = randomBoolean();
            boolean isMasterNode = randomBoolean();
            boolean isIngestNode = randomBoolean();
            Settings settings = Settings.builder().put(Node.NODE_DATA_SETTING.getKey(), isDataNode)
                    .put(Node.NODE_MASTER_SETTING.getKey(), isMasterNode).put(Node.NODE_INGEST_SETTING.getKey(), isIngestNode)
                    .build();
            internalCluster().startNode(settings);
            total++;
            waitForNodes(total);

            if (isDataNode) {
                incrementCountForRole(DiscoveryNode.Role.DATA.getRoleName(), expectedCounts);
            }
            if (isMasterNode) {
                incrementCountForRole(DiscoveryNode.Role.MASTER.getRoleName(), expectedCounts);
            }
            if (isIngestNode) {
                incrementCountForRole(DiscoveryNode.Role.INGEST.getRoleName(), expectedCounts);
            }
            if (!isDataNode && !isMasterNode && !isIngestNode) {
                incrementCountForRole(ClusterStatsNodes.Counts.COORDINATING_ONLY, expectedCounts);
            }

            response = client().admin().cluster().prepareClusterStats().get();
            assertCounts(response.getNodesStats().getCounts(), total, expectedCounts);
        }
    }

    private static void incrementCountForRole(String role, Map<String, Integer> counts) {
        Integer count = counts.get(role);
        if (count == null) {
            counts.put(role, 1);
        } else {
            counts.put(role, ++count);
        }
    }

    private void assertShardStats(ClusterStatsIndices.ShardStats stats, int indices, int total, int primaries, double replicationFactor) {
        assertThat(stats.getIndices(), Matchers.equalTo(indices));
        assertThat(stats.getTotal(), Matchers.equalTo(total));
        assertThat(stats.getPrimaries(), Matchers.equalTo(primaries));
        assertThat(stats.getReplication(), Matchers.equalTo(replicationFactor));
    }

    public void testIndicesShardStats() throws ExecutionException, InterruptedException {
        internalCluster().startNode();
        ensureGreen();
        ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().get();
        assertThat(response.getStatus(), Matchers.equalTo(ClusterHealthStatus.GREEN));

        prepareCreate("test1").setSettings(Settings.builder().put("number_of_shards", 2).put("number_of_replicas", 1)).get();

        response = client().admin().cluster().prepareClusterStats().get();
        assertThat(response.getStatus(), Matchers.equalTo(ClusterHealthStatus.YELLOW));
        assertThat(response.indicesStats.getDocs().getCount(), Matchers.equalTo(0L));
        assertThat(response.indicesStats.getIndexCount(), Matchers.equalTo(1));
        assertShardStats(response.getIndicesStats().getShards(), 1, 2, 2, 0.0);

        // add another node, replicas should get assigned
        internalCluster().startNode();
        ensureGreen();
        index("test1", "type", "1", "f", "f");
        refresh(); // make the doc visible
        response = client().admin().cluster().prepareClusterStats().get();
        assertThat(response.getStatus(), Matchers.equalTo(ClusterHealthStatus.GREEN));
        assertThat(response.indicesStats.getDocs().getCount(), Matchers.equalTo(1L));
        assertShardStats(response.getIndicesStats().getShards(), 1, 4, 2, 1.0);

        prepareCreate("test2").setSettings(Settings.builder().put("number_of_shards", 3).put("number_of_replicas", 0)).get();
        ensureGreen();
        response = client().admin().cluster().prepareClusterStats().get();
        assertThat(response.getStatus(), Matchers.equalTo(ClusterHealthStatus.GREEN));
        assertThat(response.indicesStats.getIndexCount(), Matchers.equalTo(2));
        assertShardStats(response.getIndicesStats().getShards(), 2, 7, 5, 2.0 / 5);

        assertThat(response.getIndicesStats().getShards().getAvgIndexPrimaryShards(), Matchers.equalTo(2.5));
        assertThat(response.getIndicesStats().getShards().getMinIndexPrimaryShards(), Matchers.equalTo(2));
        assertThat(response.getIndicesStats().getShards().getMaxIndexPrimaryShards(), Matchers.equalTo(3));

        assertThat(response.getIndicesStats().getShards().getAvgIndexShards(), Matchers.equalTo(3.5));
        assertThat(response.getIndicesStats().getShards().getMinIndexShards(), Matchers.equalTo(3));
        assertThat(response.getIndicesStats().getShards().getMaxIndexShards(), Matchers.equalTo(4));

        assertThat(response.getIndicesStats().getShards().getAvgIndexReplication(), Matchers.equalTo(0.5));
        assertThat(response.getIndicesStats().getShards().getMinIndexReplication(), Matchers.equalTo(0.0));
        assertThat(response.getIndicesStats().getShards().getMaxIndexReplication(), Matchers.equalTo(1.0));

    }

    public void testValuesSmokeScreen() throws IOException, ExecutionException, InterruptedException {
        internalCluster().startNodes(randomIntBetween(1, 3));
        index("test1", "type", "1", "f", "f");

        ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().get();
        String msg = response.toString();
        assertThat(msg, response.getTimestamp(), Matchers.greaterThan(946681200000L)); // 1 Jan 2000
        assertThat(msg, response.indicesStats.getStore().getSizeInBytes(), Matchers.greaterThan(0L));

        assertThat(msg, response.nodesStats.getFs().getTotal().getBytes(), Matchers.greaterThan(0L));
        assertThat(msg, response.nodesStats.getJvm().getVersions().size(), Matchers.greaterThan(0));

        assertThat(msg, response.nodesStats.getVersions().size(), Matchers.greaterThan(0));
        assertThat(msg, response.nodesStats.getVersions().contains(Version.CURRENT), Matchers.equalTo(true));
        assertThat(msg, response.nodesStats.getPlugins().size(), Matchers.greaterThanOrEqualTo(0));

        assertThat(msg, response.nodesStats.getProcess().count, Matchers.greaterThan(0));
        // 0 happens when not supported on platform
        assertThat(msg, response.nodesStats.getProcess().getAvgOpenFileDescriptors(), Matchers.greaterThanOrEqualTo(0L));
        // these can be -1 if not supported on platform
        assertThat(msg, response.nodesStats.getProcess().getMinOpenFileDescriptors(), Matchers.greaterThanOrEqualTo(-1L));
        assertThat(msg, response.nodesStats.getProcess().getMaxOpenFileDescriptors(), Matchers.greaterThanOrEqualTo(-1L));

        NodesStatsResponse nodesStatsResponse = client().admin().cluster().prepareNodesStats().setOs(true).get();
        long total = 0;
        long free = 0;
        long used = 0;
        for (NodeStats nodeStats : nodesStatsResponse.getNodes()) {
            total += nodeStats.getOs().getMem().getTotal().getBytes();
            free += nodeStats.getOs().getMem().getFree().getBytes();
            used += nodeStats.getOs().getMem().getUsed().getBytes();
        }
        assertEquals(msg, free, response.nodesStats.getOs().getMem().getFree().getBytes());
        assertEquals(msg, total, response.nodesStats.getOs().getMem().getTotal().getBytes());
        assertEquals(msg, used, response.nodesStats.getOs().getMem().getUsed().getBytes());
        assertEquals(msg, OsStats.calculatePercentage(used, total), response.nodesStats.getOs().getMem().getUsedPercent());
        assertEquals(msg, OsStats.calculatePercentage(free, total), response.nodesStats.getOs().getMem().getFreePercent());
    }

    public void testAllocatedProcessors() throws Exception {
        // start one node with 7 processors.
        internalCluster().startNode(Settings.builder().put(EsExecutors.PROCESSORS_SETTING.getKey(), 7).build());
        waitForNodes(1);

        ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().get();
        assertThat(response.getNodesStats().getOs().getAllocatedProcessors(), equalTo(7));
    }

    public void testClusterStatusWhenStateNotRecovered() throws Exception {
        internalCluster().startMasterOnlyNode(Settings.builder().put("gateway.recover_after_nodes", 2).build());
        ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().get();
        assertThat(response.getStatus(), equalTo(ClusterHealthStatus.RED));

        if (randomBoolean()) {
            internalCluster().startMasterOnlyNode(Settings.EMPTY);
        } else {
            internalCluster().startDataOnlyNode(Settings.EMPTY);
        }
        // wait for the cluster status to settle
        ensureGreen();
        response = client().admin().cluster().prepareClusterStats().get();
        assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
    }
}
