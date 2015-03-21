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
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.monitor.sigar.SigarService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.is;

@ClusterScope(scope = Scope.SUITE, numDataNodes = 1, numClientNodes = 0)
public class ClusterStatsTests extends ElasticsearchIntegrationTest {

    private void assertCounts(ClusterStatsNodes.Counts counts, int total, int masterOnly, int dataOnly, int masterData, int client) {
        assertThat(counts.getTotal(), Matchers.equalTo(total));
        assertThat(counts.getMasterOnly(), Matchers.equalTo(masterOnly));
        assertThat(counts.getDataOnly(), Matchers.equalTo(dataOnly));
        assertThat(counts.getMasterData(), Matchers.equalTo(masterData));
        assertThat(counts.getClient(), Matchers.equalTo(client));
    }

    private void waitForNodes(int numNodes) {
        ClusterHealthResponse actionGet = client().admin().cluster()
                .health(Requests.clusterHealthRequest().waitForEvents(Priority.LANGUID).waitForNodes(Integer.toString(numNodes))).actionGet();
        assertThat(actionGet.isTimedOut(), is(false));
    }

    @Test
    public void testNodeCounts() {
        ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().get();
        assertCounts(response.getNodesStats().getCounts(), 1, 0, 0, 1, 0);

        internalCluster().startNode(ImmutableSettings.builder().put("node.data", false));
        waitForNodes(2);
        response = client().admin().cluster().prepareClusterStats().get();
        assertCounts(response.getNodesStats().getCounts(), 2, 1, 0, 1, 0);

        internalCluster().startNode(ImmutableSettings.builder().put("node.master", false));
        waitForNodes(3);
        response = client().admin().cluster().prepareClusterStats().get();
        assertCounts(response.getNodesStats().getCounts(), 3, 1, 1, 1, 0);

        internalCluster().startNode(ImmutableSettings.builder().put("node.client", true));
        waitForNodes(4);
        response = client().admin().cluster().prepareClusterStats().get();
        assertCounts(response.getNodesStats().getCounts(), 4, 1, 1, 1, 1);
    }


    private void assertShardStats(ClusterStatsIndices.ShardStats stats, int indices, int total, int primaries, double replicationFactor) {
        assertThat(stats.getIndices(), Matchers.equalTo(indices));
        assertThat(stats.getTotal(), Matchers.equalTo(total));
        assertThat(stats.getPrimaries(), Matchers.equalTo(primaries));
        assertThat(stats.getReplication(), Matchers.equalTo(replicationFactor));
    }

    @Test
    public void testIndicesShardStats() {
        ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().get();
        assertThat(response.getStatus(), Matchers.equalTo(ClusterHealthStatus.GREEN));


        prepareCreate("test1").setSettings("number_of_shards", 2, "number_of_replicas", 1).get();
        ensureYellow();
        response = client().admin().cluster().prepareClusterStats().get();
        assertThat(response.getStatus(), Matchers.equalTo(ClusterHealthStatus.YELLOW));
        assertThat(response.indicesStats.getDocs().getCount(), Matchers.equalTo(0l));
        assertThat(response.indicesStats.getIndexCount(), Matchers.equalTo(1));
        assertShardStats(response.getIndicesStats().getShards(), 1, 2, 2, 0.0);

        // add another node, replicas should get assigned
        internalCluster().startNode();
        ensureGreen();
        index("test1", "type", "1", "f", "f");
        refresh(); // make the doc visible
        response = client().admin().cluster().prepareClusterStats().get();
        assertThat(response.getStatus(), Matchers.equalTo(ClusterHealthStatus.GREEN));
        assertThat(response.indicesStats.getDocs().getCount(), Matchers.equalTo(1l));
        assertShardStats(response.getIndicesStats().getShards(), 1, 4, 2, 1.0);

        prepareCreate("test2").setSettings("number_of_shards", 3, "number_of_replicas", 0).get();
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

    @Test
    public void testValuesSmokeScreen() throws IOException {
        internalCluster().ensureAtMostNumDataNodes(5);
        internalCluster().ensureAtLeastNumDataNodes(1);
        SigarService sigarService = internalCluster().getInstance(SigarService.class);
        assertAcked(prepareCreate("test1").setSettings(settingsBuilder().put(Store.INDEX_STORE_STATS_REFRESH_INTERVAL, 0).build()));
        index("test1", "type", "1", "f", "f");
        /*
         * Ensure at least one shard is allocated otherwise the FS stats might
         * return 0. This happens if the File#getTotalSpace() and friends is called
         * on a directory that doesn't exist or has not yet been created.
         */
        ensureYellow("test1");
        ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().get();
        String msg = response.toString();
        assertThat(msg, response.getTimestamp(), Matchers.greaterThan(946681200000l)); // 1 Jan 2000
        assertThat(msg, response.indicesStats.getStore().getSizeInBytes(), Matchers.greaterThan(0l));

        assertThat(msg, response.nodesStats.getFs().getTotal().bytes(), Matchers.greaterThan(0l));
        assertThat(msg, response.nodesStats.getJvm().getVersions().size(), Matchers.greaterThan(0));
        if (sigarService.sigarAvailable()) {
            // We only get those if we have sigar
            assertThat(msg, response.nodesStats.getOs().getAvailableProcessors(), Matchers.greaterThan(0));
            assertThat(msg, response.nodesStats.getOs().getAvailableMemory().bytes(), Matchers.greaterThan(0l));
            assertThat(msg, response.nodesStats.getOs().getCpus().size(), Matchers.greaterThan(0));
        }
        assertThat(msg, response.nodesStats.getVersions().size(), Matchers.greaterThan(0));
        assertThat(msg, response.nodesStats.getVersions().contains(Version.CURRENT), Matchers.equalTo(true));
        assertThat(msg, response.nodesStats.getPlugins().size(), Matchers.greaterThanOrEqualTo(0));

        assertThat(msg, response.nodesStats.getProcess().count, Matchers.greaterThan(0));
        // 0 happens when not supported on platform
        assertThat(msg, response.nodesStats.getProcess().getAvgOpenFileDescriptors(), Matchers.greaterThanOrEqualTo(0L));
        // these can be -1 if not supported on platform
        assertThat(msg, response.nodesStats.getProcess().getMinOpenFileDescriptors(), Matchers.greaterThanOrEqualTo(-1L));
        assertThat(msg, response.nodesStats.getProcess().getMaxOpenFileDescriptors(), Matchers.greaterThanOrEqualTo(-1L));

    }
}
