package org.elasticsearch.action.admin.cluster.stats;
/*
 * Licensed to ElasticSearch under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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


import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.hamcrest.Matchers;
import org.junit.Test;

@ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numNodes = 0)
public class ClusterStatsTests extends ElasticsearchIntegrationTest {

    private void assertCounts(ClusterStatsNodes.Counts counts, int total, int masterOnly, int dataOnly, int masterData, int client) {
        assertThat(counts.total(), Matchers.equalTo(total));
        assertThat(counts.masterOnly(), Matchers.equalTo(masterOnly));
        assertThat(counts.dataOnly(), Matchers.equalTo(dataOnly));
        assertThat(counts.masterData(), Matchers.equalTo(masterData));
        assertThat(counts.client(), Matchers.equalTo(client));
    }

    @Test
    public void testNodeCounts() {
        cluster().startNode();
        ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().get();
        assertCounts(response.nodesStats().counts(), 1, 0, 0, 1, 0);

        cluster().startNode(ImmutableSettings.builder().put("node.data", false));
        response = client().admin().cluster().prepareClusterStats().get();
        assertCounts(response.nodesStats().counts(), 2, 1, 0, 1, 0);

        cluster().startNode(ImmutableSettings.builder().put("node.master", false));
        response = client().admin().cluster().prepareClusterStats().get();
        assertCounts(response.nodesStats().counts(), 3, 1, 1, 1, 0);

        cluster().startNode(ImmutableSettings.builder().put("node.client", true));
        response = client().admin().cluster().prepareClusterStats().get();
        assertCounts(response.nodesStats().counts(), 4, 1, 1, 1, 1);
    }


    private void assertShardStats(ClusterStatsIndices.ShardStats stats, int indices, int total, int primaries, double replicationFactor) {
        assertThat(stats.indices(), Matchers.equalTo(indices));
        assertThat(stats.total(), Matchers.equalTo(total));
        assertThat(stats.primaries(), Matchers.equalTo(primaries));
        assertThat(stats.replication(), Matchers.equalTo(replicationFactor));
    }

    @Test
    public void testIndicesShardStats() {
        cluster().startNode();
        prepareCreate("test1").setSettings("number_of_shards", 2, "number_of_replicas", 1).get();
        ensureYellow();
        ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().get();
        assertThat(response.indicesStats.docs().getCount(), Matchers.equalTo(0l));
        assertThat(response.indicesStats.indexCount(), Matchers.equalTo(1));
        assertShardStats(response.indicesStats().shards(), 1, 2, 2, 0.0);

        // add another node, replicas should get assigned
        cluster().startNode();
        ensureGreen();
        index("test1", "type", "1", "f", "f");
        refresh(); // make the doc visible
        response = client().admin().cluster().prepareClusterStats().get();
        assertThat(response.indicesStats.docs().getCount(), Matchers.equalTo(1l));
        assertShardStats(response.indicesStats().shards(), 1, 4, 2, 1.0);

        prepareCreate("test2").setSettings("number_of_shards", 3, "number_of_replicas", 0).get();
        ensureGreen();
        response = client().admin().cluster().prepareClusterStats().get();
        assertThat(response.indicesStats.indexCount(), Matchers.equalTo(2));
        assertShardStats(response.indicesStats().shards(), 2, 7, 5, 2.0 / 5);

        assertThat(response.indicesStats().shards().avgIndexPrimaryShards(), Matchers.equalTo(2.5));
        assertThat(response.indicesStats().shards().minIndexPrimaryShards(), Matchers.equalTo(2));
        assertThat(response.indicesStats().shards().maxIndexPrimaryShards(), Matchers.equalTo(3));

        assertThat(response.indicesStats().shards().avgIndexShards(), Matchers.equalTo(3.5));
        assertThat(response.indicesStats().shards().minIndexShards(), Matchers.equalTo(3));
        assertThat(response.indicesStats().shards().maxIndexShards(), Matchers.equalTo(4));

        assertThat(response.indicesStats().shards().avgIndexReplication(), Matchers.equalTo(0.5));
        assertThat(response.indicesStats().shards().minIndexReplication(), Matchers.equalTo(0.0));
        assertThat(response.indicesStats().shards().maxIndexReplication(), Matchers.equalTo(1.0));

    }

    @Test
    public void testValuesSmokeScreen() {
        cluster().ensureAtMostNumNodes(3);
        index("test1", "type", "1", "f", "f");

        ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().get();
        assertThat(response.indicesStats.store().getSizeInBytes(), Matchers.greaterThan(0l));

        assertThat(response.nodesStats.fs().getTotal().bytes(), Matchers.greaterThan(0l));
        assertThat(response.nodesStats.jvm().versions().size(), Matchers.greaterThan(0));
        assertThat(response.nodesStats.os().availableProcessors(), Matchers.greaterThan(0));
        assertThat(response.nodesStats.os().availableMemory().bytes(), Matchers.greaterThan(0l));
        assertThat(response.nodesStats.os().cpus().size(), Matchers.greaterThan(0));
        assertThat(response.nodesStats.versions().size(), Matchers.greaterThan(0));
        assertThat(response.nodesStats.plugins().size(), Matchers.greaterThanOrEqualTo(0));

    }
}
