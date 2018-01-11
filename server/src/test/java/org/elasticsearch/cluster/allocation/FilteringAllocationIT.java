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

package org.elasticsearch.cluster.allocation;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope= Scope.TEST, numDataNodes =0)
public class FilteringAllocationIT extends ESIntegTestCase {

    private final Logger logger = Loggers.getLogger(FilteringAllocationIT.class);

    public void testDecommissionNodeNoReplicas() throws Exception {
        logger.info("--> starting 2 nodes");
        List<String> nodesIds = internalCluster().startNodes(2);
        final String node_0 = nodesIds.get(0);
        final String node_1 = nodesIds.get(1);
        assertThat(cluster().size(), equalTo(2));

        logger.info("--> creating an index with no replicas");
        client().admin().indices().prepareCreate("test")
                .setSettings(Settings.builder().put("index.number_of_replicas", 0))
                .execute().actionGet();
        ensureGreen();
        logger.info("--> index some data");
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test", "type", Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }
        client().admin().indices().prepareRefresh().execute().actionGet();
        assertThat(client().prepareSearch().setSize(0).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().getTotalHits(), equalTo(100L));

        logger.info("--> decommission the second node");
        client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("cluster.routing.allocation.exclude._name", node_1))
                .execute().actionGet();
        waitForRelocation();

        logger.info("--> verify all are allocated on node1 now");
        ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    assertThat(clusterState.nodes().get(shardRouting.currentNodeId()).getName(), equalTo(node_0));
                }
            }
        }

        client().admin().indices().prepareRefresh().execute().actionGet();
        assertThat(client().prepareSearch().setSize(0).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().getTotalHits(), equalTo(100L));
    }

    public void testDisablingAllocationFiltering() throws Exception {
        logger.info("--> starting 2 nodes");
        List<String> nodesIds = internalCluster().startNodes(2);
        final String node_0 = nodesIds.get(0);
        final String node_1 = nodesIds.get(1);
        assertThat(cluster().size(), equalTo(2));

        logger.info("--> creating an index with no replicas");
        client().admin().indices().prepareCreate("test")
                .setSettings(Settings.builder().put("index.number_of_replicas", 0))
                .execute().actionGet();

        ensureGreen();

        logger.info("--> index some data");
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test", "type", Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }
        client().admin().indices().prepareRefresh().execute().actionGet();
        assertThat(client().prepareSearch().setSize(0).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().getTotalHits(), equalTo(100L));
        ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        IndexRoutingTable indexRoutingTable = clusterState.routingTable().index("test");
        int numShardsOnNode1 = 0;
        for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
            for (ShardRouting shardRouting : indexShardRoutingTable) {
                if ("node1".equals(clusterState.nodes().get(shardRouting.currentNodeId()).getName())) {
                    numShardsOnNode1++;
                }
            }
        }

        if (numShardsOnNode1 > ThrottlingAllocationDecider.DEFAULT_CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES) {
            client().admin().cluster().prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", numShardsOnNode1)).execute().actionGet();
            // make sure we can recover all the nodes at once otherwise we might run into a state where one of the shards has not yet started relocating
            // but we already fired up the request to wait for 0 relocating shards.
        }
        logger.info("--> remove index from the first node");
        client().admin().indices().prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.routing.allocation.exclude._name", node_0))
                .execute().actionGet();
        client().admin().cluster().prepareReroute().get();
        ensureGreen();

        logger.info("--> verify all shards are allocated on node_1 now");
        clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        indexRoutingTable = clusterState.routingTable().index("test");
        for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
            for (ShardRouting shardRouting : indexShardRoutingTable) {
                assertThat(clusterState.nodes().get(shardRouting.currentNodeId()).getName(), equalTo(node_1));
            }
        }

        logger.info("--> disable allocation filtering ");
        client().admin().indices().prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.routing.allocation.exclude._name", ""))
                .execute().actionGet();
        client().admin().cluster().prepareReroute().get();
        ensureGreen();

        logger.info("--> verify that there are shards allocated on both nodes now");
        clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(clusterState.routingTable().index("test").numberOfNodesShardsAreAllocatedOn(), equalTo(2));
    }

    public void testInvalidIPFilterClusterSettings() {
        String ipKey = randomFrom("_ip", "_host_ip", "_publish_ip");
        Setting<String> filterSetting = randomFrom(FilterAllocationDecider.CLUSTER_ROUTING_REQUIRE_GROUP_SETTING,
            FilterAllocationDecider.CLUSTER_ROUTING_INCLUDE_GROUP_SETTING, FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> client().admin().cluster().prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(filterSetting.getKey() + ipKey, "192.168.1.1."))
            .execute().actionGet());
        assertEquals("invalid IP address [192.168.1.1.] for [" + filterSetting.getKey() + ipKey + "]", e.getMessage());
    }
}

