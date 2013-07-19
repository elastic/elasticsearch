/*
 * Licensed to ElasticSearch and Shay Banon under one
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

package org.elasticsearch.test.integration.cluster.allocation;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.junit.After;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class FilteringAllocationTests extends AbstractNodesTests {

    private final ESLogger logger = Loggers.getLogger(FilteringAllocationTests.class);

    @After
    public void cleanAndCloseNodes() throws Exception {
        closeAllNodes();
    }

    @Test
    public void testDecommissionNodeNoReplicas() throws Exception {
        logger.info("--> starting 2 nodes");
        startNode("node1");
        startNode("node2");

        logger.info("--> creating an index with no replicas");
        client("node1").admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_replicas", 0))
                .execute().actionGet();

        ClusterHealthResponse clusterHealthResponse = client("node1").admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        logger.info("--> index some data");
        for (int i = 0; i < 100; i++) {
            client("node1").prepareIndex("test", "type", Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }
        client("node1").admin().indices().prepareRefresh().execute().actionGet();
        assertThat(client("node1").prepareCount().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(100l));

        logger.info("--> decommission the second node");
        client("node1").admin().cluster().prepareUpdateSettings()
                .setTransientSettings(settingsBuilder().put("cluster.routing.allocation.exclude._name", "node2"))
                .execute().actionGet();

        Thread.sleep(200);

        clusterHealthResponse = client("node1").admin().cluster().prepareHealth()
                .setWaitForGreenStatus()
                .setWaitForRelocatingShards(0)
                .execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        logger.info("--> verify all are allocated on node1 now");
        ClusterState clusterState = client("node1").admin().cluster().prepareState().execute().actionGet().getState();
        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    assertThat(clusterState.nodes().get(shardRouting.currentNodeId()).name(), equalTo("node1"));
                }
            }
        }

        client("node1").admin().indices().prepareRefresh().execute().actionGet();
        assertThat(client("node1").prepareCount().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(100l));
    }

    @Test
    public void testDisablingAllocationFiltering() throws Exception {
        logger.info("--> starting 2 nodes");
        startNode("node1");
        startNode("node2");

        logger.info("--> creating an index with no replicas");
        client("node1").admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_replicas", 0))
                .execute().actionGet();

        ClusterHealthResponse clusterHealthResponse = client("node1").admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        logger.info("--> index some data");
        for (int i = 0; i < 100; i++) {
            client("node1").prepareIndex("test", "type", Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }
        client("node1").admin().indices().prepareRefresh().execute().actionGet();
        assertThat(client("node1").prepareCount().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(100l));
        ClusterState clusterState = client("node1").admin().cluster().prepareState().execute().actionGet().getState();
        IndexRoutingTable indexRoutingTable = clusterState.routingTable().index("test");
        int numShardsOnNode1 = 0;
        for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
            for (ShardRouting shardRouting : indexShardRoutingTable) {
                if ("node1".equals(clusterState.nodes().get(shardRouting.currentNodeId()).name())) {
                    numShardsOnNode1++;
                }
            }
        }

        if (numShardsOnNode1 > ThrottlingAllocationDecider.DEFAULT_CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES) {
            client("node1").admin().cluster().prepareUpdateSettings()
            .setTransientSettings(settingsBuilder().put("cluster.routing.allocation.node_concurrent_recoveries", numShardsOnNode1)).execute().actionGet();
            // make sure we can recover all the nodes at once otherwise we might run into a state where one of the shards has not yet started relocating
            // but we already fired up the request to wait for 0 relocating shards. 
        }
        logger.info("--> remove index from the first node");
        client("node1").admin().indices().prepareUpdateSettings("test")
                .setSettings(settingsBuilder().put("index.routing.allocation.exclude._name", "node1"))
                .execute().actionGet();

        Thread.sleep(200);

        clusterHealthResponse = client("node1").admin().cluster().prepareHealth()
                .setWaitForGreenStatus()
                .setWaitForRelocatingShards(0)
                .execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        logger.info("--> verify all shards are allocated on node2 now");
        clusterState = client("node1").admin().cluster().prepareState().execute().actionGet().getState();
        indexRoutingTable = clusterState.routingTable().index("test");
        for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
            for (ShardRouting shardRouting : indexShardRoutingTable) {
                assertThat(clusterState.nodes().get(shardRouting.currentNodeId()).name(), equalTo("node2"));
            }
        }

        logger.info("--> disable allocation filtering ");
        client("node1").admin().indices().prepareUpdateSettings("test")
                .setSettings(settingsBuilder().put("index.routing.allocation.exclude._name", ""))
                .execute().actionGet();

        Thread.sleep(200);

        clusterHealthResponse = client("node1").admin().cluster().prepareHealth()
                .setWaitForGreenStatus()
                .setWaitForRelocatingShards(0)
                .execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        logger.info("--> verify that there are shards allocated on both nodes now");
        clusterState = client("node1").admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(clusterState.routingTable().index("test").numberOfNodesShardsAreAllocatedOn(), equalTo(2));
    }
}

