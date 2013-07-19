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

import gnu.trove.map.hash.TObjectIntHashMap;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.junit.After;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class AwarenessAllocationTests extends AbstractNodesTests {

    private final ESLogger logger = Loggers.getLogger(AwarenessAllocationTests.class);

    @After
    public void cleanAndCloseNodes() throws Exception {
        closeAllNodes();
    }

    @Test
    public void testSimpleAwareness() throws Exception {
        Settings commonSettings = ImmutableSettings.settingsBuilder()
                .put("cluster.routing.schedule", "10ms")
                .put("cluster.routing.allocation.awareness.attributes", "rack_id")
                .build();


        logger.info("--> starting 2 nodes on the same rack");
        startNode("node1", ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "rack_1"));
        startNode("node2", ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "rack_1"));

        client("node1").admin().indices().prepareCreate("test1").execute().actionGet();
        client("node1").admin().indices().prepareCreate("test2").execute().actionGet();

        ClusterHealthResponse health = client("node1").admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        logger.info("--> starting 1 node on a different rack");
        startNode("node3", ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "rack_2"));

        long start = System.currentTimeMillis();
        TObjectIntHashMap<String> counts;
        // On slow machines the initial relocation might be delayed
        do {
            Thread.sleep(100);
            logger.info("--> waiting for no relocation");
            health = client("node1").admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("3").setWaitForRelocatingShards(0).execute().actionGet();
            assertThat(health.isTimedOut(), equalTo(false));

            logger.info("--> checking current state");
            ClusterState clusterState = client("node1").admin().cluster().prepareState().execute().actionGet().getState();
            //System.out.println(clusterState.routingTable().prettyPrint());
            // verify that we have 10 shards on node3
            counts = new TObjectIntHashMap<String>();
            for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
                for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                    for (ShardRouting shardRouting : indexShardRoutingTable) {
                        counts.adjustOrPutValue(clusterState.nodes().get(shardRouting.currentNodeId()).name(), 1, 1);
                    }
                }
            }
        } while (counts.get("node3") != 10 && (System.currentTimeMillis() - start) < 10000);
        assertThat(counts.get("node3"), equalTo(10));
    }
}
