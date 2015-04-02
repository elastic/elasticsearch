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

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.google.common.base.Predicate;
import org.apache.lucene.util.LuceneTestCase.Slow;
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
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

/**
 */
@ClusterScope(scope= ElasticsearchIntegrationTest.Scope.TEST, numDataNodes =0)
public class AwarenessAllocationTests extends ElasticsearchIntegrationTest {

    private final ESLogger logger = Loggers.getLogger(AwarenessAllocationTests.class);

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    @Test
    public void testSimpleAwareness() throws Exception {
        Settings commonSettings = ImmutableSettings.settingsBuilder()
                .put("cluster.routing.schedule", "10ms")
                .put("cluster.routing.allocation.awareness.attributes", "rack_id")
                .build();


        logger.info("--> starting 2 nodes on the same rack");
        internalCluster().startNodesAsync(2, ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "rack_1").build()).get();

        createIndex("test1");
        createIndex("test2");

        NumShards test1 = getNumShards("test1");
        NumShards test2 = getNumShards("test2");
        //no replicas will be allocated as both indices end up on a single node
        final int totalPrimaries = test1.numPrimaries + test2.numPrimaries;

        ensureGreen();

        logger.info("--> starting 1 node on a different rack");
        final String node3 = internalCluster().startNode(ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "rack_2").build());

        // On slow machines the initial relocation might be delayed
        assertThat(awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object input) {

                logger.info("--> waiting for no relocation");
                ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("3").setWaitForRelocatingShards(0).get();
                if (clusterHealth.isTimedOut()) {
                    return false;
                }

                logger.info("--> checking current state");
                ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
                // verify that we have all the primaries on node3
                ObjectIntOpenHashMap<String> counts = new ObjectIntOpenHashMap<>();
                for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
                    for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                        for (ShardRouting shardRouting : indexShardRoutingTable) {
                            counts.addTo(clusterState.nodes().get(shardRouting.currentNodeId()).name(), 1);
                        }
                    }
                }
                return counts.get(node3) == totalPrimaries;
            }
        }, 10, TimeUnit.SECONDS), equalTo(true));
    }
    
    @Test
    @Slow
    public void testAwarenessZones() throws Exception {
        Settings commonSettings = ImmutableSettings.settingsBuilder()
                .put("cluster.routing.allocation.awareness.force.zone.values", "a,b")
                .put("cluster.routing.allocation.awareness.attributes", "zone")
                .build();

        logger.info("--> starting 4 nodes on different zones");
        List<String> nodes = internalCluster().startNodesAsync(
                ImmutableSettings.settingsBuilder().put(commonSettings).put("node.zone", "a").put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES, 3).build(),
                ImmutableSettings.settingsBuilder().put(commonSettings).put("node.zone", "b").put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES, 3).build(),
                ImmutableSettings.settingsBuilder().put(commonSettings).put("node.zone", "b").put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES, 3).build(),
                ImmutableSettings.settingsBuilder().put(commonSettings).put("node.zone", "a").put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES, 3).build()
        ).get();
        String A_0 = nodes.get(0);
        String B_0 = nodes.get(1);
        String B_1 = nodes.get(2);
        String A_1 = nodes.get(3);
        client().admin().indices().prepareCreate("test")
        .setSettings(settingsBuilder().put("index.number_of_shards", 5)
                .put("index.number_of_replicas", 1)).execute().actionGet();
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("4").setWaitForRelocatingShards(0).execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));
        ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        ObjectIntOpenHashMap<String> counts = new ObjectIntOpenHashMap<>();

        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    counts.addTo(clusterState.nodes().get(shardRouting.currentNodeId()).name(), 1);
                }
            }
        }
        assertThat(counts.get(A_1), anyOf(equalTo(2),equalTo(3)));
        assertThat(counts.get(B_1), anyOf(equalTo(2),equalTo(3)));
        assertThat(counts.get(A_0), anyOf(equalTo(2),equalTo(3)));
        assertThat(counts.get(B_0), anyOf(equalTo(2),equalTo(3)));
    }
    
    @Test
    @Slow
    public void testAwarenessZonesIncrementalNodes() throws Exception {
        Settings commonSettings = ImmutableSettings.settingsBuilder()
                .put("cluster.routing.allocation.awareness.force.zone.values", "a,b")
                .put("cluster.routing.allocation.awareness.attributes", "zone")
                .build();

        logger.info("--> starting 2 nodes on zones 'a' & 'b'");
        List<String> nodes = internalCluster().startNodesAsync(
                ImmutableSettings.settingsBuilder().put(commonSettings).put("node.zone", "a").build(),
                ImmutableSettings.settingsBuilder().put(commonSettings).put("node.zone", "b").build()
        ).get();
        String A_0 = nodes.get(0);
        String B_0 = nodes.get(1);
        client().admin().indices().prepareCreate("test")
        .setSettings(settingsBuilder().put("index.number_of_shards", 5)
                .put("index.number_of_replicas", 1)).execute().actionGet();
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("2").setWaitForRelocatingShards(0).execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));
        ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        ObjectIntOpenHashMap<String> counts = new ObjectIntOpenHashMap<>();

        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    counts.addTo(clusterState.nodes().get(shardRouting.currentNodeId()).name(), 1);
                }
            }
        }
        assertThat(counts.get(A_0), equalTo(5));
        assertThat(counts.get(B_0), equalTo(5));
        logger.info("--> starting another node in zone 'b'");

        String B_1 = internalCluster().startNode(ImmutableSettings.settingsBuilder().put(commonSettings).put("node.zone", "b").build());
        health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("3").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));
        client().admin().cluster().prepareReroute().get();
        health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("3").setWaitForActiveShards(10).setWaitForRelocatingShards(0).execute().actionGet();

        assertThat(health.isTimedOut(), equalTo(false));
        clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();

        counts = new ObjectIntOpenHashMap<>();

        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    counts.addTo(clusterState.nodes().get(shardRouting.currentNodeId()).name(), 1);
                }
            }
        }
        assertThat(counts.get(A_0), equalTo(5));
        assertThat(counts.get(B_0), equalTo(3));
        assertThat(counts.get(B_1), equalTo(2));
        
        String noZoneNode = internalCluster().startNode();
        health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("4").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));
        client().admin().cluster().prepareReroute().get();
        health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("4").setWaitForActiveShards(10).setWaitForRelocatingShards(0).execute().actionGet();

        assertThat(health.isTimedOut(), equalTo(false));
        clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();

        counts = new ObjectIntOpenHashMap<>();

        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    counts.addTo(clusterState.nodes().get(shardRouting.currentNodeId()).name(), 1);
                }
            }
        }
        
        assertThat(counts.get(A_0), equalTo(5));
        assertThat(counts.get(B_0), equalTo(3));
        assertThat(counts.get(B_1), equalTo(2));
        assertThat(counts.containsKey(noZoneNode), equalTo(false));
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(ImmutableSettings.settingsBuilder().put("cluster.routing.allocation.awareness.attributes", "").build()).get();

        health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("4").setWaitForActiveShards(10).setWaitForRelocatingShards(0).execute().actionGet();

        assertThat(health.isTimedOut(), equalTo(false));
        clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();

        counts = new ObjectIntOpenHashMap<>();

        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    counts.addTo(clusterState.nodes().get(shardRouting.currentNodeId()).name(), 1);
                }
            }
        }
        
        assertThat(counts.get(A_0), equalTo(3));
        assertThat(counts.get(B_0), equalTo(3));
        assertThat(counts.get(B_1), equalTo(2));
        assertThat(counts.get(noZoneNode), equalTo(2));
    }
}
