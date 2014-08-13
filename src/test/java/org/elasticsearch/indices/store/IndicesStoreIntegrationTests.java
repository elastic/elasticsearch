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

package org.elasticsearch.indices.store;

import com.google.common.base.Predicate;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryService;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
@ClusterScope(scope= Scope.TEST, numDataNodes = 0)
public class IndicesStoreIntegrationTests extends ElasticsearchIntegrationTest {
    private static final Settings SETTINGS = settingsBuilder().put("gateway.type", "local").build();

    @Test
    @TestLogging("indices.store:TRACE,discovery.zen:TRACE,action:TRACE,cluster.service:TRACE,indices.recovery:TRACE,indices.cluster:TRACE")
    public void shardsCleanup() throws Exception {
        final String node_1 = internalCluster().startNode(SETTINGS);
        final String node_2 = internalCluster().startNode(SETTINGS);
        logger.info("--> creating index [test] with one shard and on replica");
        assertAcked(prepareCreate("test").setSettings(
                        ImmutableSettings.builder().put(indexSettings())
                                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1))
        );
        ensureGreen("test");

        logger.info("--> making sure that shard and its replica are allocated on node_1 and node_2");
        assertThat(shardDirectory(node_1, "test", 0).exists(), equalTo(true));
        assertThat(shardDirectory(node_2, "test", 0).exists(), equalTo(true));

        logger.info("--> starting node server3");
        String node_3 = internalCluster().startNode(SETTINGS);
        logger.info("--> running cluster_health");
        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth()
                .setWaitForNodes("3")
                .setWaitForRelocatingShards(0)
                .get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));

        logger.info("--> making sure that shard is not allocated on server3");
        assertThat(waitForShardDeletion(node_3, "test", 0), equalTo(false));

        File server2Shard = shardDirectory(node_2, "test", 0);
        logger.info("--> stopping node " + node_2);
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node_2));

        logger.info("--> running cluster_health");
        clusterHealth = client().admin().cluster().prepareHealth()
                .setWaitForGreenStatus()
                .setWaitForNodes("2")
                .setWaitForRelocatingShards(0)
                .get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());

        assertThat(server2Shard.exists(), equalTo(true));

        logger.info("--> making sure that shard and its replica exist on server1, server2 and server3");
        assertThat(shardDirectory(node_1, "test", 0).exists(), equalTo(true));
        assertThat(server2Shard.exists(), equalTo(true));
        assertThat(shardDirectory(node_3, "test", 0).exists(), equalTo(true));

        logger.info("--> starting node node_4");
        final String node_4 = internalCluster().startNode(SETTINGS);

        logger.info("--> running cluster_health");
        ensureGreen();

        logger.info("--> making sure that shard and its replica are allocated on server1 and server3 but not on server2");
        assertThat(shardDirectory(node_1, "test", 0).exists(), equalTo(true));
        assertThat(shardDirectory(node_3, "test", 0).exists(), equalTo(true));
        assertThat(waitForShardDeletion(node_4, "test", 0), equalTo(false));
    }

    @Test
    @TestLogging("indices.store:TRACE")
    public void testShardActiveElseWhere() throws Exception {
        String node_1 = internalCluster().startNode(SETTINGS);
        String node_2 = internalCluster().startNode(SETTINGS);
        final String node_1_id = internalCluster().getInstance(DiscoveryService.class, node_1).localNode().getId();
        final String node_2_id = internalCluster().getInstance(DiscoveryService.class, node_2).localNode().getId();

        final int numShards = scaledRandomIntBetween(2, 20);
        assertAcked(prepareCreate("test")
                        .setSettings(ImmutableSettings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0).put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numShards))
        );
        ensureGreen("test");

        ClusterStateResponse stateResponse = client().admin().cluster().prepareState().get();
        RoutingNode routingNode = stateResponse.getState().routingNodes().node(node_2_id);
        int[] node2Shards = new int[routingNode.numberOfOwningShards()];
        int i = 0;
        for (MutableShardRouting mutableShardRouting : routingNode) {
            node2Shards[i++] = mutableShardRouting.shardId().id();
        }
        logger.info("Node 2 has shards: {}", Arrays.toString(node2Shards));
        waitNoPendingTasksOnAll();
        internalCluster().getInstance(ClusterService.class, node_2).submitStateUpdateTask("test", Priority.IMMEDIATE, new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder("test");
                for (int i = 0; i < numShards; i++) {
                   indexRoutingTableBuilder.addIndexShard(
                           new IndexShardRoutingTable.Builder(new ShardId("test", i), false)
                                   .addShard(new ImmutableShardRouting("test", i, node_1_id, true, ShardRoutingState.STARTED, 1))
                                   .build()
                   );
                }
                return ClusterState.builder(currentState)
                        .routingTable(RoutingTable.builder().add(indexRoutingTableBuilder).build())
                        .build();
            }

            @Override
            public void onFailure(String source, Throwable t) {
            }
        });
        waitNoPendingTasksOnAll();
        logger.info("Checking if shards aren't removed");
        for (int shard : node2Shards) {
            assertTrue(waitForShardDeletion(node_2, "test", shard));
        }
    }

    private File shardDirectory(String server, String index, int shard) {
        NodeEnvironment env = internalCluster().getInstance(NodeEnvironment.class, server);
        return env.shardLocations(new ShardId(index, shard))[0];
    }

    private boolean waitForShardDeletion(final String server, final  String index, final int shard) throws InterruptedException {
        awaitBusy(new Predicate<Object>() {
            public boolean apply(Object o) {
                return !shardDirectory(server, index, shard).exists();
            }
        });
        return shardDirectory(server, index, shard).exists();
    }


}
