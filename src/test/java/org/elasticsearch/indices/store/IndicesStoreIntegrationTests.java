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
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryService;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.disruption.SlowClusterStateProcessing;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.Future;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class IndicesStoreIntegrationTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) { // simplify this and only use a single data path
        return ImmutableSettings.settingsBuilder().put(super.nodeSettings(nodeOrdinal)).put("path.data", "").build();
    }

    @Test
    public void indexCleanup() throws Exception {
        final String masterNode = internalCluster().startNode(ImmutableSettings.builder().put("node.data", false));
        final String node_1 = internalCluster().startNode(ImmutableSettings.builder().put("node.master", false));
        final String node_2 = internalCluster().startNode(ImmutableSettings.builder().put("node.master", false));
        logger.info("--> creating index [test] with one shard and on replica");
        assertAcked(prepareCreate("test").setSettings(
                        ImmutableSettings.builder().put(indexSettings())
                                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1))
        );
        ensureGreen("test");

        logger.info("--> making sure that shard and its replica are allocated on node_1 and node_2");
        assertThat(Files.exists(shardDirectory(node_1, "test", 0)), equalTo(true));
        assertThat(Files.exists(indexDirectory(node_1, "test")), equalTo(true));
        assertThat(Files.exists(shardDirectory(node_2, "test", 0)), equalTo(true));
        assertThat(Files.exists(indexDirectory(node_2, "test")), equalTo(true));

        logger.info("--> starting node server3");
        final String node_3 = internalCluster().startNode(ImmutableSettings.builder().put("node.master", false));
        logger.info("--> running cluster_health");
        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth()
                .setWaitForNodes("4")
                .setWaitForRelocatingShards(0)
                .get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));

        assertThat(Files.exists(shardDirectory(node_1, "test", 0)), equalTo(true));
        assertThat(Files.exists(indexDirectory(node_1, "test")), equalTo(true));
        assertThat(Files.exists(shardDirectory(node_2, "test", 0)), equalTo(true));
        assertThat(Files.exists(indexDirectory(node_2, "test")), equalTo(true));
        assertThat(Files.exists(shardDirectory(node_3, "test", 0)), equalTo(false));
        assertThat(Files.exists(indexDirectory(node_3, "test")), equalTo(false));

        logger.info("--> move shard from node_1 to node_3, and wait for relocation to finish");
        SlowClusterStateProcessing disruption = null;
        if (randomBoolean()) {
            disruption = new SlowClusterStateProcessing(node_3, getRandom(), 0, 0, 1000, 2000);
            internalCluster().setDisruptionScheme(disruption);
            disruption.startDisrupting();
        }
        internalCluster().client().admin().cluster().prepareReroute().add(new MoveAllocationCommand(new ShardId("test", 0), node_1, node_3)).get();
        clusterHealth = client().admin().cluster().prepareHealth()
                .setWaitForNodes("4")
                .setWaitForRelocatingShards(0)
                .get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));

        assertThat(waitForShardDeletion(node_1, "test", 0), equalTo(false));
        assertThat(waitForIndexDeletion(node_1, "test"), equalTo(false));
        assertThat(Files.exists(shardDirectory(node_2, "test", 0)), equalTo(true));
        assertThat(Files.exists(indexDirectory(node_2, "test")), equalTo(true));
        assertThat(Files.exists(shardDirectory(node_3, "test", 0)), equalTo(true));
        assertThat(Files.exists(indexDirectory(node_3, "test")), equalTo(true));
    }

    @Test
    public void shardsCleanup() throws Exception {
        final String node_1 = internalCluster().startNode();
        final String node_2 = internalCluster().startNode();
        logger.info("--> creating index [test] with one shard and on replica");
        assertAcked(prepareCreate("test").setSettings(
                        ImmutableSettings.builder().put(indexSettings())
                                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1))
        );
        ensureGreen("test");

        logger.info("--> making sure that shard and its replica are allocated on node_1 and node_2");
        assertThat(Files.exists(shardDirectory(node_1, "test", 0)), equalTo(true));
        assertThat(Files.exists(shardDirectory(node_2, "test", 0)), equalTo(true));

        logger.info("--> starting node server3");
        String node_3 = internalCluster().startNode();
        logger.info("--> running cluster_health");
        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth()
                .setWaitForNodes("3")
                .setWaitForRelocatingShards(0)
                .get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));

        logger.info("--> making sure that shard is not allocated on server3");
        assertThat(waitForShardDeletion(node_3, "test", 0), equalTo(false));

        Path server2Shard = shardDirectory(node_2, "test", 0);
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

        assertThat(Files.exists(server2Shard), equalTo(true));

        logger.info("--> making sure that shard and its replica exist on server1, server2 and server3");
        assertThat(Files.exists(shardDirectory(node_1, "test", 0)), equalTo(true));
        assertThat(Files.exists(server2Shard), equalTo(true));
        assertThat(Files.exists(shardDirectory(node_3, "test", 0)), equalTo(true));

        logger.info("--> starting node node_4");
        final String node_4 = internalCluster().startNode();

        logger.info("--> running cluster_health");
        ensureGreen();

        logger.info("--> making sure that shard and its replica are allocated on server1 and server3 but not on server2");
        assertThat(Files.exists(shardDirectory(node_1, "test", 0)), equalTo(true));
        assertThat(Files.exists(shardDirectory(node_3, "test", 0)), equalTo(true));
        assertThat(waitForShardDeletion(node_4, "test", 0), equalTo(false));
    }

    @Test @Slow
    public void testShardActiveElseWhere() throws Exception {
        boolean node1IsMasterEligible = randomBoolean();
        boolean node2IsMasterEligible = !node1IsMasterEligible || randomBoolean();
        Future<String> node_1_future = internalCluster().startNodeAsync(ImmutableSettings.builder().put("node.master", node1IsMasterEligible).build());
        Future<String> node_2_future = internalCluster().startNodeAsync(ImmutableSettings.builder().put("node.master", node2IsMasterEligible).build());
        final String node_1 = node_1_future.get();
        final String node_2 = node_2_future.get();
        final String node_1_id = internalCluster().getInstance(DiscoveryService.class, node_1).localNode().getId();
        final String node_2_id = internalCluster().getInstance(DiscoveryService.class, node_2).localNode().getId();

        logger.debug("node {} (node_1) is {}master eligible", node_1, node1IsMasterEligible ? "" : "not ");
        logger.debug("node {} (node_2) is {}master eligible", node_2, node2IsMasterEligible ? "" : "not ");
        logger.debug("node {} became master", internalCluster().getMasterName());
        final int numShards = scaledRandomIntBetween(2, 20);
        assertAcked(prepareCreate("test")
                        .setSettings(ImmutableSettings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0).put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numShards))
        );
        ensureGreen("test");

        waitNoPendingTasksOnAll();
        ClusterStateResponse stateResponse = client().admin().cluster().prepareState().get();

        RoutingNode routingNode = stateResponse.getState().routingNodes().node(node_2_id);
        final int[] node2Shards = new int[routingNode.numberOfOwningShards()];
        int i = 0;
        for (MutableShardRouting mutableShardRouting : routingNode) {
            node2Shards[i] = mutableShardRouting.shardId().id();
            i++;
        }
        logger.info("Node 2 has shards: {}", Arrays.toString(node2Shards));
        final long shardVersions[] = new long[numShards];
        final int shardIds[] = new int[numShards];
        i = 0;
        for (ShardRouting shardRouting : stateResponse.getState().getRoutingTable().allShards("test")) {
            shardVersions[i] = shardRouting.version();
            shardIds[i] = shardRouting.getId();
            i++;
        }
        internalCluster().getInstance(ClusterService.class, node_2).submitStateUpdateTask("test", Priority.IMMEDIATE, new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder("test");
                for (int i = 0; i < numShards; i++) {
                    indexRoutingTableBuilder.addIndexShard(
                            new IndexShardRoutingTable.Builder(new ShardId("test", i), false)
                                    .addShard(new ImmutableShardRouting("test", i, node_1_id, true, ShardRoutingState.STARTED, shardVersions[shardIds[i]]))
                                    .build()
                    );
                }
                return ClusterState.builder(currentState)
                        .routingTable(RoutingTable.builder().add(indexRoutingTableBuilder).build())
                        .build();
            }

            public boolean runOnlyOnMaster() {
                return false;
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

    private Path indexDirectory(String server, String index) {
        NodeEnvironment env = internalCluster().getInstance(NodeEnvironment.class, server);
        final Path[] paths = env.indexPaths(new Index(index));
        assert paths.length == 1;
        return paths[0];
    }

    private Path shardDirectory(String server, String index, int shard) {
        NodeEnvironment env = internalCluster().getInstance(NodeEnvironment.class, server);
        final Path[] paths = env.availableShardPaths(new ShardId(index, shard));
        assert paths.length == 1;
        return paths[0];
    }

    private boolean waitForShardDeletion(final String server, final String index, final int shard) throws InterruptedException {
        awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                return !Files.exists(shardDirectory(server, index, shard));
            }
        });
        return Files.exists(shardDirectory(server, index, shard));
    }

    private boolean waitForIndexDeletion(final String server, final String index) throws InterruptedException {
        awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                return !Files.exists(indexDirectory(server, index));
            }
        });
        return Files.exists(indexDirectory(server, index));
    }
}
