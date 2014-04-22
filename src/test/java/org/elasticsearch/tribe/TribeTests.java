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

package org.elasticsearch.tribe;

import com.google.common.base.Predicate;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.TestCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

/**
 * Note, when talking to tribe client, no need to set the local flag on master read operations, it
 * does it by default.
 */
public class TribeTests extends ElasticsearchIntegrationTest {

    private static TestCluster cluster2;

    private Node tribeNode;
    private Client tribeClient;

    @BeforeClass
    public static void setupSecondCluster() throws Exception {
        ElasticsearchIntegrationTest.beforeClass();
        // create another cluster
        cluster2 = new TestCluster(randomLong(), 2, 2, Strings.randomBase64UUID(getRandom()));
        cluster2.beforeTest(getRandom(), 0.1, ElasticsearchIntegrationTest.CURRENT_VERSION);
        cluster2.ensureAtLeastNumNodes(2);
    }

    @AfterClass
    public static void tearDownSecondCluster() {
        if (cluster2 != null) {
            try {
                cluster2.close();
            } finally {
                cluster2 = null;
            }
        }
    }

    @After
    public void tearDownTribeNode() {
        if (cluster2 != null) {
            try {
                cluster2.wipe();
            } finally {
                cluster2.afterTest();
            }
        }
        if (tribeNode != null) {
            tribeNode.close();
            tribeNode = null;
        }
    }

    private void setupTribeNode(Settings settings) {
        Settings merged = ImmutableSettings.builder()
                .put("tribe.t1.cluster.name", cluster().getClusterName())
                .put("tribe.t2.cluster.name", cluster2.getClusterName())
                .put("tribe.blocks.write", false)
                .put("tribe.blocks.read", false)
                .put(settings)
                .build();

        tribeNode = NodeBuilder.nodeBuilder()
                .settings(merged)
                .node();
        tribeClient = tribeNode.client();
    }

    @Test
    public void testGlobalReadWriteBlocks() throws Exception {
        logger.info("create 2 indices, test1 on t1, and test2 on t2");
        cluster().client().admin().indices().prepareCreate("test1").get();
        cluster2.client().admin().indices().prepareCreate("test2").get();


        setupTribeNode(ImmutableSettings.builder()
                .put("tribe.blocks.write", true)
                .put("tribe.blocks.metadata", true)
                .build());

        logger.info("wait till tribe has the same nodes as the 2 clusters");
        awaitSameNodeCounts();
        // wait till the tribe node connected to the cluster, by checking if the index exists in the cluster state
        logger.info("wait till test1 and test2 exists in the tribe node state");
        awaitIndicesInClusterState("test1", "test2");

        try {
            tribeClient.prepareIndex("test1", "type1", "1").setSource("field1", "value1").execute().actionGet();
            fail("cluster block should be thrown");
        } catch (ClusterBlockException e) {
            // all is well!
        }
        try {
            tribeClient.admin().indices().prepareOptimize("test1").execute().actionGet();
            fail("cluster block should be thrown");
        } catch (ClusterBlockException e) {
            // all is well!
        }
        try {
            tribeClient.admin().indices().prepareOptimize("test2").execute().actionGet();
            fail("cluster block should be thrown");
        } catch (ClusterBlockException e) {
            // all is well!
        }
    }

    @Test
    public void testIndexWriteBlocks() throws Exception {
        logger.info("create 2 indices, test1 on t1, and test2 on t2");
        cluster().client().admin().indices().prepareCreate("test1").get();
        cluster().client().admin().indices().prepareCreate("block_test1").get();
        cluster2.client().admin().indices().prepareCreate("test2").get();
        cluster2.client().admin().indices().prepareCreate("block_test2").get();

        setupTribeNode(ImmutableSettings.builder()
                .put("tribe.blocks.write.indices", "block_*")
                .build());
        logger.info("wait till tribe has the same nodes as the 2 clusters");
        awaitSameNodeCounts();
        // wait till the tribe node connected to the cluster, by checking if the index exists in the cluster state
        logger.info("wait till test1 and test2 exists in the tribe node state");
        awaitIndicesInClusterState("test1", "test2", "block_test1", "block_test2");

        tribeClient.prepareIndex("test1", "type1", "1").setSource("field1", "value1").get();
        try {
            tribeClient.prepareIndex("block_test1", "type1", "1").setSource("field1", "value1").get();
            fail("cluster block should be thrown");
        } catch (ClusterBlockException e) {
            // all is well!
        }

        tribeClient.prepareIndex("test2", "type1", "1").setSource("field1", "value1").get();
        try {
            tribeClient.prepareIndex("block_test2", "type1", "1").setSource("field1", "value1").get();
            fail("cluster block should be thrown");
        } catch (ClusterBlockException e) {
            // all is well!
        }
    }

    @Test
    public void testOnConflictDrop() throws Exception {
        logger.info("create 2 indices, test1 on t1, and test2 on t2");
        assertAcked(cluster().client().admin().indices().prepareCreate("conflict"));
        assertAcked(cluster2.client().admin().indices().prepareCreate("conflict"));
        assertAcked(cluster().client().admin().indices().prepareCreate("test1"));
        assertAcked(cluster2.client().admin().indices().prepareCreate("test2"));

        setupTribeNode(ImmutableSettings.builder()
                .put("tribe.on_conflict", "drop")
                .build());

        logger.info("wait till tribe has the same nodes as the 2 clusters");
        awaitSameNodeCounts();

        // wait till the tribe node connected to the cluster, by checking if the index exists in the cluster state
        logger.info("wait till test1 and test2 exists in the tribe node state");
        awaitIndicesInClusterState("test1", "test2");

        assertThat(tribeClient.admin().cluster().prepareState().get().getState().getMetaData().index("test1").getSettings().get(TribeService.TRIBE_NAME), equalTo("t1"));
        assertThat(tribeClient.admin().cluster().prepareState().get().getState().getMetaData().index("test2").getSettings().get(TribeService.TRIBE_NAME), equalTo("t2"));
        assertThat(tribeClient.admin().cluster().prepareState().get().getState().getMetaData().hasIndex("conflict"), equalTo(false));
    }

    @Test
    public void testOnConflictPrefer() throws Exception {
        testOnConflictPrefer(randomBoolean() ? "t1" : "t2");
    }

    private void testOnConflictPrefer(String tribe) throws Exception {
        logger.info("testing preference for tribe {}", tribe);

        logger.info("create 2 indices, test1 on t1, and test2 on t2");
        cluster().client().admin().indices().prepareCreate("conflict").get();
        cluster2.client().admin().indices().prepareCreate("conflict").get();
        cluster().client().admin().indices().prepareCreate("test1").get();
        cluster2.client().admin().indices().prepareCreate("test2").get();

        setupTribeNode(ImmutableSettings.builder()
                .put("tribe.on_conflict", "prefer_" + tribe)
                .build());
        logger.info("wait till tribe has the same nodes as the 2 clusters");
        awaitSameNodeCounts();
        // wait till the tribe node connected to the cluster, by checking if the index exists in the cluster state
        logger.info("wait till test1 and test2 exists in the tribe node state");
        awaitIndicesInClusterState("test1", "test2", "conflict");

        assertThat(tribeClient.admin().cluster().prepareState().get().getState().getMetaData().index("test1").getSettings().get(TribeService.TRIBE_NAME), equalTo("t1"));
        assertThat(tribeClient.admin().cluster().prepareState().get().getState().getMetaData().index("test2").getSettings().get(TribeService.TRIBE_NAME), equalTo("t2"));
        assertThat(tribeClient.admin().cluster().prepareState().get().getState().getMetaData().index("conflict").getSettings().get(TribeService.TRIBE_NAME), equalTo(tribe));
    }

    @Test
    public void testTribeOnOneCluster() throws Exception {
        setupTribeNode(ImmutableSettings.EMPTY);
        logger.info("create 2 indices, test1 on t1, and test2 on t2");
        cluster().client().admin().indices().prepareCreate("test1").get();
        cluster2.client().admin().indices().prepareCreate("test2").get();


        // wait till the tribe node connected to the cluster, by checking if the index exists in the cluster state
        logger.info("wait till test1 and test2 exists in the tribe node state");
        awaitIndicesInClusterState("test1", "test2");

        logger.info("wait till tribe has the same nodes as the 2 clusters");
        awaitSameNodeCounts();

        assertThat(tribeClient.admin().cluster().prepareHealth().setWaitForGreenStatus().get().getStatus(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("create 2 docs through the tribe node");
        tribeClient.prepareIndex("test1", "type1", "1").setSource("field1", "value1").get();
        tribeClient.prepareIndex("test2", "type1", "1").setSource("field1", "value1").get();
        tribeClient.admin().indices().prepareRefresh().get();

        logger.info("verify they are there");
        assertHitCount(tribeClient.prepareCount().get(), 2l);
        assertHitCount(tribeClient.prepareSearch().get(), 2l);
        awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                ClusterState tribeState = tribeNode.client().admin().cluster().prepareState().get().getState();
                return tribeState.getMetaData().index("test1").mapping("type1") != null &&
                        tribeState.getMetaData().index("test2").mapping("type2") != null;
            }
        });


        logger.info("write to another type");
        tribeClient.prepareIndex("test1", "type2", "1").setSource("field1", "value1").get();
        tribeClient.prepareIndex("test2", "type2", "1").setSource("field1", "value1").get();
        tribeClient.admin().indices().prepareRefresh().get();


        logger.info("verify they are there");
        assertHitCount(tribeClient.prepareCount().get(), 4l);
        assertHitCount(tribeClient.prepareSearch().get(), 4l);
        awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                ClusterState tribeState = tribeNode.client().admin().cluster().prepareState().get().getState();
                return tribeState.getMetaData().index("test1").mapping("type1") != null && tribeState.getMetaData().index("test1").mapping("type2") != null &&
                        tribeState.getMetaData().index("test2").mapping("type1") != null && tribeState.getMetaData().index("test2").mapping("type2") != null;
            }
        });

        logger.info("make sure master level write operations fail... (we don't really have a master)");
        try {
            tribeClient.admin().indices().prepareCreate("tribe_index").setMasterNodeTimeout("10ms").get();
            fail();
        } catch (MasterNotDiscoveredException e) {
            // all is well!
        }

        logger.info("delete an index, and make sure its reflected");
        cluster2.client().admin().indices().prepareDelete("test2").get();
        awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                ClusterState tribeState = tribeNode.client().admin().cluster().prepareState().get().getState();
                return tribeState.getMetaData().hasIndex("test1") && !tribeState.getMetaData().hasIndex("test2") &&
                        tribeState.getRoutingTable().hasIndex("test1") && !tribeState.getRoutingTable().hasIndex("test2");
            }
        });

        logger.info("stop a node, make sure its reflected");
        cluster2.stopRandomNode();
        awaitSameNodeCounts();
    }

    private void awaitIndicesInClusterState(final String... indices) throws Exception {
        awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                ClusterState tribeState = tribeNode.client().admin().cluster().prepareState().get().getState();
                for (String index : indices) {
                    if (!tribeState.getMetaData().hasIndex(index)) {
                        return false;
                    }
                    if (!tribeState.getRoutingTable().hasIndex(index)) {
                        return false;
                    }
                }
                return true;
            }
        });
    }

    private void awaitSameNodeCounts() throws Exception {
        awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                DiscoveryNodes tribeNodes = tribeNode.client().admin().cluster().prepareState().get().getState().getNodes();
                return countDataNodesForTribe("t1", tribeNodes) == cluster().client().admin().cluster().prepareState().get().getState().getNodes().dataNodes().size()
                        && countDataNodesForTribe("t2", tribeNodes) == cluster2.client().admin().cluster().prepareState().get().getState().getNodes().dataNodes().size();
            }
        });
    }

    private int countDataNodesForTribe(String tribeName, DiscoveryNodes nodes) {
        int count = 0;
        for (DiscoveryNode node : nodes) {
            if (!node.dataNode()) {
                continue;
            }
            if (tribeName.equals(node.getAttributes().get(TribeService.TRIBE_NAME))) {
                count++;
            }
        }
        return count;
    }
}
