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

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastZenPing;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.NodeConfigurationSource;
import org.elasticsearch.test.TestCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Note, when talking to tribe client, no need to set the local flag on master read operations, it
 * does it by default.
 */
@LuceneTestCase.SuppressFileSystems("ExtrasFS") // doesn't work with potential multi data path from test cluster yet
public class TribeIT extends ESIntegTestCase {

    public static final String SECOND_CLUSTER_NODE_PREFIX = "node_tribe2";

    private static InternalTestCluster cluster2;

    private Node tribeNode;
    private Client tribeClient;

    @BeforeClass
    public static void setupSecondCluster() throws Exception {
        ESIntegTestCase.beforeClass();
        cluster2 = new InternalTestCluster(InternalTestCluster.configuredNodeMode(), randomLong(), createTempDir(), 2, 2,
                Strings.randomBase64UUID(getRandom()), NodeConfigurationSource.EMPTY, 0, false, SECOND_CLUSTER_NODE_PREFIX, true);

        cluster2.beforeTest(getRandom(), 0.1);
        cluster2.ensureAtLeastNumDataNodes(2);
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
    public void tearDownTribeNode() throws IOException {
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
        Map<String,String> asMap = internalCluster().getDefaultSettings().getAsMap();
        Settings.Builder tribe1Defaults = Settings.builder();
        Settings.Builder tribe2Defaults = Settings.builder();
        for (Map.Entry<String, String> entry : asMap.entrySet()) {
            tribe1Defaults.put("tribe.t1." + entry.getKey(), entry.getValue());
            tribe2Defaults.put("tribe.t2." + entry.getKey(), entry.getValue());
        }
        // give each tribe it's unicast hosts to connect to
        tribe1Defaults.putArray("tribe.t1." + UnicastZenPing.DISCOVERY_ZEN_PING_UNICAST_HOSTS, getUnicastHosts(internalCluster().client()));
        tribe1Defaults.putArray("tribe.t2." + UnicastZenPing.DISCOVERY_ZEN_PING_UNICAST_HOSTS, getUnicastHosts(cluster2.client()));

        Settings merged = Settings.builder()
                .put("tribe.t1.cluster.name", internalCluster().getClusterName())
                .put("tribe.t2.cluster.name", cluster2.getClusterName())
                .put("tribe.blocks.write", false)
                .put("tribe.blocks.read", false)
                .put(settings)
                .put(tribe1Defaults.build())
                .put(tribe2Defaults.build())
                .put(internalCluster().getDefaultSettings())
                .put("node.name", "tribe_node") // make sure we can identify threads from this node
                .build();

        tribeNode = NodeBuilder.nodeBuilder()
                .settings(merged)
                .node();
        tribeClient = tribeNode.client();
    }

    @Test
    public void testGlobalReadWriteBlocks() throws Exception {
        logger.info("create 2 indices, test1 on t1, and test2 on t2");
        internalCluster().client().admin().indices().prepareCreate("test1").get();
        assertAcked(cluster2.client().admin().indices().prepareCreate("test2"));


        setupTribeNode(Settings.builder()
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
        assertAcked(internalCluster().client().admin().indices().prepareCreate("test1"));
        assertAcked(internalCluster().client().admin().indices().prepareCreate("block_test1"));
        assertAcked(cluster2.client().admin().indices().prepareCreate("test2"));
        assertAcked(cluster2.client().admin().indices().prepareCreate("block_test2"));

        setupTribeNode(Settings.builder()
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

        setupTribeNode(Settings.builder()
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
        assertAcked(internalCluster().client().admin().indices().prepareCreate("conflict"));
        assertAcked(cluster2.client().admin().indices().prepareCreate("conflict"));
        assertAcked(internalCluster().client().admin().indices().prepareCreate("test1"));
        assertAcked(cluster2.client().admin().indices().prepareCreate("test2"));

        setupTribeNode(Settings.builder()
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
        setupTribeNode(Settings.EMPTY);
        logger.info("create 2 indices, test1 on t1, and test2 on t2");
        assertAcked(internalCluster().client().admin().indices().prepareCreate("test1"));
        assertAcked(cluster2.client().admin().indices().prepareCreate("test2"));


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
        assertBusy(new Runnable() {
            @Override
            public void run() {
                ClusterState tribeState = tribeNode.client().admin().cluster().prepareState().get().getState();
                assertThat(tribeState.getMetaData().index("test1").mapping("type1"), notNullValue());
                assertThat(tribeState.getMetaData().index("test2").mapping("type1"), notNullValue());
            }
        });


        logger.info("write to another type");
        tribeClient.prepareIndex("test1", "type2", "1").setSource("field1", "value1").get();
        tribeClient.prepareIndex("test2", "type2", "1").setSource("field1", "value1").get();
        assertNoFailures(tribeClient.admin().indices().prepareRefresh().get());


        logger.info("verify they are there");
        assertHitCount(tribeClient.prepareCount().get(), 4l);
        assertHitCount(tribeClient.prepareSearch().get(), 4l);
        assertBusy(new Runnable() {
            @Override
            public void run() {
                ClusterState tribeState = tribeNode.client().admin().cluster().prepareState().get().getState();
                assertThat(tribeState.getMetaData().index("test1").mapping("type1"), notNullValue());
                assertThat(tribeState.getMetaData().index("test1").mapping("type2"), notNullValue());
                assertThat(tribeState.getMetaData().index("test2").mapping("type1"), notNullValue());
                assertThat(tribeState.getMetaData().index("test2").mapping("type2"), notNullValue());
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
        awaitIndicesNotInClusterState("test2");

        try {
            logger.info("stop a node, make sure its reflected");
            cluster2.stopRandomDataNode();
            awaitSameNodeCounts();
        } finally {
            cluster2.startNode();
            awaitSameNodeCounts();
        }
    }

    @Test
    public void testCloseAndOpenIndex() throws Exception {
        //create an index and close it even before starting the tribe node
        assertAcked(internalCluster().client().admin().indices().prepareCreate("test1"));
        ensureGreen(internalCluster());
        assertAcked(internalCluster().client().admin().indices().prepareClose("test1"));

        setupTribeNode(Settings.EMPTY);
        awaitSameNodeCounts();

        //the closed index is not part of the tribe node cluster state
        ClusterState tribeState = tribeNode.client().admin().cluster().prepareState().get().getState();
        assertThat(tribeState.getMetaData().hasIndex("test1"), equalTo(false));

        //open the index, it becomes part of the tribe node cluster state
        assertAcked(internalCluster().client().admin().indices().prepareOpen("test1"));
        awaitIndicesInClusterState("test1");
        ensureGreen(internalCluster());

        //create a second index, wait till it is seen from within the tribe node
        assertAcked(cluster2.client().admin().indices().prepareCreate("test2"));
        awaitIndicesInClusterState("test1", "test2");
        ensureGreen(cluster2);

        //close the second index, wait till it gets removed from the tribe node cluster state
        assertAcked(cluster2.client().admin().indices().prepareClose("test2"));
        awaitIndicesNotInClusterState("test2");

        //open the second index, wait till it gets added back to the tribe node cluster state
        assertAcked(cluster2.client().admin().indices().prepareOpen("test2"));
        awaitIndicesInClusterState("test1", "test2");
        ensureGreen(cluster2);
    }

    private void awaitIndicesInClusterState(final String... indices) throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                ClusterState tribeState = tribeNode.client().admin().cluster().prepareState().get().getState();
                for (String index : indices) {
                    assertTrue(tribeState.getMetaData().hasIndex(index));
                    assertTrue(tribeState.getRoutingTable().hasIndex(index));
                }
            }
        });
    }

    private void awaitIndicesNotInClusterState(final String... indices) throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                ClusterState tribeState = tribeNode.client().admin().cluster().prepareState().get().getState();
                for (String index : indices) {
                    assertFalse(tribeState.getMetaData().hasIndex(index));
                    assertFalse(tribeState.getRoutingTable().hasIndex(index));
                }
            }
        });
    }

    private void ensureGreen(TestCluster testCluster) {
        ClusterHealthResponse actionGet = testCluster.client().admin().cluster()
                .health(Requests.clusterHealthRequest().waitForGreenStatus().waitForEvents(Priority.LANGUID).waitForRelocatingShards(0)).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info("ensureGreen timed out, cluster state:\n{}\n{}", testCluster.client().admin().cluster().prepareState().get().getState().prettyPrint(), testCluster.client().admin().cluster().preparePendingClusterTasks().get().prettyPrint());
            assertThat("timed out waiting for green state", actionGet.isTimedOut(), equalTo(false));
        }
        assertThat(actionGet.getStatus(), equalTo(ClusterHealthStatus.GREEN));
    }

    private void awaitSameNodeCounts() throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                DiscoveryNodes tribeNodes = tribeNode.client().admin().cluster().prepareState().get().getState().getNodes();
                assertThat(countDataNodesForTribe("t1", tribeNodes), equalTo(internalCluster().client().admin().cluster().prepareState().get().getState().getNodes().dataNodes().size()));
                assertThat(countDataNodesForTribe("t2", tribeNodes), equalTo(cluster2.client().admin().cluster().prepareState().get().getState().getNodes().dataNodes().size()));
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

    public String[] getUnicastHosts(Client client) {
        ArrayList<String> unicastHosts = new ArrayList<>();
        NodesInfoResponse nodeInfos = client.admin().cluster().prepareNodesInfo().clear().setTransport(true).get();
        for (NodeInfo info : nodeInfos.getNodes()) {
            TransportAddress address = info.getTransport().getAddress().publishAddress();
            unicastHosts.add(address.getAddress() + ":" + address.getPort());
        }
        return unicastHosts.toArray(new String[unicastHosts.size()]);
    }
}
