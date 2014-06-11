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

package org.elasticsearch.discovery;

import com.google.common.base.Predicate;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportModule;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.*;

/**
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class DiscoveryWithNetworkFailuresTests extends ElasticsearchIntegrationTest {

    private static final Settings nodeSettings = ImmutableSettings.settingsBuilder()
            .put("discovery.type", "zen") // <-- To override the local setting if set externally
            .put("discovery.zen.fd.ping_timeout", "1s") // <-- for hitting simulated network failures quickly
            .put("discovery.zen.fd.ping_retries", "1") // <-- for hitting simulated network failures quickly
            .put(DiscoverySettings.PUBLISH_TIMEOUT, "1s") // <-- for hitting simulated network failures quickly
            .put("discovery.zen.minimum_master_nodes", 2)
            .put(TransportModule.TRANSPORT_SERVICE_TYPE_KEY, MockTransportService.class.getName())
            .build();

    @Override
    protected int numberOfShards() {
        return 3;
    }

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    @Override
    public Settings indexSettings() {
        Settings settings = super.indexSettings();
        return ImmutableSettings.builder()
                .put(settings)
                .put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE, 2)
                .build();
    }

    @Test
    @TestLogging("discovery.zen:TRACE")
    public void failWithMinimumMasterNodesConfigured() throws Exception {

        List<String> nodes = internalCluster().startNodesAsync(3, nodeSettings).get();

        // Wait until 3 nodes are part of the cluster
        ensureStableCluster(3);

        // Figure out what is the elected master node
        DiscoveryNode masterDiscoNode = findMasterNode(nodes);
        logger.info("---> legit elected master node=" + masterDiscoNode);
        final Client masterClient = internalCluster().masterClient();

        // Everything is stable now, it is now time to simulate evil...

        // Pick a node that isn't the elected master.
        String unluckyNode = null;
        for (String node : nodes) {
            if (!node.equals(masterDiscoNode.getName())) {
                unluckyNode = node;
            }
        }
        assert unluckyNode != null;

        // Simulate a network issue between the unlucky node and elected master node in both directions.
        addFailToSendNoConnectRule(masterDiscoNode.getName(), unluckyNode);
        addFailToSendNoConnectRule(unluckyNode, masterDiscoNode.getName());
        try {
            // Wait until elected master has removed that the unlucky node...
            boolean applied = awaitBusy(new Predicate<Object>() {
                @Override
                public boolean apply(Object input) {
                    return masterClient.admin().cluster().prepareState().setLocal(true).get().getState().nodes().size() == 2;
                }
            }, 1, TimeUnit.MINUTES);
            assertThat(applied, is(true));

            // The unlucky node must report *no* master node, since it can't connect to master and in fact it should
            // continuously ping until network failures have been resolved. However
            final Client isolatedNodeClient = internalCluster().client(unluckyNode);
            // It may a take a bit before the node detects it has been cut off from the elected master
            applied = awaitBusy(new Predicate<Object>() {
                @Override
                public boolean apply(Object input) {
                    ClusterState localClusterState = isolatedNodeClient.admin().cluster().prepareState().setLocal(true).get().getState();
                    DiscoveryNodes localDiscoveryNodes = localClusterState.nodes();
                    logger.info("localDiscoveryNodes=" + localDiscoveryNodes.prettyPrint());
                    return localDiscoveryNodes.masterNode() == null;
                }
            }, 10, TimeUnit.SECONDS);
            assertThat(applied, is(true));
        } finally {
            // stop simulating network failures, from this point on the unlucky node is able to rejoin
            // We also need to do this even if assertions fail, since otherwise the test framework can't work properly
            clearNoConnectRule(masterDiscoNode.getName(), unluckyNode);
            clearNoConnectRule(unluckyNode, masterDiscoNode.getName());
        }

        // Wait until the master node sees all 3 nodes again.
        ensureStableCluster(3);

        for (String node : nodes) {
            ClusterState state = internalCluster().client(node).admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
            assertThat(state.nodes().size(), equalTo(3));
            // The elected master shouldn't have changed, since the unlucky node never could have elected himself as
            // master since m_m_n of 2 could never be satisfied.
            assertThat(state.nodes().masterNode(), equalTo(masterDiscoNode));
        }
    }

    @Test
    @TestLogging("discovery.zen:TRACE,action:TRACE,cluster.service:TRACE,indices.recovery:TRACE,indices.cluster:TRACE")
    public void testDataConsistency() throws Exception {
        List<String> nodes = internalCluster().startNodesAsync(3, nodeSettings).get();

        // Wait until a 3 nodes are part of the cluster
        ensureStableCluster(3);

        assertAcked(prepareCreate("test")
                .addMapping("type", "field", "type=long")
                .get());

        IndexRequestBuilder[] indexRequests = new IndexRequestBuilder[scaledRandomIntBetween(1, 1000)];
        for (int i = 0; i < indexRequests.length; i++) {
            indexRequests[i] = client().prepareIndex("test", "type", String.valueOf(i)).setSource("field", i);
        }
        indexRandom(true, indexRequests);


        for (int i = 0; i < indexRequests.length; i++) {
            GetResponse getResponse = client().prepareGet("test", "type", String.valueOf(i)).get();
            assertThat(getResponse.isExists(), is(true));
            assertThat(getResponse.getVersion(), equalTo(1l));
            assertThat(getResponse.getId(), equalTo(String.valueOf(i)));
        }
        SearchResponse searchResponse = client().prepareSearch("test").setTypes("type")
                .addSort("field", SortOrder.ASC)
                .get();
        assertHitCount(searchResponse, indexRequests.length);
        for (int i = 0; i < searchResponse.getHits().getHits().length; i++) {
            SearchHit searchHit = searchResponse.getHits().getAt(i);
            assertThat(searchHit.id(), equalTo(String.valueOf(i)));
            assertThat((long) searchHit.sortValues()[0], equalTo((long) i));
        }

        // Everything is stable now, it is now time to simulate evil...
        // but first make sure we have no initializing shards and all is green
        // (waiting for green here, because indexing / search in a yellow index is fine as long as no other nodes go down)
        ensureGreen("test");

        // Pick a node that isn't the elected master.
        final String isolatedNode = nodes.get(0);
        final String nonIsolatedNode = nodes.get(1);

        // Simulate a network issue between the unlucky node and the rest of the cluster.
        randomIsolateNode(isolatedNode, nodes);
        try {
            logger.info("wait until elected master has removed [{}]", isolatedNode);
            boolean applied = awaitBusy(new Predicate<Object>() {
                @Override
                public boolean apply(Object input) {
                    return client(nonIsolatedNode).admin().cluster().prepareState().setLocal(true).get().getState().nodes().size() == 2;
                }
            }, 1, TimeUnit.MINUTES);
            assertThat(applied, is(true));

            // The unlucky node must report *no* master node, since it can't connect to master and in fact it should
            // continuously ping until network failures have been resolved. However
            // It may a take a bit before the node detects it has been cut off from the elected master
            logger.info("waiting for isolated node [{}] to have no master", isolatedNode);
            applied = awaitBusy(new Predicate<Object>() {
                @Override
                public boolean apply(Object input) {
                    ClusterState localClusterState = client(isolatedNode).admin().cluster().prepareState().setLocal(true).get().getState();
                    DiscoveryNodes localDiscoveryNodes = localClusterState.nodes();
                    logger.info("localDiscoveryNodes=" + localDiscoveryNodes.prettyPrint());
                    return localDiscoveryNodes.masterNode() == null;
                }
            }, 10, TimeUnit.SECONDS);
            assertThat(applied, is(true));

            ClusterHealthResponse healthResponse = client(nonIsolatedNode).admin().cluster().prepareHealth("test")
                    .setWaitForYellowStatus().get();
            assertThat(healthResponse.isTimedOut(), is(false));
            assertThat(healthResponse.getStatus(), equalTo(ClusterHealthStatus.YELLOW));

            // Reads on the right side of the split must work
            logger.info("verifying healthy part of cluster returns data");
            searchResponse = client(nonIsolatedNode).prepareSearch("test").setTypes("type")
                    .addSort("field", SortOrder.ASC)
                    .get();
            assertHitCount(searchResponse, indexRequests.length);
            for (int i = 0; i < searchResponse.getHits().getHits().length; i++) {
                SearchHit searchHit = searchResponse.getHits().getAt(i);
                assertThat(searchHit.id(), equalTo(String.valueOf(i)));
                assertThat((long) searchHit.sortValues()[0], equalTo((long) i));
            }

            // Reads on the wrong side of the split are partial
            logger.info("verifying isolated node [{}] returns partial data", isolatedNode);
            searchResponse = client(isolatedNode).prepareSearch("test").setTypes("type")
                    .addSort("field", SortOrder.ASC).setPreference("_only_local")
                    .get();
            assertThat(searchResponse.getSuccessfulShards(), lessThan(searchResponse.getTotalShards()));
            assertThat(searchResponse.getHits().totalHits(), lessThan((long) indexRequests.length));

            logger.info("verifying writes on healthy cluster");
            UpdateResponse updateResponse = client(nonIsolatedNode).prepareUpdate("test", "type", "0").setDoc("field2", 2).get();
            assertThat(updateResponse.getVersion(), equalTo(2l));

            try {
                logger.info("verifying writes on isolated [{}] fail", isolatedNode);
                client(isolatedNode).prepareUpdate("test", "type", "0").setDoc("field2", 2)
                        .setTimeout("1s") // Fail quick, otherwise we wait 60 seconds.
                        .get();
                fail();
            } catch (ClusterBlockException exception) {
                assertThat(exception.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
                assertThat(exception.blocks().size(), equalTo(1));
                ClusterBlock clusterBlock = exception.blocks().iterator().next();
                assertThat(clusterBlock.id(), equalTo(DiscoverySettings.NO_MASTER_BLOCK_ID));
            }
        } finally {
            // stop simulating network failures, from this point on the unlucky node is able to rejoin
            // We also need to do this even if assertions fail, since otherwise the test framework can't work properly
            restoreIsolation(isolatedNode, nodes);
        }

        // Wait until the master node sees all 3 nodes again.
        ensureStableCluster(3);

        logger.info("verifying all nodes return all data");
        for (Client client : clients()) {
            searchResponse = client.prepareSearch("test").setTypes("type")
                    .addSort("field", SortOrder.ASC)
                    .get();
            for (int i = 0; i < searchResponse.getHits().getHits().length; i++) {
                SearchHit searchHit = searchResponse.getHits().getAt(i);
                assertThat(searchHit.id(), equalTo(String.valueOf(i)));
                assertThat((long) searchHit.sortValues()[0], equalTo((long) i));
            }

            GetResponse getResponse = client.prepareGet("test", "type", "0").setPreference("_local").get();
            assertThat(getResponse.isExists(), is(true));
            assertThat(getResponse.getId(), equalTo("0"));
            assertThat(getResponse.getVersion(), equalTo(2l));
            for (int i = 1; i < indexRequests.length; i++) {
                getResponse = client.prepareGet("test", "type", String.valueOf(i)).setPreference("_local").get();
                assertThat(getResponse.isExists(), is(true));
                assertThat(getResponse.getVersion(), equalTo(1l));
                assertThat(getResponse.getId(), equalTo(String.valueOf(i)));
            }
        }
    }


    @Test
    @TestLogging("discovery.zen:TRACE,action:TRACE,cluster.service:TRACE,indices.recovery:TRACE,indices.cluster:TRACE")
    public void voidIsolateMasterAndVerifyClusterStateConsensus() throws Exception {
        final List<String> nodes = internalCluster().startNodesAsync(3, nodeSettings).get();
        ensureStableCluster(3);

        assertAcked(prepareCreate("test")
                .setSettings(ImmutableSettings.builder()
                                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1 + randomInt(2))
                                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, randomInt(2))
                ));

        ensureGreen();
        String isolatedNode = findMasterNode(nodes).name();
        String nonIsolatedNode = null;
        for (String node : nodes) {
            if (!node.equals(isolatedNode)) {
                nonIsolatedNode = node;
                break;
            }
        }
        randomIsolateNode(isolatedNode, nodes);

        // make sure cluster reforms
        ensureStableCluster(2, nonIsolatedNode);

        // restore isolation
        restoreIsolation(isolatedNode, nodes);

        ensureStableCluster(3);

        logger.info("issue a reroute");
        // trigger a reroute now, instead of waiting for the background reroute of RerouteService
        assertAcked(client().admin().cluster().prepareReroute());
        // and wait for it to finish.
        assertFalse(client().admin().cluster().prepareHealth().setWaitForRelocatingShards(0).get().isTimedOut());


        // verify all cluster states are the same
        ClusterState state = null;
        for (String node : nodes) {
            ClusterState nodeState = client(node).admin().cluster().prepareState().setLocal(true).get().getState();
            if (state == null) {
                state = nodeState;
                continue;
            }
            // assert nodes are identical
            try {
                assertEquals("unequal versions", state.version(), nodeState.version());
                assertEquals("unequal node count", state.nodes().size(), nodeState.nodes().size());
                assertEquals("different masters ", state.nodes().masterNodeId(), nodeState.nodes().masterNodeId());
                assertEquals("different meta data version", state.metaData().version(), nodeState.metaData().version());
                if (!state.routingTable().prettyPrint().equals(nodeState.routingTable().prettyPrint())) {
                    fail("different routing");
                }
            } catch (AssertionError t) {
                fail("failed comparing cluster state: " + t.getMessage() + "\n" +
                        "--- cluster state of node [" + nodes.get(0) + "]: ---\n" + state.prettyPrint() +
                        "\n--- cluster state [" + node + "]: ---\n" + nodeState.prettyPrint());
            }

        }

    }


    @Test
    @TestLogging("discovery.zen:TRACE,action:TRACE,cluster.service:TRACE,indices.recovery:TRACE,indices.cluster:TRACE")
    public void testRejoinDocumentExistsInAllShardCopies() throws Exception {
        List<String> nodes = internalCluster().startNodesAsync(3, nodeSettings).get();
        ensureStableCluster(3);

        assertAcked(prepareCreate("test")
                .setSettings(ImmutableSettings.builder()
                                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 2)
                )
                .get());
        ensureGreen("test");

        nodes = new ArrayList<>(nodes);
        Collections.shuffle(nodes, getRandom());
        String isolatedNode = nodes.get(0);
        String notIsolatedNode = nodes.get(1);

        randomIsolateNode(isolatedNode, nodes);
        ensureStableCluster(2, notIsolatedNode);
        assertFalse(client(notIsolatedNode).admin().cluster().prepareHealth("test").setWaitForYellowStatus().get().isTimedOut());


        IndexResponse indexResponse = internalCluster().client(notIsolatedNode).prepareIndex("test", "type").setSource("field", "value").get();
        assertThat(indexResponse.getVersion(), equalTo(1l));

        logger.info("Verifying if document exists via node[" + notIsolatedNode + "]");
        GetResponse getResponse = internalCluster().client(notIsolatedNode).prepareGet("test", "type", indexResponse.getId())
                .setPreference("_local")
                .get();
        assertThat(getResponse.isExists(), is(true));
        assertThat(getResponse.getVersion(), equalTo(1l));
        assertThat(getResponse.getId(), equalTo(indexResponse.getId()));

        restoreIsolation(isolatedNode, nodes);

        ensureStableCluster(3);
        ensureGreen("test");

        for (String node : nodes) {
            logger.info("Verifying if document exists after isolating node[" + isolatedNode + "] via node[" + node + "]");
            getResponse = internalCluster().client(node).prepareGet("test", "type", indexResponse.getId())
                    .setPreference("_local")
                    .get();
            assertThat(getResponse.isExists(), is(true));
            assertThat(getResponse.getVersion(), equalTo(1l));
            assertThat(getResponse.getId(), equalTo(indexResponse.getId()));
        }
    }

    protected void restoreIsolation(String isolatedNode, List<String> nodes) {
        logger.info("restoring isolation of [{}]", isolatedNode);
        for (String nodeId : nodes) {
            if (!nodeId.equals(isolatedNode)) {
                clearNoConnectRule(nodeId, isolatedNode);
                clearNoConnectRule(isolatedNode, nodeId);
            }
        }
    }

    protected void randomIsolateNode(String isolatedNode, List<String> nodes) {
        boolean unresponsive = randomBoolean();
        logger.info("isolating [{}] with unresponsive: [{}]", isolatedNode, unresponsive);
        for (String nodeId : nodes) {
            if (!nodeId.equals(isolatedNode)) {
                if (unresponsive) {
                    addUnresponsiveRule(nodeId, isolatedNode);
                    addUnresponsiveRule(isolatedNode, nodeId);
                } else {
                    addFailToSendNoConnectRule(nodeId, isolatedNode);
                    addFailToSendNoConnectRule(isolatedNode, nodeId);
                }
            }
        }
    }

    private DiscoveryNode findMasterNode(List<String> nodes) {
        DiscoveryNode masterDiscoNode = null;
        for (String node : nodes) {
            ClusterState state = internalCluster().client(node).admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
            assertThat(state.nodes().size(), equalTo(3));
            if (masterDiscoNode == null) {
                masterDiscoNode = state.nodes().masterNode();
            } else {
                assertThat(state.nodes().masterNode(), equalTo(masterDiscoNode));
            }
        }
        assert masterDiscoNode != null;
        return masterDiscoNode;
    }

    private void addFailToSendNoConnectRule(String fromNode, String toNode) {
        TransportService mockTransportService = internalCluster().getInstance(TransportService.class, fromNode);
        ((MockTransportService) mockTransportService).addFailToSendNoConnectRule(internalCluster().getInstance(Discovery.class, toNode).localNode());
    }

    private void addUnresponsiveRule(String fromNode, String toNode) {
        TransportService mockTransportService = internalCluster().getInstance(TransportService.class, fromNode);
        ((MockTransportService) mockTransportService).addUnresponsiveRule(internalCluster().getInstance(Discovery.class, toNode).localNode());
    }

    private void clearNoConnectRule(String fromNode, String toNode) {
        TransportService mockTransportService = internalCluster().getInstance(TransportService.class, fromNode);
        ((MockTransportService) mockTransportService).clearRule(internalCluster().getInstance(Discovery.class, toNode).localNode());
    }


    private void ensureStableCluster(int nodeCount) {
        ensureStableCluster(nodeCount, null);
    }

    private void ensureStableCluster(int nodeCount, @Nullable String viaNode) {
        ClusterHealthResponse clusterHealthResponse = client(viaNode).admin().cluster().prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForNodes(Integer.toString(nodeCount))
                .setWaitForRelocatingShards(0)
                .get();
        assertThat(clusterHealthResponse.isTimedOut(), is(false));
    }

}
