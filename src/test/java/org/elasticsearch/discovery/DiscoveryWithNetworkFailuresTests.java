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
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportModule;
import org.elasticsearch.transport.TransportService;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.*;

/**
 */
@ClusterScope(scope= Scope.TEST, numDataNodes =0)
public class DiscoveryWithNetworkFailuresTests extends ElasticsearchIntegrationTest {

    private static final Settings nodeSettings = ImmutableSettings.settingsBuilder()
            .put("discovery.type", "zen") // <-- To override the local setting if set externally
            .put("discovery.zen.fd.ping_timeout", "1s") // <-- for hitting simulated network failures quickly
            .put("discovery.zen.fd.ping_retries", "1") // <-- for hitting simulated network failures quickly
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

        // Wait until a green status has been reaches and 3 nodes are part of the cluster
        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForNodes("3")
                .get();
        assertThat(clusterHealthResponse.isTimedOut(), is(false));

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
        clusterHealthResponse = masterClient.admin().cluster().prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForNodes("3")
                .get();
        assertThat(clusterHealthResponse.isTimedOut(), is(false));

        for (String node : nodes) {
            ClusterState state = internalCluster().client(node).admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
            assertThat(state.nodes().size(), equalTo(3));
            // The elected master shouldn't have changed, since the unlucky node never could have elected himself as
            // master since m_m_n of 2 could never be satisfied.
            assertThat(state.nodes().masterNode(), equalTo(masterDiscoNode));
        }
    }

    @Test
    @Ignore
    @TestLogging("discovery.zen:TRACE,action:TRACE,cluster.service:TRACE,indices.recovery:TRACE,indices.cluster:TRACE")
    public void testDataConsistency() throws Exception {
        List<String> nodes = internalCluster().startNodesAsync(3, nodeSettings).get();

        // Wait until a green status has been reaches and 3 nodes are part of the cluster
        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForNodes("3")
                .get();
        assertThat(clusterHealthResponse.isTimedOut(), is(false));

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
        String isolatedNode = nodes.get(0);
        String nonIsolatedNode = nodes.get(1);
        final Client nonIsolatedNodeClient = internalCluster().client(nonIsolatedNode);

        // Simulate a network issue between the unlucky node and the rest of the cluster.
        for (String nodeId : nodes) {
            if (!nodeId.equals(isolatedNode)) {
                addFailToSendNoConnectRule(nodeId, isolatedNode);
                addFailToSendNoConnectRule(isolatedNode, nodeId);
            }
        }
        try {
            // Wait until elected master has removed that the unlucky node...
            boolean applied = awaitBusy(new Predicate<Object>() {
                @Override
                public boolean apply(Object input) {
                    return nonIsolatedNodeClient.admin().cluster().prepareState().setLocal(true).get().getState().nodes().size() == 2;
                }
            }, 1, TimeUnit.MINUTES);
            assertThat(applied, is(true));

            // The unlucky node must report *no* master node, since it can't connect to master and in fact it should
            // continuously ping until network failures have been resolved. However
            final Client isolatedNodeClient = internalCluster().client(isolatedNode);
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

            ClusterHealthResponse healthResponse = nonIsolatedNodeClient.admin().cluster().prepareHealth("test")
                    .setWaitForYellowStatus().get();
            assertThat(healthResponse.isTimedOut(), is(false));
            assertThat(healthResponse.getStatus(), equalTo(ClusterHealthStatus.YELLOW));

            // Reads on the right side of the split must work
            searchResponse = nonIsolatedNodeClient.prepareSearch("test").setTypes("type")
                    .addSort("field", SortOrder.ASC)
                    .get();
            assertHitCount(searchResponse, indexRequests.length);
            for (int i = 0; i < searchResponse.getHits().getHits().length; i++) {
                SearchHit searchHit = searchResponse.getHits().getAt(i);
                assertThat(searchHit.id(), equalTo(String.valueOf(i)));
                assertThat((long) searchHit.sortValues()[0], equalTo((long) i));
            }

            // Reads on the wrong side of the split are partial
            searchResponse = isolatedNodeClient.prepareSearch("test").setTypes("type")
                    .addSort("field", SortOrder.ASC)
                    .get();
            assertThat(searchResponse.getSuccessfulShards(), lessThan(searchResponse.getTotalShards()));
            assertThat(searchResponse.getHits().totalHits(), lessThan((long) indexRequests.length));

            // Writes on the right side of the split must work
            UpdateResponse updateResponse = nonIsolatedNodeClient.prepareUpdate("test", "type", "0").setDoc("field2", 2).get();
            assertThat(updateResponse.getVersion(), equalTo(2l));

            // Writes on the wrong side of the split fail
            try {
                isolatedNodeClient.prepareUpdate("test", "type", "0").setDoc("field2", 2)
                        .setTimeout(TimeValue.timeValueSeconds(5)) // Fail quick, otherwise we wait 60 seconds.
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
            for (String nodeId : nodes) {
                if (!nodeId.equals(isolatedNode)) {
                    clearNoConnectRule(nodeId, isolatedNode);
                    clearNoConnectRule(isolatedNode, nodeId);
                }
            }
        }

        // Wait until the master node sees all 3 nodes again.
        clusterHealthResponse = nonIsolatedNodeClient.admin().cluster().prepareHealth()
                .setWaitForGreenStatus()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForNodes("3")
                .get();
        assertThat(clusterHealthResponse.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealthResponse.isTimedOut(), is(false));

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
    @Ignore
    @TestLogging("discovery.zen:TRACE,action:TRACE,cluster.service:TRACE,indices.recovery:TRACE,indices.cluster:TRACE")
    public void testRejoinDocumentExistsInAllShardCopies() throws Exception {
        final List<String> nodes = internalCluster().startNodesAsync(3, nodeSettings).get();
        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForNodes("3")
                .get();
        assertThat(clusterHealthResponse.isTimedOut(), is(false));
        assertAcked(prepareCreate("test")
                .setSettings(ImmutableSettings.builder()
                                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 2)
                )
                .get());
        ensureGreen("test");

        String isolatedNode = findMasterNode(nodes).getName();
        String notIsolatedNode = null;
        for (String node : nodes) {
            if (!node.equals(isolatedNode)) {
                notIsolatedNode = node;
                break;
            }
        }

        logger.info("Isolating node[" + isolatedNode + "]");
        for (String nodeId : nodes) {
            if (!nodeId.equals(isolatedNode)) {
                addFailToSendNoConnectRule(nodeId, isolatedNode);
                addFailToSendNoConnectRule(isolatedNode, nodeId);
            }
        }
        ensureYellow("test");

        IndexResponse indexResponse = internalCluster().client(notIsolatedNode).prepareIndex("test", "type").setSource("field", "value").get();
        assertThat(indexResponse.getVersion(), equalTo(1l));

        logger.info("Verifying if document exists via node[" + notIsolatedNode + "]");
        GetResponse getResponse = internalCluster().client(notIsolatedNode).prepareGet("test", "type", indexResponse.getId())
                .setPreference("_local")
                .get();
        assertThat(getResponse.isExists(), is(true));
        assertThat(getResponse.getVersion(), equalTo(1l));
        assertThat(getResponse.getId(), equalTo(indexResponse.getId()));

        for (String nodeId : nodes) {
            if (!nodeId.equals(isolatedNode)) {
                clearNoConnectRule(nodeId, isolatedNode);
                clearNoConnectRule(isolatedNode, nodeId);
            }
        }

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

    private void clearNoConnectRule(String fromNode, String toNode) {
        TransportService mockTransportService = internalCluster().getInstance(TransportService.class, fromNode);
        ((MockTransportService) mockTransportService).clearRule(internalCluster().getInstance(Discovery.class, toNode).localNode());
    }

}
