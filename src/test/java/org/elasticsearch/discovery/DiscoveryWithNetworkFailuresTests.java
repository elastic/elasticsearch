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
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportModule;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 */
@ClusterScope(scope= Scope.TEST, numDataNodes =0)
public class DiscoveryWithNetworkFailuresTests extends ElasticsearchIntegrationTest {

    @Test
    @TestLogging("discovery.zen:TRACE")
    public void failWithMinimumMasterNodesConfigured() throws Exception {
        final Settings settings = ImmutableSettings.settingsBuilder()
                .put("discovery.type", "zen") // <-- To override the local setting if set externally
                .put("discovery.zen.fd.ping_timeout", "1s") // <-- for hitting simulated network failures quickly
                .put("discovery.zen.fd.ping_retries", "1") // <-- for hitting simulated network failures quickly
                .put("discovery.zen.minimum_master_nodes", 2)
                .put(TransportModule.TRANSPORT_SERVICE_TYPE_KEY, MockTransportService.class.getName())
                .build();

        List<String> nodes = internalCluster().startNodesAsync(3, settings).get();

        // Wait until a green status has been reaches and 3 nodes are part of the cluster
        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForNodes("3")
                .get();
        assertThat(clusterHealthResponse.isTimedOut(), is(false));

        // Figure out what is the elected master node
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
                    logger.info("localDiscoveryNodes=" + localDiscoveryNodes.toString());
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

    private void addFailToSendNoConnectRule(String fromNode, String toNode) {
        TransportService mockTransportService = internalCluster().getInstance(TransportService.class, fromNode);
        ((MockTransportService) mockTransportService).addFailToSendNoConnectRule(internalCluster().getInstance(Discovery.class, toNode).localNode());
    }

    private void clearNoConnectRule(String fromNode, String toNode) {
        TransportService mockTransportService = internalCluster().getInstance(TransportService.class, fromNode);
        ((MockTransportService) mockTransportService).clearRule(internalCluster().getInstance(Discovery.class, toNode).localNode());
    }

}
