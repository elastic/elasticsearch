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

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class ZenUnicastDiscoveryTests extends ElasticsearchIntegrationTest {

    private static int NUM_OF_NODES = -1;

    static int BASE_PORT = -1;
    static int NUM_OF_UNICAST_HOSTS = -1;

    @Before
    public void setUP() throws Exception {
        ElasticsearchIntegrationTest.beforeClass();
        NUM_OF_NODES = randomIntBetween(3, 5);
        NUM_OF_UNICAST_HOSTS = randomIntBetween(1, NUM_OF_NODES);
        BASE_PORT = 25000 + randomInt(100);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder()
                .put("discovery.type", "zen")
                .put("discovery.zen.ping.multicast.enabled", false)
                .put("http.enabled", false) // just to make test quicker
                .put(super.nodeSettings(nodeOrdinal));

        String[] unicastHosts = new String[NUM_OF_UNICAST_HOSTS];
        if (internalCluster().getDefaultSettings().get("node.mode").equals("local")) {
            builder.put("transport.address", "unicast_test_" + nodeOrdinal);
            // TODO - find a better solution for the test. prefix
            builder.put("test.transport.address", "unicast_test_" + nodeOrdinal);
            for (int i = 0; i < unicastHosts.length; i++) {
                unicastHosts[i] = "unicast_test_" + i;
            }
        } else {
            // we need to pin the node ports so we'd know where to point things
            builder.put("transport.tcp.port", BASE_PORT + nodeOrdinal);
            for (int i = 0; i < unicastHosts.length; i++) {
                unicastHosts[i] = "localhost:" + (BASE_PORT + i);
            }
        }
        builder.putArray("discovery.zen.ping.unicast.hosts", unicastHosts);
        return builder.build();
    }

    @Test
    public void testSimpleDiscovery() throws ExecutionException, InterruptedException {
        internalCluster().startNodesAsync(NUM_OF_NODES).get();

        for (Client client : clients()) {
            ClusterState state = client.admin().cluster().prepareState().execute().actionGet().getState();
            //client nodes might be added randomly
            int dataNodes = 0;
            for (DiscoveryNode discoveryNode : state.nodes()) {
                if (discoveryNode.isDataNode()) {
                    dataNodes++;
                }
            }
            assertThat(dataNodes, equalTo(internalCluster().numDataNodes()));
        }
    }

    @Test
    // Without the 'include temporalResponses responses to nodesToConnect' improvement in UnicastZenPing#sendPings this
    // test fails, because 2 nodes elect themselves as master and the health request times out b/c waiting_for_nodes=N
    // can't be satisfied.
    public void testMinimumMasterNode() throws Exception {
        final Settings settings = ImmutableSettings.settingsBuilder().put("discovery.zen.minimum_master_nodes", NUM_OF_NODES).build();

        List<String> nodes = internalCluster().startNodesAsync(NUM_OF_NODES, settings).get();

        ensureGreen();

        DiscoveryNode masterDiscoNode = null;
        for (String node : nodes) {
            ClusterState state = internalCluster().client(node).admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
            assertThat(state.nodes().size(), equalTo(NUM_OF_NODES));
            if (masterDiscoNode == null) {
                masterDiscoNode = state.nodes().masterNode();
            } else {
                assertThat(masterDiscoNode.equals(state.nodes().masterNode()), equalTo(true));
            }
        }
    }
}
