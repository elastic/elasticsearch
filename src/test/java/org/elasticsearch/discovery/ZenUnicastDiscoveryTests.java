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

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.elasticsearch.test.discovery.ClusterDiscoveryConfiguration;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
@Slow
public class ZenUnicastDiscoveryTests extends ElasticsearchIntegrationTest {

    private ClusterDiscoveryConfiguration discoveryConfig;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return discoveryConfig.node(nodeOrdinal);
    }

    @Before
    public void clearConfig() {
        discoveryConfig = null;
    }

    @Test
    public void testNormalClusterForming() throws ExecutionException, InterruptedException {
        int currentNumNodes = randomIntBetween(3, 5);

        // use explicit unicast hosts so we can start those first
        int[] unicastHostOrdinals = new int[randomIntBetween(1, currentNumNodes)];
        for (int i = 0; i < unicastHostOrdinals.length; i++) {
            unicastHostOrdinals[i] = i;
        }
        discoveryConfig = new ClusterDiscoveryConfiguration.UnicastZen(currentNumNodes, unicastHostOrdinals);

        // start the unicast hosts
        internalCluster().startNodesAsync(unicastHostOrdinals.length).get();

        // start the rest of the cluster
        internalCluster().startNodesAsync(currentNumNodes - unicastHostOrdinals.length).get();

        if (client().admin().cluster().prepareHealth().setWaitForNodes("" + currentNumNodes).get().isTimedOut()) {
            logger.info("cluster forming timed out, cluster state:\n{}", client().admin().cluster().prepareState().get().getState().prettyPrint());
            fail("timed out waiting for cluster to form with [" + currentNumNodes + "] nodes");
        }
    }

    @Test
    // Without the 'include temporalResponses responses to nodesToConnect' improvement in UnicastZenPing#sendPings this
    // test fails, because 2 nodes elect themselves as master and the health request times out b/c waiting_for_nodes=N
    // can't be satisfied.
    public void testMinimumMasterNodes() throws Exception {
        int currentNumNodes = randomIntBetween(3, 5);
        final int min_master_nodes = currentNumNodes / 2 + 1;
        int currentNumOfUnicastHosts = randomIntBetween(min_master_nodes, currentNumNodes);
        final Settings settings = ImmutableSettings.settingsBuilder().put("discovery.zen.minimum_master_nodes", min_master_nodes).build();
        discoveryConfig = new ClusterDiscoveryConfiguration.UnicastZen(currentNumNodes, currentNumOfUnicastHosts, settings);

        List<String> nodes = internalCluster().startNodesAsync(currentNumNodes).get();

        ensureGreen();

        DiscoveryNode masterDiscoNode = null;
        for (String node : nodes) {
            ClusterState state = internalCluster().client(node).admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
            assertThat(state.nodes().size(), equalTo(currentNumNodes));
            if (masterDiscoNode == null) {
                masterDiscoNode = state.nodes().masterNode();
            } else {
                assertThat(masterDiscoNode.equals(state.nodes().masterNode()), equalTo(true));
            }
        }
    }
}
