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

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import java.util.List;

import static org.apache.lucene.util.LuceneTestCase.Slow;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.hamcrest.Matchers.equalTo;

/**
 */
@Slow
@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class ZenUnicastDiscoveryTestsSpecificNodes extends ElasticsearchIntegrationTest {

    @Test
    @TestLogging("discovery.zen:TRACE")
    // The bug zen unicast ping override bug, may rarely manifest itself, it is very timing dependant.
    // Without the fix in UnicastZenPing, this test fails roughly 1 out of 10 runs from the command line.
    public void testMasterElectionNotMissed() throws Exception {
        final Settings settings = settingsBuilder()
                // Failure only manifests if multicast ping is disabled!
                .put("discovery.zen.ping.multicast.ping.enabled", false)
                .put("discovery.zen.minimum_master_nodes", 2)
                // Can't use this, b/c at the moment all node will only ping localhost:9300
//                .put("discovery.zen.ping.unicast.hosts", "localhost")
                .put("discovery.zen.ping.unicast.hosts", "localhost:15300,localhost:15301,localhost:15302")
                .put("transport.tcp.port", "15300-15400")
                .build();
        List<String> nodes = internalCluster().startNodesAsync(3, settings).get();

        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("3").execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        DiscoveryNode masterDiscoNode = null;
        for (String node : nodes.toArray(new String[3])) {
            ClusterState state = internalCluster().client(node).admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
            assertThat(state.nodes().size(), equalTo(3));
            if (masterDiscoNode == null) {
                masterDiscoNode = state.nodes().masterNode();
            } else {
                assertThat(masterDiscoNode.equals(state.nodes().masterNode()), equalTo(true));
            }
        }
    }

}
