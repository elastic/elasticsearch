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

package org.elasticsearch.rest.discovery;

import org.apache.http.HttpHost;
import org.elasticsearch.ESNetty4IntegTestCase;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.coordination.ClusterBootstrapService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.zen.ElectMasterService;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.discovery.TestZenDiscovery;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.core.Is.is;

// TODO: Move these tests to a more appropriate module
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, transportClientRatio = 0, autoMinMasterNodes = false)
public class Zen2RestApiIT extends ESNetty4IntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
            .put(TestZenDiscovery.USE_ZEN2.getKey(), true)
            .put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), 2)
            .put(ClusterBootstrapService.INITIAL_MASTER_NODE_COUNT_SETTING.getKey(), 2)
            .put(DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING.getKey(), "5s")
            .build();
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    public void testAddAndClearVotingTombstones() throws Exception {
        final int nodeCount = 2;
        final List<String> nodes = internalCluster().startNodes(nodeCount);
        createIndex("test",
            Settings.builder()
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.ZERO) // assign shards
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, nodeCount) // causes rebalancing
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build());
        ensureGreen("test");

        RestClient restClient = getRestClient();

        internalCluster().rollingRestart(new InternalTestCluster.RestartCallback() {
            @Override
            public void doAfterNodes(int n, Client client) throws IOException {
                ensureGreen("test");
                Response response =
                    restClient.performRequest(new Request("POST", "/_cluster/withdrawn_votes/" + internalCluster().getNodeNames()[n]));
                assertThat(response.getStatusLine().getStatusCode(), is(200));
            }

            @Override
            public Settings onNodeStopped(String nodeName) throws IOException {
                String viaNode = randomValueOtherThan(nodeName, () -> randomFrom(nodes));

                List<Node> allNodes = restClient.getNodes();
                try {
                    restClient.setNodes(
                        Collections.singletonList(
                            new Node(
                                HttpHost.create(
                                    internalCluster().getInstance(HttpServerTransport.class, viaNode)
                                        .boundAddress().publishAddress().toString()
                                )
                            )
                        )
                    );
                    Response deleteResponse = restClient.performRequest(new Request("DELETE", "/_cluster/withdrawn_votes"));
                    assertThat(deleteResponse.getStatusLine().getStatusCode(), is(200));

                    final ClusterHealthRequestBuilder clusterHealthRequestBuilder = client(viaNode).admin().cluster().prepareHealth()
                        .setWaitForEvents(Priority.LANGUID)
                        .setWaitForNodes(Integer.toString(nodeCount - 1))
                        .setTimeout(TimeValue.timeValueSeconds(30L));

                    clusterHealthRequestBuilder.setWaitForYellowStatus();
                    ClusterHealthResponse clusterHealthResponse = clusterHealthRequestBuilder.get();
                    assertFalse(nodeName, clusterHealthResponse.isTimedOut());
                    return Settings.EMPTY;
                } finally {
                    restClient.setNodes(allNodes);
                }
            }
        });
        ensureStableCluster(nodeCount);
        ensureGreen("test");
        assertThat(internalCluster().size(), is(2));
    }

    public void testBasicRestApi() throws Exception {
        List<String> nodes = internalCluster().startNodes(3);
        RestClient restClient = getRestClient();
        Response deleteResponse = restClient.performRequest(new Request("DELETE", "/_cluster/withdrawn_votes"));
        assertThat(deleteResponse.getStatusLine().getStatusCode(), is(200));
        assertThat(deleteResponse.getEntity().getContentLength(), is(0L));
        Response response =
            restClient.performRequest(new Request("POST", "/_cluster/withdrawn_votes/" + nodes.get(0)));
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        assertThat(response.getEntity().getContentLength(), is(0L));
        try {
            restClient.performRequest(new Request("POST", "/_cluster/withdrawn_votes/invalid"));
            fail("Invalid node name should throw.");
        } catch (ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(400));
            assertThat(
                e.getCause().getMessage(),
                Matchers.containsString("add voting tombstones request for [invalid] matched no master-eligible nodes")
            );
        }
    }
}
