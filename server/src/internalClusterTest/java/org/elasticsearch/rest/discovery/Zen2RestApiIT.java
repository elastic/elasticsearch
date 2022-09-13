/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.discovery;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class Zen2RestApiIT extends ESIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    public void testRollingRestartOfTwoNodeCluster() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(1);
        final List<String> nodes = internalCluster().startNodes(2);
        createIndex(
            "test",
            Settings.builder()
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.ZERO) // assign shards
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2) // causes rebalancing
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .build()
        );
        ensureGreen("test");

        final DiscoveryNodes discoveryNodes = client().admin().cluster().prepareState().clear().setNodes(true).get().getState().nodes();
        final Map<String, String> nodeIdsByName = Maps.newMapWithExpectedSize(discoveryNodes.getSize());
        discoveryNodes.forEach(n -> nodeIdsByName.put(n.getName(), n.getId()));

        RestClient restClient = getRestClient();

        internalCluster().rollingRestart(new InternalTestCluster.RestartCallback() {
            @Override
            public void doAfterNodes(int n, Client client) throws IOException {
                ensureGreen("test");
                final Request request = new Request("POST", "/_cluster/voting_config_exclusions");
                final String nodeName = internalCluster().getNodeNames()[n];
                if (randomBoolean()) {
                    request.addParameter("node_names", nodeName);
                } else {
                    final String nodeId = nodeIdsByName.get(nodeName);
                    assertNotNull(nodeName, nodeId);
                    request.addParameter("node_ids", nodeId);
                }

                final Response response = restClient.performRequest(request);
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
                                        .boundAddress()
                                        .publishAddress()
                                        .toString()
                                )
                            )
                        )
                    );
                    Response deleteResponse = restClient.performRequest(new Request("DELETE", "/_cluster/voting_config_exclusions"));
                    assertThat(deleteResponse.getStatusLine().getStatusCode(), is(200));

                    ClusterHealthResponse clusterHealthResponse = client(viaNode).admin()
                        .cluster()
                        .prepareHealth()
                        .setWaitForEvents(Priority.LANGUID)
                        .setWaitForNodes(Integer.toString(1))
                        .setTimeout(TimeValue.timeValueSeconds(30L))
                        .setWaitForYellowStatus()
                        .get();
                    assertFalse(nodeName, clusterHealthResponse.isTimedOut());
                    return Settings.EMPTY;
                } finally {
                    restClient.setNodes(allNodes);
                }
            }
        });
        ensureStableCluster(2);
        ensureGreen("test");
        assertThat(internalCluster().size(), is(2));
    }

    public void testClearVotingTombstonesNotWaitingForRemoval() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(2);
        List<String> nodes = internalCluster().startNodes(3);
        ensureStableCluster(3);
        RestClient restClient = getRestClient();
        final Request request = new Request("POST", "/_cluster/voting_config_exclusions");
        request.addParameter("node_names", nodes.get(2));
        final Response response = restClient.performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        assertThat(response.getEntity().getContentLength(), is(0L));
        Response deleteResponse = restClient.performRequest(
            new Request("DELETE", "/_cluster/voting_config_exclusions/?wait_for_removal=false")
        );
        assertThat(deleteResponse.getStatusLine().getStatusCode(), is(200));
        assertThat(deleteResponse.getEntity().getContentLength(), is(0L));
    }

    public void testClearVotingTombstonesWaitingForRemoval() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(2);
        List<String> nodes = internalCluster().startNodes(3);
        ensureStableCluster(3);
        RestClient restClient = getRestClient();
        String nodeToWithdraw = nodes.get(randomIntBetween(0, 2));
        final Request request = new Request("POST", "/_cluster/voting_config_exclusions");
        request.addParameter("node_names", nodeToWithdraw);
        final Response response = restClient.performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        assertThat(response.getEntity().getContentLength(), is(0L));
        internalCluster().stopNode(nodeToWithdraw);
        Response deleteResponse = restClient.performRequest(new Request("DELETE", "/_cluster/voting_config_exclusions"));
        assertThat(deleteResponse.getStatusLine().getStatusCode(), is(200));
        assertThat(deleteResponse.getEntity().getContentLength(), is(0L));
    }

    public void testRemoveTwoNodesAtOnce() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(2);
        List<String> nodes = internalCluster().startNodes(3);
        ensureStableCluster(3);
        RestClient restClient = getRestClient();
        final Request request = new Request("POST", "/_cluster/voting_config_exclusions");
        request.addParameter("node_names", nodes.get(2) + "," + nodes.get(0));
        final Response response = restClient.performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        assertThat(response.getEntity().getContentLength(), is(0L));
        internalCluster().stopNode(nodes.get(0));
        internalCluster().stopNode(nodes.get(2));
        ensureStableCluster(1);
    }
}
