/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestResponseUtils;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestCatCircuitBreakerActionTests extends RestActionTestCase {

    private RestCatCircuitBreakerAction action;
    private NodeClient nodeClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        action = new RestCatCircuitBreakerAction();
        ClusterStateResponse clusterStateResponse = createClusterStateResponse();
        NodesStatsResponse nodeStatsResponse = mock(NodesStatsResponse.class);
        List<NodeStats> allNodeStats = createNodeStatsList();
        when(nodeStatsResponse.getNodes()).thenReturn(allNodeStats);
        try (var threadPool = createThreadPool()) {
            nodeClient = buildNodeClient(threadPool, clusterStateResponse, nodeStatsResponse);
        }
    }

    public void testRestCatCircuitBreakerActionSetup() {
        assertEquals("cat_circuitbreaker_action", action.getName());
        assertEquals(2, action.routes().size());
        assertEquals(GET, action.routes().getFirst().getMethod());
        assertEquals("/_cat/circuit_breaker", action.routes().getFirst().getPath());
        assertEquals(GET, action.routes().get(1).getMethod());
        assertEquals("/_cat/circuit_breaker/{circuit_breaker_patterns}", action.routes().get(1).getPath());

        StringBuilder sb = new StringBuilder();
        action.documentation(sb);
        assertEquals("/_cat/circuit_breaker\n/_cat/circuit_breaker/{circuit_breaker_patterns}\n", sb.toString());
    }

    public void testRestCatCircuitBreakerAction() throws Exception {
        FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(GET)
            .withPath("/_cat/circuit_breaker")
            .build();
        FakeRestChannel channel = new FakeRestChannel(restRequest, true, 0);

        action.handleRequest(restRequest, channel, nodeClient);

        assertThat(channel.responses().get(), equalTo(1));
        try (RestResponse response = channel.capturedResponse()) {
            assertThat(response.status(), equalTo(RestStatus.OK));
            String responseContent = RestResponseUtils.getBodyContent(response).utf8ToString();
            assertEquals(
                "node-1 request 1.4mb 750kb 0\n"
                    + "node-1 normal  2.4mb 1.2mb 1\n"
                    + "node-2 request 1.4mb 1.3mb 25\n"
                    + "node-3 big     1.5gb 768mb 5\n",
                responseContent
            );
        }
    }

    public void testRestCatCircuitBreakerActionWithPatternMatching() throws Exception {
        FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(GET)
            .withPath("/_cat/circuit_breaker/request")
            .withParams(Map.of("circuit_breaker_patterns", "request"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(restRequest, true, 0);

        action.handleRequest(restRequest, channel, nodeClient);

        assertThat(channel.responses().get(), equalTo(1));
        try (RestResponse response = channel.capturedResponse()) {
            assertThat(response.status(), equalTo(RestStatus.OK));
            String responseContent = RestResponseUtils.getBodyContent(response).utf8ToString();
            assertEquals("node-1 request 1.4mb 750kb 0\n" + "node-2 request 1.4mb 1.3mb 25\n", responseContent);
        }
    }

    private ClusterStateResponse createClusterStateResponse() {
        DiscoveryNode node1 = createDiscoveryNode("node-1", "test-node-1");
        DiscoveryNode node2 = createDiscoveryNode("node-2", "test-node-2");
        DiscoveryNode node3 = createDiscoveryNode("node-3", "test-node-3");
        DiscoveryNodes discoveryNodes = createDiscoveryNodes(node1, node2, node3);
        ClusterState clusterState = createClusterState(discoveryNodes);
        return createClusterStateResponse(clusterState);
    }

    private DiscoveryNode createDiscoveryNode(final String nodeId, final String nodeName) {
        DiscoveryNode node = mock(DiscoveryNode.class);
        when(node.getId()).thenReturn(nodeId);
        when(node.getName()).thenReturn(nodeName);
        return node;
    }

    private DiscoveryNodes createDiscoveryNodes(final DiscoveryNode... nodes) {
        DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        when(discoveryNodes.stream()).thenReturn(Stream.of(nodes));
        return discoveryNodes;
    }

    private ClusterState createClusterState(final DiscoveryNodes discoveryNodes) {
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.nodes()).thenReturn(discoveryNodes);
        return clusterState;
    }

    private ClusterStateResponse createClusterStateResponse(final ClusterState clusterState) {
        ClusterStateResponse clusterStateResponse = mock(ClusterStateResponse.class);
        when(clusterStateResponse.getState()).thenReturn(clusterState);
        return clusterStateResponse;
    }

    private List<NodeStats> createNodeStatsList() {
        return List.of(
            createNodeStats(
                "node-1",
                "test-node-1",
                createCircuitBreakerStats(
                    createBreakerStats("request", 1536000L, 768000L, 1.5, 0L),
                    createBreakerStats("normal", 2560000L, 1280000L, 1.0, 1L)
                )
            ),
            createNodeStats(
                "node-2",
                "test-node-2",
                createCircuitBreakerStats(createBreakerStats("request", 1536000L, 1459200L, 1.5, 25L))
            ),
            createNodeStats("node-3", "test-node-3", createCircuitBreakerStats(createBreakerStats("big", 1610612736L, 805306368L, 1.2, 5L)))
        );
    }

    private NodeStats createNodeStats(final String nodeId, final String nodeName, final AllCircuitBreakerStats breakerStats) {
        NodeStats nodeStats = mock(NodeStats.class);
        DiscoveryNode node = mock(DiscoveryNode.class);
        when(node.getId()).thenReturn(nodeId);
        when(node.getName()).thenReturn(nodeName);
        when(nodeStats.getNode()).thenReturn(node);
        when(nodeStats.getBreaker()).thenReturn(breakerStats);
        return nodeStats;
    }

    private CircuitBreakerStats createBreakerStats(
        final String name,
        final long limit,
        final long estimated,
        final double overhead,
        final long trippedCount
    ) {
        CircuitBreakerStats breaker = mock(CircuitBreakerStats.class);
        when(breaker.getName()).thenReturn(name);
        when(breaker.getLimit()).thenReturn(limit);
        when(breaker.getEstimated()).thenReturn(estimated);
        when(breaker.getOverhead()).thenReturn(overhead);
        when(breaker.getTrippedCount()).thenReturn(trippedCount);
        return breaker;
    }

    private AllCircuitBreakerStats createCircuitBreakerStats(final CircuitBreakerStats... breakers) {
        AllCircuitBreakerStats allStats = mock(AllCircuitBreakerStats.class);
        when(allStats.getAllStats()).thenReturn(breakers);
        return allStats;
    }

    private NoOpNodeClient buildNodeClient(
        ThreadPool threadPool,
        ClusterStateResponse clusterStateResponse,
        NodesStatsResponse nodesStatsResponse
    ) {
        return new NoOpNodeClient(threadPool) {
            @Override
            @SuppressWarnings("unchecked")
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (request instanceof ClusterStateRequest) {
                    listener.onResponse((Response) clusterStateResponse);
                } else if (request instanceof NodesStatsRequest) {
                    listener.onResponse((Response) nodesStatsResponse);
                } else {
                    throw new AssertionError(String.format(Locale.ROOT, "Unexpected action type: %s request: %s", action, request));
                }
            }
        };
    }
}
