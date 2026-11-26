/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xpack.esql.heap_attack;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Listing the tasks of a cluster whose nodes hold many queries with long descriptions: each node replies with the descriptions of its own
 * tasks, and the coordinating node holds the replies of all the other nodes until it has all of them. Each reply fits in the in-flight
 * requests breaker of the coordinating node, but the replies of two nodes do not, so the coordinating node must trip the breaker, and fail
 * to take those replies, instead of exhausting its heap. The queries are held through the other nodes, so that the coordinating node does
 * not hold any of them itself.
 */
public class ListTasksCoordinatingNodeLimitHeapAttackIT extends ListTasksHeapAttackTestCase {

    private static final int HELD_QUERIES = 8;
    private static final int DATA_NODES = 2;
    private static final int LIST_CALLS = 5;
    private static final String COORDINATING_NODE_LIMIT = "30mb";
    private static final long IN_FLIGHT_SLACK_BYTES = 1024 * 1024;

    @ClassRule
    public static ElasticsearchCluster cluster = buildCluster();

    private static ElasticsearchCluster buildCluster() {
        try {
            return Clusters.buildClusterSpec()
                .nodes(3)
                .node(0, node -> node.setting("network.breaker.inflight_requests.limit", COORDINATING_NODE_LIMIT))
                .build();
        } catch (UnsupportedOperationException e) {
            return Clusters.buildCluster();
        }
    }

    @Override
    protected ElasticsearchCluster testCluster() {
        return cluster;
    }

    public void testRepliesOfSeveralNodesTripCircuitBreakerOfCoordinatingNode() throws Exception {
        assumeFalse("serverless does not allow changing the number of nodes", isServerless());
        List<String> asyncIds = new ArrayList<>();
        try (RestClient coordinatingClient = buildCoordinatingClient()) {
            createPausableIndex(3, coordinatingNodeId(coordinatingClient));
            blockPauseField();
            try {
                for (int i = 0; i < HELD_QUERIES; i++) {
                    asyncIds.add(submitHeldQuery(i));
                }
                awaitDataNodeTasksOfHeldQueries(coordinatingClient, HELD_QUERIES * DATA_NODES);

                for (String nodeId : remoteNodeIds(coordinatingClient)) {
                    assertReplyOfNodeFits(coordinatingClient, nodeId);
                }

                long inFlightBefore = inFlightBytes(coordinatingClient);
                int tripped = 0;
                for (int call = 0; call < LIST_CALLS; call++) {
                    if (listTasksTripsCoordinatingNodeBreaker(coordinatingClient)) {
                        tripped++;
                    }
                    assertBusy(
                        () -> assertThat(inFlightBytes(coordinatingClient), lessThanOrEqualTo(inFlightBefore + IN_FLIGHT_SLACK_BYTES))
                    );
                }
                assertThat(tripped, equalTo(LIST_CALLS));
            } catch (Exception | Error e) {
                try {
                    cleanUp(asyncIds);
                } catch (Exception | Error cleanUpFailure) {
                    e.addSuppressed(cleanUpFailure);
                }
                throw e;
            }
            cleanUp(asyncIds);
        }
    }

    private long inFlightBytes(RestClient coordinatingClient) throws IOException {
        Request request = new Request("GET", "/_nodes/_local/stats/breaker");
        request.addParameter("filter_path", "nodes.*.breakers.inflight_requests.estimated_size_in_bytes");
        Map<?, ?> nodes = (Map<?, ?>) entityAsMap(coordinatingClient.performRequest(request)).get("nodes");
        Map<?, ?> node = (Map<?, ?>) nodes.values().iterator().next();
        return ((Number) XContentMapValues.extractValue("breakers.inflight_requests.estimated_size_in_bytes", node)).longValue();
    }

    private List<String> remoteNodeIds(RestClient coordinatingClient) throws IOException {
        String coordinatingNodeId = coordinatingNodeId(coordinatingClient);
        Request all = new Request("GET", "/_nodes");
        all.addParameter("filter_path", "nodes.*.name");
        List<String> remote = new ArrayList<>();
        for (Object nodeId : ((Map<?, ?>) entityAsMap(client().performRequest(all)).get("nodes")).keySet()) {
            if (nodeId.toString().equals(coordinatingNodeId) == false) {
                remote.add(nodeId.toString());
            }
        }
        assertThat(remote, hasSize(DATA_NODES));
        return remote;
    }

    private void assertReplyOfNodeFits(RestClient coordinatingClient, String nodeId) throws IOException {
        Request request = new Request("GET", "/_tasks");
        request.addParameter("detailed", "true");
        request.addParameter("actions", "*esql*");
        request.addParameter("nodes", nodeId);
        request.addParameter("filter_path", "node_failures,nodes.*.tasks.*.action");
        Map<String, Object> response = entityAsMap(coordinatingClient.performRequest(request));
        assertNull("node failures listing the tasks of node [" + nodeId + "] alone", response.get("node_failures"));
        Map<?, ?> nodes = (Map<?, ?>) response.get("nodes");
        assertNotNull("no tasks listed from node [" + nodeId + "]", nodes);
        Map<?, ?> node = (Map<?, ?>) nodes.get(nodeId);
        assertNotNull("no tasks listed from node [" + nodeId + "]", node);
        assertThat(((Map<?, ?>) node.get("tasks")).size(), greaterThan(0));
    }

    /**
     * Every node failure of the listing must be the trip of the in-flight breaker on a reply. Both replies are rejected if they arrive
     * together, and only the second one if they do not, so the number of failures is not asserted.
     */
    private boolean listTasksTripsCoordinatingNodeBreaker(RestClient coordinatingClient) throws IOException {
        Request request = new Request("GET", "/_tasks");
        request.addParameter("detailed", "true");
        request.addParameter("actions", "*esql*");
        request.addParameter("filter_path", "node_failures");
        Map<String, Object> response = entityAsMap(coordinatingClient.performRequest(request));
        List<?> nodeFailures = (List<?>) response.get("node_failures");
        if (nodeFailures == null) {
            return false;
        }
        for (Object failure : nodeFailures) {
            assertTrue("node failure other than a trip of the in-flight breaker on a reply: " + failure, isReplyBreakerTrip(failure));
        }
        return true;
    }

    private static boolean isReplyBreakerTrip(Object failure) {
        return "circuit_breaking_exception".equals(XContentMapValues.extractValue("caused_by.type", (Map<?, ?>) failure))
            && String.valueOf(XContentMapValues.extractValue("caused_by.reason", (Map<?, ?>) failure))
                .startsWith("[inflight_requests] Data too large, data for [<node_tasks_response>]");
    }
}
