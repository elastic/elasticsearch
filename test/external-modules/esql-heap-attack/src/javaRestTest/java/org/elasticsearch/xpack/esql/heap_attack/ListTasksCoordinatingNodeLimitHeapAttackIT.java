/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xpack.esql.heap_attack;

import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

/**
 * Listing the tasks of a cluster whose nodes hold many queries with long descriptions: each node replies with the descriptions of its own
 * tasks, and the coordinating node holds the replies of all the other nodes until it has all of them. Each reply fits in the in-flight
 * requests breaker of the coordinating node, but the replies of two nodes do not, so the coordinating node must trip the breaker, and fail
 * to take those replies, instead of exhausting its heap. The queries are held through the other nodes, so that the coordinating node does
 * not hold any of them itself.
 */
public class ListTasksCoordinatingNodeLimitHeapAttackIT extends HeapAttackRestHelpers {

    private static final int LITERAL_LENGTH = 900_000;
    private static final int HELD_QUERIES = 8;
    private static final int LIST_CALLS = 5;
    private static final String COORDINATING_NODE_LIMIT = "30mb";

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.buildClusterSpec()
        .nodes(3)
        .node(0, node -> node.setting("network.breaker.inflight_requests.limit", COORDINATING_NODE_LIMIT))
        .build();

    @Override
    protected String getTestRestCluster() {
        String[] addresses = Clusters.testRestCluster(cluster).split(",");
        return String.join(",", Arrays.copyOfRange(addresses, 1, addresses.length));
    }

    public void testRepliesOfSeveralNodesTripCircuitBreakerOfCoordinatingNode() throws Exception {
        createPausableIndex();
        blockPauseField();
        List<String> asyncIds = new ArrayList<>();
        String coordinatingAddress = cluster.getHttpAddresses().split(",")[0];
        int portSeparator = coordinatingAddress.lastIndexOf(':');
        String host = coordinatingAddress.substring(0, portSeparator).replace("[", "").replace("]", "");
        int port = Integer.parseInt(coordinatingAddress.substring(portSeparator + 1));
        try (RestClient coordinatingClient = buildClient(restClientSettings(), new HttpHost[] { new HttpHost(host, port, "http") })) {
            for (int i = 0; i < HELD_QUERIES; i++) {
                asyncIds.add(submitAsyncQuery(queryWithLongLiteral("q" + i + "-" + "\u20ac".repeat(LITERAL_LENGTH))));
            }

            for (String nodeId : remoteNodeIds(coordinatingClient)) {
                assertReplyOfNodeFits(coordinatingClient, nodeId);
            }

            int tripped = 0;
            for (int call = 0; call < LIST_CALLS; call++) {
                if (listTasksTripsCoordinatingNodeBreaker(coordinatingClient)) {
                    tripped++;
                }
            }
            assertThat(tripped, equalTo(LIST_CALLS));
        } finally {
            unblockPauseField();
            for (String id : asyncIds) {
                deleteAsyncQuery(id);
            }
            for (String id : asyncIds) {
                ensureAsyncQueryCompleted(id);
            }
        }
    }

    private List<String> remoteNodeIds(RestClient coordinatingClient) throws IOException {
        Request local = new Request("GET", "/_nodes/_local");
        local.addParameter("filter_path", "nodes.*.name");
        String coordinatingNodeId = ((Map<?, ?>) entityAsMap(coordinatingClient.performRequest(local)).get("nodes")).keySet()
            .iterator()
            .next()
            .toString();
        Request all = new Request("GET", "/_nodes");
        all.addParameter("filter_path", "nodes.*.name");
        List<String> remote = new ArrayList<>();
        for (Object nodeId : ((Map<?, ?>) entityAsMap(client().performRequest(all)).get("nodes")).keySet()) {
            if (nodeId.toString().equals(coordinatingNodeId) == false) {
                remote.add(nodeId.toString());
            }
        }
        assertThat(remote, hasSize(2));
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

    private boolean listTasksTripsCoordinatingNodeBreaker(RestClient coordinatingClient) throws IOException {
        Request request = new Request("GET", "/_tasks");
        request.addParameter("detailed", "true");
        request.addParameter("actions", "*esql*");
        request.addParameter("filter_path", "node_failures");
        Map<String, Object> response = entityAsMap(coordinatingClient.performRequest(request));
        List<?> nodeFailures = (List<?>) response.get("node_failures");
        return nodeFailures != null
            && nodeFailures.stream()
                .anyMatch(
                    failure -> "circuit_breaking_exception".equals(XContentMapValues.extractValue("caused_by.type", (Map<?, ?>) failure))
                        && String.valueOf(XContentMapValues.extractValue("caused_by.reason", (Map<?, ?>) failure))
                            .contains("<node_tasks_response>")
                );
    }

    private void createPausableIndex() throws IOException {
        Request createIndex = new Request("PUT", "/pause_test");
        createIndex.setJsonEntity("""
            {
                "settings": {
                    "number_of_shards": 3,
                    "number_of_replicas": 0
                },
                "mappings": {
                    "runtime": {
                        "pause_long_field": {
                            "type": "long",
                            "script": {
                                "source": "",
                                "lang": "pause"
                            }
                        }
                    },
                    "properties": {
                        "tag": {
                            "type": "keyword"
                        }
                    }
                }
            }""");
        client().performRequest(createIndex);
        Request bulk = new Request("POST", "/pause_test/_bulk?refresh=true");
        StringBuilder body = new StringBuilder();
        for (int i = 0; i < 30; i++) {
            body.append("{\"index\":{}}\n").append("{\"tag\":\"tag-").append(i).append("\"}\n");
        }
        bulk.setJsonEntity(body.toString());
        client().performRequest(bulk);
        ensureGreen("pause_test");
    }

    private String queryWithLongLiteral(String literal) {
        return "FROM pause_test | WHERE tag != \\\"" + literal + "\\\" | STATS SUM(pause_long_field) | LIMIT 1";
    }

    private String submitAsyncQuery(String query) throws IOException {
        Request request = new Request("POST", "/_query/async");
        request.setJsonEntity("{\"query\": \"" + query + "\", \"wait_for_completion_timeout\": \"10ms\", \"keep_on_completion\": false}");
        Response response = client().performRequest(request);
        return (String) entityAsMap(response).get("id");
    }

    private void deleteAsyncQuery(String id) throws Exception {
        assertBusy(() -> {
            try {
                client().performRequest(new Request("DELETE", "/_query/async/" + id));
            } catch (ResponseException ex) {
                if (ex.getResponse().getStatusLine().getStatusCode() != HttpStatus.SC_NOT_FOUND) {
                    throw ex;
                }
            }
        });
    }

    private void ensureAsyncQueryCompleted(String id) throws Exception {
        assertBusy(() -> {
            try {
                client().performRequest(new Request("GET", "/_query/async/" + id));
                fail("expected async query [" + id + "] to be gone");
            } catch (ResponseException ex) {
                int statusCode = ex.getResponse().getStatusLine().getStatusCode();
                if (statusCode != HttpStatus.SC_TOO_MANY_REQUESTS && statusCode != HttpStatus.SC_NOT_FOUND) {
                    throw ex;
                }
            }
        });
    }

    private void blockPauseField() throws IOException {
        client().performRequest(new Request("POST", "/_pause_field/block"));
    }

    private void unblockPauseField() throws IOException {
        client().performRequest(new Request("POST", "/_pause_field/unblock"));
    }
}
