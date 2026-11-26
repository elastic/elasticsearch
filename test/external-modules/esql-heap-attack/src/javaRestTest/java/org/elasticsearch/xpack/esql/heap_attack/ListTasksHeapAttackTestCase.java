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
import org.elasticsearch.test.cluster.ElasticsearchCluster;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * What the heap attack tests that list the tasks of a cluster share: they hold queries with long descriptions through every node but one,
 * which they then list the tasks through. The queries are paused by the pausable field, and their descriptions carry a long literal of
 * characters that take 3 bytes each when sent between nodes.
 */
abstract class ListTasksHeapAttackTestCase extends HeapAttackRestHelpers {

    private static final int LITERAL_LENGTH = 900_000;

    protected abstract ElasticsearchCluster testCluster();

    /**
     * The queries are held through all the nodes but the first one, which is the coordinating node of the listings. The only address of a
     * cluster that has just one, as in serverless, is kept.
     */
    @Override
    protected String getTestRestCluster() {
        String[] addresses = Clusters.testRestCluster(testCluster()).split(",");
        return addresses.length == 1 ? addresses[0] : String.join(",", Arrays.copyOfRange(addresses, 1, addresses.length));
    }

    protected RestClient buildCoordinatingClient() throws IOException {
        String address = testCluster().getHttpAddresses().split(",")[0];
        int portSeparator = address.lastIndexOf(':');
        String host = address.substring(0, portSeparator).replace("[", "").replace("]", "");
        int port = Integer.parseInt(address.substring(portSeparator + 1));
        return buildClient(restClientSettings(), new HttpHost[] { new HttpHost(host, port, "http") });
    }

    protected String coordinatingNodeId(RestClient coordinatingClient) throws IOException {
        Request request = new Request("GET", "/_nodes/_local");
        request.addParameter("filter_path", "nodes.*.name");
        Map<?, ?> nodes = (Map<?, ?>) entityAsMap(coordinatingClient.performRequest(request)).get("nodes");
        return nodes.keySet().iterator().next().toString();
    }

    protected void createPausableIndex(int shards, String excludedNodeId) throws IOException {
        Request createIndex = new Request("PUT", "/pause_test");
        createIndex.setJsonEntity("""
            {
                "settings": {
                    "number_of_shards": %d,
                    "number_of_replicas": 0,
                    "index.routing.allocation.exclude._id": "%s"
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
            }""".formatted(shards, excludedNodeId));
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

    protected String submitHeldQuery(int number) throws IOException {
        String literal = "q" + number + "-" + "€".repeat(LITERAL_LENGTH);
        String query = "FROM pause_test | WHERE tag != \\\"" + literal + "\\\" | STATS SUM(pause_long_field) | LIMIT 1";
        Request request = new Request("POST", "/_query/async");
        request.setJsonEntity("{\"query\": \"" + query + "\", \"wait_for_completion_timeout\": \"10ms\", \"keep_on_completion\": false}");
        Response response = client().performRequest(request);
        return (String) entityAsMap(response).get("id");
    }

    /**
     * Waits for the tasks that the data nodes run for the held queries. An async query returns its ID before they are registered.
     */
    protected void awaitDataNodeTasksOfHeldQueries(RestClient coordinatingClient, int expected) throws Exception {
        assertBusy(() -> {
            Request request = new Request("GET", "/_tasks");
            request.addParameter("actions", "indices:data/read/esql/data*");
            request.addParameter("group_by", "none");
            request.addParameter("filter_path", "tasks.action");
            List<?> tasks = (List<?>) entityAsMap(coordinatingClient.performRequest(request)).get("tasks");
            assertThat(tasks == null ? 0 : tasks.size(), greaterThanOrEqualTo(expected));
        });
    }

    protected void blockPauseField() throws IOException {
        client().performRequest(new Request("POST", "/_pause_field/block"));
    }

    /**
     * Releases the held queries and waits until they are gone. Every step is tried even if another failed, and the failures are
     * reported together.
     */
    protected void cleanUp(List<String> asyncIds) throws Exception {
        Throwable failure = null;
        try {
            client().performRequest(new Request("POST", "/_pause_field/unblock"));
        } catch (Exception | AssertionError e) {
            failure = e;
        }
        for (String id : asyncIds) {
            try {
                deleteAsyncQuery(id);
            } catch (Exception | AssertionError e) {
                failure = accumulate(failure, e);
            }
        }
        for (String id : asyncIds) {
            try {
                awaitAsyncQueryGone(id);
            } catch (Exception | AssertionError e) {
                failure = accumulate(failure, e);
            }
        }
        if (failure instanceof Exception exception) {
            throw exception;
        }
        if (failure instanceof Error error) {
            throw error;
        }
    }

    private static Throwable accumulate(Throwable first, Throwable next) {
        if (first == null) {
            return next;
        }
        first.addSuppressed(next);
        return first;
    }

    private void deleteAsyncQuery(String id) throws Exception {
        assertBusy(() -> {
            try {
                client().performRequest(new Request("DELETE", "/_query/async/" + id));
                return;
            } catch (ResponseException ex) {
                int statusCode = ex.getResponse().getStatusLine().getStatusCode();
                if (statusCode == HttpStatus.SC_NOT_FOUND) {
                    return;
                }
                if (statusCode != HttpStatus.SC_TOO_MANY_REQUESTS) {
                    throw ex;
                }
            }
            fail("deleting async query [" + id + "] was rejected");
        });
    }

    private void awaitAsyncQueryGone(String id) throws Exception {
        assertBusy(() -> {
            try {
                client().performRequest(new Request("GET", "/_query/async/" + id));
            } catch (ResponseException ex) {
                int statusCode = ex.getResponse().getStatusLine().getStatusCode();
                if (statusCode == HttpStatus.SC_NOT_FOUND) {
                    return;
                }
                if (statusCode != HttpStatus.SC_TOO_MANY_REQUESTS) {
                    throw ex;
                }
            }
            fail("expected async query [" + id + "] to be gone");
        });
    }
}
