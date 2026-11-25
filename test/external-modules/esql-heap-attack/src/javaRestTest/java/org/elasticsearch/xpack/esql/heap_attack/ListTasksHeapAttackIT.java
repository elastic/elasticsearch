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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Lists the tasks of a cluster whose other nodes hold many queries with long descriptions, from many clients at once, through a node that
 * holds none of them. The coordinating node holds the replies of all the other nodes until it has them, so it must trip a circuit breaker
 * and fail to take those replies. Without a breaker on the replies it receives, the same load ran nodes of the cluster out of memory.
 */
public class ListTasksHeapAttackIT extends HeapAttackRestHelpers {

    private static final int NODES = 5;
    private static final int LITERAL_LENGTH = 900_000;
    private static final int HELD_QUERIES = 24;
    private static final int CLIENTS = 8;
    private static final int CALLS_PER_CLIENT = 10;

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.buildClusterSpec().nodes(NODES).build();

    @Override
    protected String getTestRestCluster() {
        String[] addresses = Clusters.testRestCluster(cluster).split(",");
        return String.join(",", Arrays.copyOfRange(addresses, 1, addresses.length));
    }

    public void testListingTheTasksOfManyNodesDoesNotExhaustTheHeapOfTheCoordinatingNode() throws Exception {
        createPausableIndex();
        blockPauseField();
        List<String> asyncIds = new ArrayList<>();
        String coordinatingAddress = cluster.getHttpAddresses().split(",")[0];
        int portSeparator = coordinatingAddress.lastIndexOf(':');
        String host = coordinatingAddress.substring(0, portSeparator).replace("[", "").replace("]", "");
        int port = Integer.parseInt(coordinatingAddress.substring(portSeparator + 1));
        ExecutorService executor = Executors.newFixedThreadPool(CLIENTS);
        try (RestClient coordinatingClient = buildClient(restClientSettings(), new HttpHost[] { new HttpHost(host, port, "http") })) {
            for (int i = 0; i < HELD_QUERIES; i++) {
                asyncIds.add(submitAsyncQuery(queryWithLongLiteral("q" + i + "-" + "€".repeat(LITERAL_LENGTH))));
            }
            logger.info("--> submitted {} held queries", HELD_QUERIES);
            AtomicInteger listed = new AtomicInteger();
            AtomicInteger tripped = new AtomicInteger();
            AtomicInteger failed = new AtomicInteger();
            List<Future<?>> calls = new ArrayList<>();
            for (int c = 0; c < CLIENTS; c++) {
                calls.add(executor.submit(() -> {
                    for (int call = 0; call < CALLS_PER_CLIENT; call++) {
                        try {
                            if (listTasksTrips(coordinatingClient)) {
                                tripped.incrementAndGet();
                            } else {
                                listed.incrementAndGet();
                            }
                        } catch (IOException e) {
                            logger.info("--> listing failed: {}", e.toString());
                            failed.incrementAndGet();
                        }
                    }
                    return null;
                }));
            }
            for (Future<?> call : calls) {
                call.get(10, TimeUnit.MINUTES);
            }
            logger.info("--> listed={} tripped={} failed={}", listed.get(), tripped.get(), failed.get());
            assertThat("listings that failed because the coordinating node is gone or in trouble", failed.get(), equalTo(0));
            Request health = new Request("GET", "/_cluster/health");
            health.addParameter("filter_path", "number_of_nodes");
            assertThat(
                "nodes left in the cluster, some ran out of memory",
                entityAsMap(client().performRequest(health)).get("number_of_nodes"),
                equalTo(NODES)
            );
            assertThat("listings that tripped a circuit breaker", tripped.get(), greaterThan(0));
        } finally {
            executor.shutdownNow();
            unblockPauseField();
            for (String id : asyncIds) {
                deleteAsyncQuery(id);
            }
            for (String id : asyncIds) {
                ensureAsyncQueryCompleted(id);
            }
        }
    }

    private boolean listTasksTrips(RestClient coordinatingClient) throws IOException {
        Request request = new Request("GET", "/_tasks");
        request.addParameter("detailed", "true");
        request.addParameter("actions", "*esql*");
        request.addParameter("filter_path", "node_failures");
        try {
            Map<String, Object> response = entityAsMap(coordinatingClient.performRequest(request));
            List<?> nodeFailures = (List<?>) response.get("node_failures");
            return nodeFailures != null
                && nodeFailures.stream()
                    .anyMatch(f -> "circuit_breaking_exception".equals(XContentMapValues.extractValue("caused_by.type", (Map<?, ?>) f)));
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == HttpStatus.SC_TOO_MANY_REQUESTS) {
                return true;
            }
            throw e;
        }
    }

    private void createPausableIndex() throws IOException {
        Request createIndex = new Request("PUT", "/pause_test");
        createIndex.setJsonEntity("""
            {
                "settings": {
                    "number_of_shards": 1,
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
