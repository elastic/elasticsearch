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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Lists the tasks of a cluster whose other nodes hold many queries with long descriptions, from many clients at once, through a node that
 * holds none of them. The coordinating node holds the replies of all the other nodes until it has them, so it must trip a circuit breaker
 * and fail to take those replies. Without a breaker on the replies it receives, the same load ran nodes of the cluster out of memory.
 */
public class ListTasksHeapAttackIT extends ListTasksHeapAttackTestCase {

    private static final int NODES = 5;
    private static final int HELD_QUERIES = 24;
    private static final int CLIENTS = 8;
    private static final int CALLS_PER_CLIENT = 10;

    @ClassRule
    public static ElasticsearchCluster cluster = buildCluster();

    private static ElasticsearchCluster buildCluster() {
        try {
            return Clusters.buildClusterSpec().nodes(NODES).build();
        } catch (UnsupportedOperationException e) {
            return Clusters.buildCluster();
        }
    }

    @Override
    protected ElasticsearchCluster testCluster() {
        return cluster;
    }

    public void testListingTheTasksOfManyNodesDoesNotExhaustTheHeapOfTheCoordinatingNode() throws Exception {
        assumeFalse("serverless does not allow changing the number of nodes", isServerless());
        List<String> asyncIds = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(CLIENTS);
        try (RestClient coordinatingClient = buildCoordinatingClient()) {
            createPausableIndex(1, coordinatingNodeId(coordinatingClient));
            blockPauseField();
            try {
                final long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(3);
                for (int i = 0; i < HELD_QUERIES; i++) {
                    asyncIds.add(submitHeldQuery(i));
                }
                logger.info("--> submitted {} held queries", HELD_QUERIES);
                AtomicInteger listed = new AtomicInteger();
                AtomicInteger tripped = new AtomicInteger();
                AtomicInteger failed = new AtomicInteger();
                Queue<String> unexpectedNodeFailures = new ConcurrentLinkedQueue<>();
                List<Future<?>> calls = new ArrayList<>();
                for (int c = 0; c < CLIENTS; c++) {
                    calls.add(executor.submit(() -> {
                        for (int call = 0; call < CALLS_PER_CLIENT; call++) {
                            try {
                                if (listTasksTrips(coordinatingClient, unexpectedNodeFailures)) {
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
                    call.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                }
                logger.info("--> listed={} tripped={} failed={}", listed.get(), tripped.get(), failed.get());
                assertThat("listings that failed because the coordinating node is gone or in trouble", failed.get(), equalTo(0));
                assertThat("node failures other than a trip of the breaker on the replies", unexpectedNodeFailures, empty());
                Request health = new Request("GET", "/_cluster/health");
                health.addParameter("filter_path", "number_of_nodes");
                assertThat(
                    "nodes left in the cluster, some ran out of memory",
                    entityAsMap(client().performRequest(health)).get("number_of_nodes"),
                    equalTo(NODES)
                );
                assertThat("listings that tripped the breaker on the replies", tripped.get(), greaterThan(0));
            } catch (Exception | Error e) {
                try {
                    cleanUp(asyncIds);
                } catch (Exception cleanUpFailure) {
                    e.addSuppressed(cleanUpFailure);
                }
                throw e;
            }
            cleanUp(asyncIds);
        } finally {
            executor.shutdownNow();
        }
    }

    private boolean listTasksTrips(RestClient coordinatingClient, Queue<String> unexpectedNodeFailures) throws IOException {
        Request request = new Request("GET", "/_tasks");
        request.addParameter("detailed", "true");
        request.addParameter("actions", "*esql*");
        request.addParameter("filter_path", "node_failures");
        Map<String, Object> response = entityAsMap(coordinatingClient.performRequest(request));
        List<?> nodeFailures = (List<?>) response.get("node_failures");
        if (nodeFailures == null) {
            return false;
        }
        boolean tripped = false;
        for (Object failure : nodeFailures) {
            Map<?, ?> cause = (Map<?, ?>) XContentMapValues.extractValue("caused_by", (Map<?, ?>) failure);
            if (cause != null
                && "circuit_breaking_exception".equals(cause.get("type"))
                && String.valueOf(cause.get("reason")).contains("<node_tasks_response>")) {
                tripped = true;
            } else {
                unexpectedNodeFailures.add(failure.toString());
            }
        }
        return tripped;
    }
}
