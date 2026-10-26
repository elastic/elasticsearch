/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.rest;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

/**
 * Base class for REST tests that send requests to a heap-constrained node and expect the circuit breakers to reject the excess rather
 * than let the node run out of memory. Tests that hold requests in flight do it with the {@code pause} runtime field of the
 * {@code test-pausable-field} module, which the cluster of the subclass then has to load. Each subclass provides its own
 * {@code @ClassRule} cluster, because the heap size is fixed at node startup.
 */
public abstract class HeapPressureTestCase extends ESRestTestCase {

    private static volatile boolean pauseFieldBlocked;

    /** The cluster of the subclass, whose node processes {@link #assertNodeAlive} checks. */
    protected abstract ElasticsearchCluster cluster();

    @Override
    protected String getTestRestCluster() {
        return cluster().getHttpAddresses();
    }

    /** Never leave the field blocked: a blocked script would hang every later search on the node. */
    @After
    public void unblockPausableField() throws IOException {
        if (pauseFieldBlocked) {
            // Cleared first so that the flag never outlives this test, even if the request fails.
            pauseFieldBlocked = false;
            unblockPauseField();
        }
    }

    protected static void blockPauseField() throws IOException {
        pauseFieldBlocked = true;
        // The control requests go through the admin client, which has its own connection pool: with every connection of client() held by a
        // parked search, they would otherwise wait for a connection that only they can free.
        adminClient().performRequest(new Request("POST", "/_pause_field/block"));
    }

    protected static void unblockPauseField() throws IOException {
        adminClient().performRequest(new Request("POST", "/_pause_field/unblock"));
        pauseFieldBlocked = false;
    }

    /**
     * Blocks the pause field like {@link #blockPauseField}, and also makes every query on it wait at an admission gate while it is being
     * built, before the clauses after it are built, until {@link #admitOne} lets it in. With the pause field as the first clause, this
     * serializes building the rest. {@link #unblockPauseField} removes the gate too.
     */
    protected static void stopPauseField() throws IOException {
        pauseFieldBlocked = true;
        adminClient().performRequest(new Request("POST", "/_pause_field/stop"));
    }

    /** Lets one query waiting at the admission gate of {@link #stopPauseField} in. */
    protected static void admitOne() throws IOException {
        adminClient().performRequest(new Request("POST", "/_pause_field/admit"));
    }

    /**
     * Checks that every node process is still running, since {@code -XX:+ExitOnOutOfMemoryError} ends one that ran out of memory, and
     * that the cluster still answers requests. The cluster health API is used because it exists in every distribution, unlike
     * {@code GET /}.
     */
    protected void assertNodeAlive() throws IOException {
        assertTrue("a node exited, most likely on an OutOfMemoryError", nodesAreAlive());
        assertThat(client().performRequest(new Request("GET", "/_cluster/health")).getStatusLine().getStatusCode(), equalTo(200));
    }

    protected boolean nodesAreAlive() {
        for (int node = 0; node < cluster().getNumNodes(); node++) {
            if (ProcessHandle.of(cluster().getPid(node)).map(ProcessHandle::isAlive).orElse(false) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Waits until enough requests overlap: at least {@code minActive} search threads are busy, which with the field blocked
     * means each is holding its memory, or the request breaker has already rejected one. Either shows the requests overlapped, and
     * which of the two happens depends on whether the memory is charged to the breaker: without the charge the pool fills, with it
     * the breaker rejects before the pool does. Fails if neither happens, rather than carrying on without any overlap.
     */
    protected void waitForOverlappingRequests(int minActive) throws Exception {
        assertBusy(() -> {
            Map<String, Object> node = nodeStats();
            Number active = (Number) XContentMapValues.extractValue("thread_pool.search.active", node);
            Number tripped = (Number) XContentMapValues.extractValue("breakers.request.tripped", node);
            assertTrue(
                Strings.format("expected %d active search threads or a tripped request breaker, got %s and %s", minActive, active, tripped),
                active.intValue() >= minActive || tripped.longValue() > 0
            );
        }, 30, TimeUnit.SECONDS);
    }

    /** The memory currently charged to the request breaker of the node, which includes what in-flight searches have retained.
     * This method is only suitable for one-node setups, otherwise it will return data for just one of the nodes.
     */
    protected long requestBreakerUsedBytes() throws IOException {
        return ((Number) XContentMapValues.extractValue("breakers.request.estimated_size_in_bytes", nodeStats())).longValue();
    }

    /** The number of times any circuit breaker of the node has tripped. */
    @SuppressWarnings("unchecked")
    protected long trippedCount() throws IOException {
        long tripped = 0;
        for (Object breaker : ((Map<String, Object>) nodeStats().get("breakers")).values()) {
            tripped += ((Number) ((Map<String, Object>) breaker).get("tripped")).longValue();
        }
        return tripped;
    }

    /** The number of threads currently blocked on the pause field, across all nodes. */
    protected static int parkedCount() throws IOException {
        return ((Number) pauseFieldStatus().get("parked")).intValue();
    }

    /** The number of threads currently waiting at the admission gate of the pause field, across all nodes. */
    protected static int gatedCount() throws IOException {
        return ((Number) pauseFieldStatus().get("gated")).intValue();
    }

    private static Map<String, Object> pauseFieldStatus() throws IOException {
        return entityAsMap(adminClient().performRequest(new Request("GET", "/_pause_field/status")));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> nodeStats() throws IOException {
        Map<String, Object> nodes = (Map<String, Object>) entityAsMap(
            adminClient().performRequest(new Request("GET", "/_nodes/stats/thread_pool,breaker,jvm"))
        ).get("nodes");
        return (Map<String, Object>) nodes.values().iterator().next();
    }

    /**
     * Performs the request and returns the parsed error body, or {@code null} if it succeeded. The entity is read here so that
     * it is consumed on the calling thread before the HTTP connection is returned to the pool.
     */
    protected Map<String, Object> errorBodyOrNull(Request request) throws IOException {
        try {
            client().performRequest(request);
            return null;
        } catch (ResponseException e) {
            return entityAsMap(e.getResponse());
        }
    }

    /**
     * Submits {@code count} identical callables that all start at the same instant via a latch, and returns without waiting for them:
     * they may be held in flight until the caller unblocks the pause field. The pool is shut down before returning so that its threads
     * go away once the tasks finish.
     */
    protected <T> List<Future<T>> submit(int count, Callable<T> task) {
        ExecutorService pool = Executors.newFixedThreadPool(count);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<T>> futures = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return task.call();
                }));
            }
            start.countDown();
            return futures;
        } finally {
            pool.shutdown();
        }
    }

    /**
     * Performs the search and returns how many of its shards were rejected by a circuit breaker: none if it fully succeeded, the shards
     * that failed if it returned partial results, and all of them if every shard failed. Fails the test if any failure isn't a circuit
     * breaker rejection. Partial results are the default, so a breaker rejecting some shards of a search doesn't fail the search.
     *
     * @param shardCount the number of shards the search targets, which is what a search that failed on all of them counts
     */
    protected int circuitBreakerRejectedShards(Request search, int shardCount) throws IOException {
        Map<String, Object> body;
        try {
            body = entityAsMap(client().performRequest(search));
        } catch (ResponseException e) {
            Map<String, Object> error = entityAsMap(e.getResponse());
            // Failed on all shards: the failures are grouped by cause, so check them all but count the shards searched.
            assertTrue("expected only circuit_breaking_exception failures, but got: " + error, containsCircuitBreakingException(error));
            List<?> failedShards = (List<?>) XContentMapValues.extractValue("error.failed_shards", error);
            if (failedShards != null) {
                for (Object failure : failedShards) {
                    assertTrue(
                        "expected only circuit_breaking_exception failures, but got: " + failure,
                        containsCircuitBreakingException(failure)
                    );
                }
            }
            return shardCount;
        }
        List<?> failures = (List<?>) XContentMapValues.extractValue("_shards.failures", body);
        if (failures != null) {
            for (Object failure : failures) {
                assertTrue(
                    "expected only circuit_breaking_exception failures, but got: " + failure,
                    containsCircuitBreakingException(failure)
                );
            }
        }
        return ((Number) XContentMapValues.extractValue("_shards.failed", body)).intValue();
    }

    /** The breaker error is nested at a depth that depends on the failing phase, so search the whole body. */
    protected static boolean containsCircuitBreakingException(Object obj) {
        return containsCircuitBreakingException(obj, null);
    }

    /**
     * Like {@link #containsCircuitBreakingException(Object)}, but only matches an error whose reason contains {@code reasonFragment},
     * such as the label of the charge that tripped. {@code null} matches any reason.
     */
    protected static boolean containsCircuitBreakingException(Object obj, String reasonFragment) {
        if (obj instanceof Map<?, ?> map) {
            if ("circuit_breaking_exception".equals(map.get("type"))
                && (reasonFragment == null || (map.get("reason") instanceof String reason && reason.contains(reasonFragment)))) {
                return true;
            }
            for (Object value : map.values()) {
                if (containsCircuitBreakingException(value, reasonFragment)) {
                    return true;
                }
            }
        } else if (obj instanceof List<?> list) {
            for (Object item : list) {
                if (containsCircuitBreakingException(item, reasonFragment)) {
                    return true;
                }
            }
        }
        return false;
    }
}
