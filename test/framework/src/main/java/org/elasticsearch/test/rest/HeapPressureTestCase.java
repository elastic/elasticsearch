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
 * Base class for REST tests that overlap many requests on a heap-constrained node and expect the circuit breakers to reject the excess
 * rather than let the node run out of memory. Requests are held in flight with the {@code pause} runtime field of the
 * {@code test-pausable-field} module, which the cluster of the subclass has to load. Each subclass provides its own {@code @ClassRule}
 * cluster, because the heap size is fixed at node startup.
 */
public abstract class HeapPressureTestCase extends ESRestTestCase {

    /** Never leave the field blocked: a blocked script would hang every later search on the node. */
    @After
    public void unblockPausableField() throws IOException {
        unblockPauseField();
    }

    protected static void blockPauseField() throws IOException {
        // The control requests go through the admin client, which has its own connection pool: with every connection of client() held by a
        // parked search, they would otherwise wait for a connection that only they can free.
        adminClient().performRequest(new Request("POST", "/_pause_field/block"));
    }

    protected static void unblockPauseField() throws IOException {
        adminClient().performRequest(new Request("POST", "/_pause_field/unblock"));
    }

    /**
     * Checks that the node still answers requests, which one that ran out of memory would not. The cluster health API is used because
     * it exists in every distribution, unlike {@code GET /}.
     */
    protected void assertNodeAlive() throws IOException {
        assertThat(client().performRequest(new Request("GET", "/_cluster/health")).getStatusLine().getStatusCode(), equalTo(200));
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

    /** The breaker error is nested at a depth that depends on the failing phase, so search the whole body. */
    protected static boolean containsCircuitBreakingException(Object obj) {
        if (obj instanceof Map<?, ?> map) {
            if ("circuit_breaking_exception".equals(map.get("type"))) {
                return true;
            }
            for (Object value : map.values()) {
                if (containsCircuitBreakingException(value)) {
                    return true;
                }
            }
        } else if (obj instanceof List<?> list) {
            for (Object item : list) {
                if (containsCircuitBreakingException(item)) {
                    return true;
                }
            }
        }
        return false;
    }
}
