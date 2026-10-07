/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.heappressure;

import org.elasticsearch.client.Request;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import static org.hamcrest.Matchers.equalTo;

/**
 * Wildcard queries on a {@code wildcard} field must be accounted by the request circuit breaker.
 *
 * <p>A wildcard pattern is bounded by the fixed determinization work limit (10,000), so a single one allocates about 50 MB and
 * retains about 10 MB per shard. That cannot exhaust a normal heap, so the cluster runs with a deliberately small one, where the
 * automatons of a few overlapping requests do. Charged to the breaker, the excess requests are rejected; uncharged, the node runs
 * out of memory.
 */
public class WildcardFieldWildcardHeapPressureIT extends WildcardFieldHeapPressureTestCase {

    // Kept below the REST client's default of 10 connections per route, so that the status polling and the unblock request that run
    // while these are parked can still get a connection. Five shards each, this is more than enough to occupy the search pool.
    private static final int THREAD_COUNT = 24;
    // '*a' followed by N '?' determinizes to 2^N states: 8192 for N=11, the most the default 10,000 work limit accepts.
    private static final String HEAVY_WILDCARD = "*a???????????*";
    private static final String SMALL_WILDCARD = "*a????*";

    /**
     * Heavy requests held in flight at the same time either complete or are rejected by the breaker, never anything else, and the node
     * survives them. The pausable field blocks each request after its automaton is built, so the overlap is guaranteed rather than a
     * matter of timing: every thread of the search pool ends up holding one. How many requests are rejected depends on how the builds
     * overlap, but with a 77 MB request breaker (60% of the heap) and about 70 MB reserved per automaton build, some must be.
     */
    public void testPausedHeavyWildcardsTripBreaker() throws Exception {
        /* A pattern that fits comfortably under the breaker is unaffected. */
        assertNull(errorBodyOrNull(wildcardSearch(SMALL_WILDCARD)));

        List<Future<Map<String, Object>>> futures;
        blockPauseField();
        try {
            futures = submit(THREAD_COUNT, () -> errorBodyOrNull(pausableWildcardSearch(HEAVY_WILDCARD)));
            waitForSearchPoolToFill();
        } finally {
            unblockPauseField();
        }
        int rejected = 0;
        for (Future<Map<String, Object>> future : futures) {
            Map<String, Object> body = future.get();
            if (body != null) {
                assertTrue("expected only circuit_breaking_exception failures, but got: " + body, containsCircuitBreakingException(body));
                rejected++;
            }
        }
        assertThat("expected the request breaker to reject some of the overlapping requests", rejected > 0, equalTo(true));
        // Check that the node didn't OOM and is still alive.
        assertThat(client().performRequest(new Request("GET", "/")).getStatusLine().getStatusCode(), equalTo(200));
    }
}
