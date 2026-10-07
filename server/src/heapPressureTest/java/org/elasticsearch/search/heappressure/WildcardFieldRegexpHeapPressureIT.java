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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import static org.hamcrest.Matchers.equalTo;

/**
 * Regexp queries on a {@code wildcard} field must be rejected by the request circuit breaker instead of exhausting the heap.
 *
 * <p>The pattern {@code [ac]*a[ac]{18}} determinizes to about 524k DFA states, and {@code max_determinized_states} is chosen by the
 * caller, so nothing but the circuit breaker bounds the work. The cluster runs with a 512 MB heap, where a handful of concurrent
 * requests would otherwise outgrow it. The automaton is built on every shard, at query parse time and again when the
 * {@code BinaryDvConfirmedQuery} weight is created, so both construction sites must account for it.
 *
 * <p>Concurrent requests are used because each one allocates hundreds of MB while it determinizes; it is the overlap that
 * turns an unaccounted allocation into an {@link OutOfMemoryError}.
 */
public class WildcardFieldRegexpHeapPressureIT extends WildcardFieldHeapPressureTestCase {

    private static final int THREAD_COUNT = 8;
    // 2^19 DFA states: far beyond what a 512 MB heap tolerates once several requests overlap.
    private static final String HEAVY_REGEXP = "[ac]*a[ac]{18}";
    private static final int MAX_DETERMINIZED_STATES = 1_000_000;

    /** A pattern that fits comfortably under the breaker is unaffected. */
    public void testSmallRegexpSucceeds() throws IOException {
        assertNull(errorBodyOrNull(regexpSearch("[ac]*a[ac]{4}", MAX_DETERMINIZED_STATES)));
    }

    /** Every concurrent heavy request is rejected by the breaker and the node survives them. */
    public void testConcurrentHeavyRegexpTripsBreaker() throws Exception {
        List<Future<Map<String, Object>>> futures = submit(
            THREAD_COUNT,
            () -> errorBodyOrNull(regexpSearch(HEAVY_REGEXP, MAX_DETERMINIZED_STATES))
        );
        for (int i = 0; i < futures.size(); i++) {
            Map<String, Object> body = futures.get(i).get();
            assertNotNull("thread " + i + " expected the circuit breaker to fire but the search succeeded", body);
            assertTrue("thread " + i + " expected a circuit_breaking_exception, but got: " + body, containsCircuitBreakingException(body));
        }
        // An out-of-memory node would not answer.
        assertThat(client().performRequest(new Request("GET", "/")).getStatusLine().getStatusCode(), equalTo(200));
    }
}
