/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.wildcard.heappressure;

import org.elasticsearch.client.Request;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * Regexp queries on a {@code wildcard} field must be rejected by the request circuit breaker instead of exhausting the heap.
 *
 * <p>The pattern {@code [ac]*a[ac]{18}} determinizes to about 524k DFA states, and {@code max_determinized_states} is chosen by the
 * caller, so nothing but the circuit breaker bounds the work. The automaton is built on every shard, so one request builds five of
 * them at once, which the small heap of the cluster does not survive unless the breaker rejects it first.
 */
public class WildcardFieldRegexpHeapPressureIT extends WildcardFieldHeapPressureTestCase {

    // 2^19 DFA states: a single request is rejected, and without the breaker it is meant to run the node out of memory.
    private static final String HEAVY_REGEXP = "[ac]*a[ac]{18}";
    private static final int MAX_DETERMINIZED_STATES = 1_000_000;

    public void testHeavyRegexpTripsBreaker() throws Exception {
        /* A pattern that fits comfortably under the breaker is unaffected. */
        assertNull(errorBodyOrNull(regexpSearch("[ac]*a[ac]{4}", MAX_DETERMINIZED_STATES)));

        Map<String, Object> body = errorBodyOrNull(regexpSearch(HEAVY_REGEXP, MAX_DETERMINIZED_STATES));
        assertNotNull("expected the circuit breaker to fire but the search succeeded", body);
        assertTrue("expected a circuit_breaking_exception, but got: " + body, containsCircuitBreakingException(body));
        // An out-of-memory node would not answer.
        assertThat(client().performRequest(new Request("GET", "/")).getStatusLine().getStatusCode(), equalTo(200));
    }
}
