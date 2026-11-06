/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.breaker.CircuitBreaker;

/**
 * A breaker that accounts into another breaker: the per-driver {@link LocalCircuitBreaker}, and the per-query breaker and
 * its views above it. A child block factory's blocks may end up released through the parent factory's breaker, so the
 * factory checks that the two breakers keep one account, through {@link #sharesAccountWith}, without knowing which layers
 * sit between them.
 */
public interface DelegatingCircuitBreaker extends CircuitBreaker {

    /**
     * The breaker this one charges.
     */
    CircuitBreaker parentBreaker();

    /**
     * True when a byte charged through this breaker may be released through {@code other} without the two accounts
     * drifting apart. Identity by default: a thread-pinned local breaker keeps its own reserve, so no other breaker may
     * release on its behalf. Views of one query breaker override this, since they share the query's counter.
     */
    default boolean sharesAccountWith(CircuitBreaker other) {
        return this == other;
    }
}
