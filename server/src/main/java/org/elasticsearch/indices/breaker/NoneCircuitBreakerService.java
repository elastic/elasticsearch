/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.breaker;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;

import java.util.Collection;
import java.util.Set;

/**
 * Class that returns a breaker that never breaks
 */
public class NoneCircuitBreakerService extends CircuitBreakerService {

    private final CircuitBreaker breaker = new NoopCircuitBreaker(CircuitBreaker.FIELDDATA);

    /**
     * Creates a service that returns a {@link NoopCircuitBreaker} for every name.
     * Used in tests and contexts where all circuit breaking is intentionally disabled.
     */
    public NoneCircuitBreakerService() {
        super();
        this.ownedNames = null;
    }

    /**
     * Creates a service scoped to the given breaker names.
     * {@link #getBreaker(String)} returns a {@link NoopCircuitBreaker} only when
     * {@code name} is in {@code breakerNames}, and {@code null} otherwise.
     * This allows a {@link CompositeCircuitBreakerService} to route unknown names
     * (such as {@code native_memory}) to another delegate instead of silently
     * receiving a noop.
     */
    public NoneCircuitBreakerService(Collection<String> breakerNames) {
        super();
        this.ownedNames = Set.copyOf(breakerNames);
    }

    private final Set<String> ownedNames;

    @Override
    public CircuitBreaker getBreaker(String name) {
        if (ownedNames != null && ownedNames.contains(name) == false) {
            return null;
        }
        return breaker;
    }

    @Override
    public AllCircuitBreakerStats stats() {
        return new AllCircuitBreakerStats(new CircuitBreakerStats[] { stats(CircuitBreaker.FIELDDATA) });
    }

    @Override
    public CircuitBreakerStats stats(String name) {
        if (ownedNames != null && ownedNames.contains(name) == false) {
            return null;
        }
        return new CircuitBreakerStats(CircuitBreaker.FIELDDATA, -1, -1, 0, 0);
    }

}
