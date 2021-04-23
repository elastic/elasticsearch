/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.breaker;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;

/**
 * Class that returns a breaker that never breaks
 */
public class NoneCircuitBreakerService extends CircuitBreakerService {

    private final CircuitBreaker breaker = new NoopCircuitBreaker(CircuitBreaker.FIELDDATA);

    public NoneCircuitBreakerService() {
        super();
    }

    @Override
    public CircuitBreaker getBreaker(String name) {
        return breaker;
    }

    @Override
    public AllCircuitBreakerStats stats() {
        return new AllCircuitBreakerStats(new CircuitBreakerStats[] {stats(CircuitBreaker.FIELDDATA)});
    }

    @Override
    public CircuitBreakerStats stats(String name) {
        return new CircuitBreakerStats(CircuitBreaker.FIELDDATA, -1, -1, 0, 0);
    }

}
