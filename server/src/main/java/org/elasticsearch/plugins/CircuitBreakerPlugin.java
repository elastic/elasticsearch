/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;

/**
 * An extension point for {@link Plugin} implementations to add custom circuit breakers
 */
public interface CircuitBreakerPlugin {

    /**
     * Each of the factory functions are passed to the configured {@link CircuitBreakerService}.
     *
     * The service then constructs a {@link CircuitBreaker} given the resulting {@link BreakerSettings}.
     *
     * Custom circuit breakers settings can be found in {@link BreakerSettings}.
     * See:
     *  - limit (example: `breaker.foo.limit`) {@link BreakerSettings#CIRCUIT_BREAKER_LIMIT_SETTING}
     *  - overhead (example: `breaker.foo.overhead`) {@link BreakerSettings#CIRCUIT_BREAKER_OVERHEAD_SETTING}
     *  - type (example: `breaker.foo.type`) {@link BreakerSettings#CIRCUIT_BREAKER_TYPE}
     *
     * The `limit` and `overhead` settings will be dynamically updated in the circuit breaker service iff a {@link BreakerSettings}
     * object with the same name is provided at node startup.
     */
    BreakerSettings getCircuitBreaker(Settings settings);

    /**
     * The passed {@link CircuitBreaker} object is the same one that was constructed by the {@link BreakerSettings}
     * provided by {@link CircuitBreakerPlugin#getCircuitBreaker(Settings)}.
     *
     * This reference should never change throughout the lifetime of the node.
     *
     * @param circuitBreaker The constructed {@link CircuitBreaker} object from the {@link BreakerSettings}
     *                       provided by {@link CircuitBreakerPlugin#getCircuitBreaker(Settings)}
     */
    void setCircuitBreaker(CircuitBreaker circuitBreaker);
}
