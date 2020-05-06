/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;

import java.util.List;
import java.util.function.Function;

/**
 * An extension point for {@link Plugin} implementations to add custom circuit breakers
 */
public interface CircuitBreakerPlugin {

    /**
     * Each of the {@link CircuitBreaker} objects are passed to the configured {@link CircuitBreakerService}.
     *
     * Custom circuit breakers settings can be found in {@link BreakerSettings}.
     * See:
     *  - limit (example: `breaker.foo.limit`) {@link BreakerSettings#CIRCUIT_BREAKER_LIMIT_SETTING}
     *  - overhead (example: `breaker.foo.overhead`) {@link BreakerSettings#CIRCUIT_BREAKER_OVERHEAD_SETTING}
     *  - type (example: `breaker.foo.type`) {@link BreakerSettings#CIRCUIT_BREAKER_TYPE}
     *
     * The `limit` and `overhead` settings will be dynamically updated in the circuit breaker service iff a {@link BreakerSettings}
     * object with the same name is provided at node startup.
     *
     * @param circuitBreakerFactory A factory function that will take the provided BreakerSettings and construct a new circuit breaker
     *                              The constructed circuitBreaker applies any overridden settings.
     */
    List<CircuitBreaker> getCircuitBreakers(Function<BreakerSettings, CircuitBreaker> circuitBreakerFactory);

}
