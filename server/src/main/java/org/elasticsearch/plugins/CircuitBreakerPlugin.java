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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;

import java.util.Collections;
import java.util.List;

/**
 * An extension point for {@link Plugin} implementations to add custom circuit breakers
 */
public interface CircuitBreakerPlugin {

    /**
     * Returns additional circuit breaker settings added by this plugin.
     *
     * This each of the {@link BreakerSettings} are passed to the configured {@link CircuitBreakerService}.
     * The service will create a new breaker according to the provided settings and overall environment.
     *
     * Custom circuit breakers settings should adhere to the affix settings described in {@link BreakerSettings}.
     * See:
     *  - limit (example: `breaker.foo.limit`) {@link BreakerSettings#CIRCUIT_BREAKER_LIMIT_SETTING}
     *  - overhead (example: `breaker.foo.overhead`) {@link BreakerSettings#CIRCUIT_BREAKER_OVERHEAD_SETTING}
     *  - type (example: `breaker.foo.type`) {@link BreakerSettings#CIRCUIT_BREAKER_TYPE}
     *
     * The `limit` and `overhead` settings will be dynamically updated in the circuit breaker service iff a {@link BreakerSettings}
     * object with the same name is provided at node startup.
     *
     * @param settings The current settings.
     *                 Should be used to construct the {@link BreakerSettings} objects.
     *                 See {@link BreakerSettings#fromSettings(String,
     *                                                         Settings,
     *                                                         String,
     *                                                         double,
     *                                                         CircuitBreaker.Type,
     *                                                         CircuitBreaker.Durability)} }
     */
    default List<BreakerSettings> getCircuitBreakers(Settings settings) {
        return Collections.emptyList();
    }

}
