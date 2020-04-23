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

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * An extension point for {@link Plugin} implementations to add custom circuit breakers
 */
public interface CircuitMemoryBreakerPlugin {

    /**
     * Returns additional circuit breaker settings added by this plugin.
     *
     * This each of the {@link BreakerSettings} are passed to the configured {@link CircuitBreakerService}.
     * The service will create a new breaker according to the provided settings and overall environment.
     *
     */
    default List<BreakerSettings> getCircuitBreakers() {
        return Collections.emptyList();
    }

    /**
     * If the custom {@link BreakerSettings} have dynamic settings.
     * A settings update consumer can be added with this method.
     * Once the new settings are constructed, pass them to the updatedBreakerSettingsListener.
     */
    default void addDynamicBreakerUpdates(ClusterSettings settings, Consumer<BreakerSettings> updatedBreakerSettingsListener) {
    }
}
