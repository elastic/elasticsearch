/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.breaker;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;

/**
 * Interface for Circuit Breaker services, which provide breakers to classes
 * that load field data.
 */
public abstract class CircuitBreakerService extends AbstractLifecycleComponent<CircuitBreakerService> {

    protected CircuitBreakerService(Settings settings) {
        super(settings);
    }

    /**
     * Allows to register of a custom circuit breaker.
     */
    public abstract void registerBreaker(BreakerSettings breakerSettings);

    /**
     * @return the breaker that can be used to register estimates against
     */
    public abstract CircuitBreaker getBreaker(String name);

    /**
     * @return stats about all breakers
     */
    public abstract AllCircuitBreakerStats stats();

    /**
     * @return stats about a specific breaker
     */
    public abstract CircuitBreakerStats stats(String name);

    @Override
    protected void doStart() {
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() {
    }

}
