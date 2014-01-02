/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.fielddata.breaker;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.breaker.MemoryCircuitBreaker;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.node.settings.NodeSettingsService;

/**
 * The InternalCircuitBreakerService handles providing
 * {@link org.elasticsearch.common.breaker.MemoryCircuitBreaker}s
 * that can be used to keep track of memory usage across the node, preventing
 * actions that could cause an {@link OutOfMemoryError} on the node.
 */
public class InternalCircuitBreakerService extends AbstractLifecycleComponent<InternalCircuitBreakerService> implements CircuitBreakerService {

    public static final String CIRCUIT_BREAKER_MAX_BYTES_SETTING = "indices.fielddata.breaker.limit";
    public static final String CIRCUIT_BREAKER_OVERHEAD_SETTING = "indices.fielddata.breaker.overhead";

    public static final double DEFAULT_OVERHEAD_CONSTANT = 1.03;

    private volatile MemoryCircuitBreaker breaker;
    private volatile long maxBytes;
    private volatile double overhead;

    @Inject
    public InternalCircuitBreakerService(Settings settings, NodeSettingsService nodeSettingsService, IndicesFieldDataCache fieldDataCache) {
        super(settings);
        long fieldDataMax = fieldDataCache.computeSizeInBytes();
        this.maxBytes = settings.getAsBytesSize(CIRCUIT_BREAKER_MAX_BYTES_SETTING, new ByteSizeValue(fieldDataMax)).bytes();
        this.overhead = settings.getAsDouble(CIRCUIT_BREAKER_OVERHEAD_SETTING, DEFAULT_OVERHEAD_CONSTANT);

        this.breaker = new MemoryCircuitBreaker(new ByteSizeValue(maxBytes), overhead, null, logger);

        nodeSettingsService.addListener(new ApplySettings());
    }

    class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            // clear breaker now that settings have changed
            ByteSizeValue newMaxByteSizeValue = settings.getAsBytesSize(CIRCUIT_BREAKER_MAX_BYTES_SETTING, null);
            boolean breakerResetNeeded = false;

            if (newMaxByteSizeValue != null) {
                logger.info("updating [{}] from [{}] to [{}]", CIRCUIT_BREAKER_MAX_BYTES_SETTING,
                        new ByteSizeValue(InternalCircuitBreakerService.this.maxBytes), newMaxByteSizeValue);
                InternalCircuitBreakerService.this.maxBytes = newMaxByteSizeValue.bytes();
                breakerResetNeeded = true;
            }

            double newOverhead = settings.getAsDouble(CIRCUIT_BREAKER_OVERHEAD_SETTING, overhead);
            if (newOverhead != overhead) {
                logger.info("updating [{}] from [{}] to [{}]", CIRCUIT_BREAKER_OVERHEAD_SETTING,
                        overhead, newOverhead);
                InternalCircuitBreakerService.this.overhead = newOverhead;
                breakerResetNeeded = true;
            }

            if (breakerResetNeeded) {
                resetBreaker();
            }
        }
    }

    /**
     * @return a {@link org.elasticsearch.common.breaker.MemoryCircuitBreaker} that can be used for aggregating memory usage
     */
    public MemoryCircuitBreaker getBreaker() {
        return this.breaker;
    }

    /**
     * Reset the breaker, creating a new one and initializing its used value
     * to the actual field data usage, or the existing estimated usage if the
     * actual value is not available. Will not trip the breaker even if the
     * used value is higher than the limit for the breaker.
     */
    public synchronized void resetBreaker() {
        final MemoryCircuitBreaker oldBreaker = this.breaker;
        // discard old breaker by creating a new one and pre-populating from the current breaker
        this.breaker = new MemoryCircuitBreaker(new ByteSizeValue(maxBytes), overhead, oldBreaker, logger);
    }

    @Override
    public FieldDataBreakerStats stats() {
        return new FieldDataBreakerStats(breaker.getMaximum(), breaker.getUsed(), breaker.getOverhead());
    }

    @Override
    protected void doStart() throws ElasticSearchException {
    }

    @Override
    protected void doStop() throws ElasticSearchException {
    }

    @Override
    protected void doClose() throws ElasticSearchException {
    }
}
