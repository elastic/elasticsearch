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

import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * CircuitBreakerService that attempts to redistribute space between breakers
 * if tripped
 */
public class HierarchyCircuitBreakerService extends CircuitBreakerService {

    private final ConcurrentMap<String, CircuitBreaker> breakers = new ConcurrentHashMap();

    // Old pre-1.4.0 backwards compatible settings
    public static final String OLD_CIRCUIT_BREAKER_MAX_BYTES_SETTING = "indices.fielddata.breaker.limit";
    public static final String OLD_CIRCUIT_BREAKER_OVERHEAD_SETTING = "indices.fielddata.breaker.overhead";

    public static final String TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING = "indices.breaker.total.limit";
    public static final String DEFAULT_TOTAL_CIRCUIT_BREAKER_LIMIT = "70%";

    public static final String FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING = "indices.breaker.fielddata.limit";
    public static final String FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING = "indices.breaker.fielddata.overhead";
    public static final String FIELDDATA_CIRCUIT_BREAKER_TYPE_SETTING = "indices.breaker.fielddata.type";
    public static final String DEFAULT_FIELDDATA_BREAKER_LIMIT = "60%";
    public static final double DEFAULT_FIELDDATA_OVERHEAD_CONSTANT = 1.03;

    public static final String REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING = "indices.breaker.request.limit";
    public static final String REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING = "indices.breaker.request.overhead";
    public static final String REQUEST_CIRCUIT_BREAKER_TYPE_SETTING = "indices.breaker.request.type";
    public static final String DEFAULT_REQUEST_BREAKER_LIMIT = "40%";

    public static final String DEFAULT_BREAKER_TYPE = "memory";

    private volatile BreakerSettings parentSettings;
    private volatile BreakerSettings fielddataSettings;
    private volatile BreakerSettings requestSettings;

    // Tripped count for when redistribution was attempted but wasn't successful
    private final AtomicLong parentTripCount = new AtomicLong(0);

    @Inject
    public HierarchyCircuitBreakerService(Settings settings, NodeSettingsService nodeSettingsService) {
        super(settings);

        // This uses the old InternalCircuitBreakerService.CIRCUIT_BREAKER_MAX_BYTES_SETTING
        // setting to keep backwards compatibility with 1.3, it can be safely
        // removed when compatibility with 1.3 is no longer needed
        String compatibilityFielddataLimitDefault = DEFAULT_FIELDDATA_BREAKER_LIMIT;
        ByteSizeValue compatibilityFielddataLimit = settings.getAsMemory(OLD_CIRCUIT_BREAKER_MAX_BYTES_SETTING, null);
        if (compatibilityFielddataLimit != null) {
            compatibilityFielddataLimitDefault = compatibilityFielddataLimit.toString();
        }

        // This uses the old InternalCircuitBreakerService.CIRCUIT_BREAKER_OVERHEAD_SETTING
        // setting to keep backwards compatibility with 1.3, it can be safely
        // removed when compatibility with 1.3 is no longer needed
        double compatibilityFielddataOverheadDefault = DEFAULT_FIELDDATA_OVERHEAD_CONSTANT;
        Double compatibilityFielddataOverhead = settings.getAsDouble(OLD_CIRCUIT_BREAKER_OVERHEAD_SETTING, null);
        if (compatibilityFielddataOverhead != null) {
            compatibilityFielddataOverheadDefault = compatibilityFielddataOverhead;
        }

        this.fielddataSettings = new BreakerSettings(CircuitBreaker.FIELDDATA,
                settings.getAsMemory(FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING, compatibilityFielddataLimitDefault).bytes(),
                settings.getAsDouble(FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING, compatibilityFielddataOverheadDefault),
                CircuitBreaker.Type.parseValue(settings.get(FIELDDATA_CIRCUIT_BREAKER_TYPE_SETTING, DEFAULT_BREAKER_TYPE))
        );

        this.requestSettings = new BreakerSettings(CircuitBreaker.REQUEST,
                settings.getAsMemory(REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING, DEFAULT_REQUEST_BREAKER_LIMIT).bytes(),
                settings.getAsDouble(REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING, 1.0),
                CircuitBreaker.Type.parseValue(settings.get(REQUEST_CIRCUIT_BREAKER_TYPE_SETTING, DEFAULT_BREAKER_TYPE))
        );

        this.parentSettings = new BreakerSettings(CircuitBreaker.PARENT,
                settings.getAsMemory(TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING, DEFAULT_TOTAL_CIRCUIT_BREAKER_LIMIT).bytes(), 1.0, CircuitBreaker.Type.PARENT);
        if (logger.isTraceEnabled()) {
            logger.trace("parent circuit breaker with settings {}", this.parentSettings);
        }

        registerBreaker(this.requestSettings);
        registerBreaker(this.fielddataSettings);

        nodeSettingsService.addListener(new ApplySettings());
    }

    public class ApplySettings implements NodeSettingsService.Listener {

        @Override
        public void onRefreshSettings(Settings settings) {

            // Fielddata settings
            ByteSizeValue newFielddataMax = settings.getAsMemory(FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING, null);
            Double newFielddataOverhead = settings.getAsDouble(FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING, null);
            if (newFielddataMax != null || newFielddataOverhead != null) {
                long newFielddataLimitBytes = newFielddataMax == null ? HierarchyCircuitBreakerService.this.fielddataSettings.getLimit() : newFielddataMax.bytes();
                newFielddataOverhead = newFielddataOverhead == null ? HierarchyCircuitBreakerService.this.fielddataSettings.getOverhead() : newFielddataOverhead;

                BreakerSettings newFielddataSettings = new BreakerSettings(CircuitBreaker.FIELDDATA, newFielddataLimitBytes, newFielddataOverhead,
                        HierarchyCircuitBreakerService.this.fielddataSettings.getType());
                registerBreaker(newFielddataSettings);
                HierarchyCircuitBreakerService.this.fielddataSettings = newFielddataSettings;
                logger.info("Updated breaker settings fielddata: {}", newFielddataSettings);
            }

            // Request settings
            ByteSizeValue newRequestMax = settings.getAsMemory(REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING, null);
            Double newRequestOverhead = settings.getAsDouble(REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING, null);
            if (newRequestMax != null || newRequestOverhead != null) {
                long newRequestLimitBytes = newRequestMax == null ? HierarchyCircuitBreakerService.this.requestSettings.getLimit() : newRequestMax.bytes();
                newRequestOverhead = newRequestOverhead == null ? HierarchyCircuitBreakerService.this.requestSettings.getOverhead() : newRequestOverhead;

                BreakerSettings newRequestSettings = new BreakerSettings(CircuitBreaker.REQUEST, newRequestLimitBytes, newRequestOverhead,
                        HierarchyCircuitBreakerService.this.requestSettings.getType());
                registerBreaker(newRequestSettings);
                HierarchyCircuitBreakerService.this.requestSettings = newRequestSettings;
                logger.info("Updated breaker settings request: {}", newRequestSettings);
            }

            // Parent settings
            long oldParentMax = HierarchyCircuitBreakerService.this.parentSettings.getLimit();
            ByteSizeValue newParentMax = settings.getAsMemory(TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING, null);
            if (newParentMax != null && (newParentMax.bytes() != oldParentMax)) {
                BreakerSettings newParentSettings = new BreakerSettings(CircuitBreaker.PARENT, newParentMax.bytes(), 1.0, CircuitBreaker.Type.PARENT);
                validateSettings(new BreakerSettings[]{newParentSettings});
                HierarchyCircuitBreakerService.this.parentSettings = newParentSettings;
                logger.info("Updated breaker settings parent: {}", newParentSettings);
            }
        }
    }

    /**
     * Validate that child settings are valid
     * @throws IllegalStateException
     */
    public static void validateSettings(BreakerSettings[] childrenSettings) throws IllegalStateException {
        for (BreakerSettings childSettings : childrenSettings) {
            // If the child is disabled, ignore it
            if (childSettings.getLimit() == -1) {
                continue;
            }

            if (childSettings.getOverhead() < 0) {
                throw new IllegalStateException("Child breaker overhead " + childSettings + " must be non-negative");
            }
        }
    }

    @Override
    public CircuitBreaker getBreaker(String name) {
        return this.breakers.get(name);
    }

    @Override
    public AllCircuitBreakerStats stats() {
        long parentEstimated = 0;
        List<CircuitBreakerStats> allStats = new ArrayList<>();
        // Gather the "estimated" count for the parent breaker by adding the
        // estimations for each individual breaker
        for (CircuitBreaker breaker : this.breakers.values()) {
            allStats.add(stats(breaker.getName()));
            parentEstimated += breaker.getUsed();
        }
        // Manually add the parent breaker settings since they aren't part of the breaker map
        allStats.add(new CircuitBreakerStats(CircuitBreaker.PARENT, parentSettings.getLimit(),
                parentEstimated, 1.0, parentTripCount.get()));
        return new AllCircuitBreakerStats(allStats.toArray(new CircuitBreakerStats[allStats.size()]));
    }

    @Override
    public CircuitBreakerStats stats(String name) {
        CircuitBreaker breaker = this.breakers.get(name);
        return new CircuitBreakerStats(breaker.getName(), breaker.getLimit(), breaker.getUsed(), breaker.getOverhead(), breaker.getTrippedCount());
    }

    /**
     * Checks whether the parent breaker has been tripped
     * @param label
     * @throws CircuitBreakingException
     */
    public void checkParentLimit(String label) throws CircuitBreakingException {
        long totalUsed = 0;
        for (CircuitBreaker breaker : this.breakers.values()) {
            totalUsed += (breaker.getUsed() * breaker.getOverhead());
        }

        long parentLimit = this.parentSettings.getLimit();
        if (totalUsed > parentLimit) {
            this.parentTripCount.incrementAndGet();
            throw new CircuitBreakingException("[parent] Data too large, data for [" +
                    label + "] would be larger than limit of [" +
                    parentLimit + "/" + new ByteSizeValue(parentLimit) + "]",
                    totalUsed, parentLimit);
        }
    }

    /**
     * Allows to register a custom circuit breaker.
     * Warning: Will overwrite any existing custom breaker with the same name.
     *
     * @param breakerSettings
     */
    @Override
    public void registerBreaker(BreakerSettings breakerSettings) {
        // Validate the settings
        validateSettings(new BreakerSettings[] {breakerSettings});

        if (breakerSettings.getType() == CircuitBreaker.Type.NOOP) {
            CircuitBreaker breaker = new NoopCircuitBreaker(breakerSettings.getName());
            breakers.put(breakerSettings.getName(), breaker);
        } else {
            CircuitBreaker oldBreaker;
            CircuitBreaker breaker = new ChildMemoryCircuitBreaker(breakerSettings,
                    logger, this, breakerSettings.getName());

            for (;;) {
                oldBreaker = breakers.putIfAbsent(breakerSettings.getName(), breaker);
                if (oldBreaker == null) {
                    return;
                }
                breaker = new ChildMemoryCircuitBreaker(breakerSettings,
                        (ChildMemoryCircuitBreaker)oldBreaker, logger, this, breakerSettings.getName());

                if (breakers.replace(breakerSettings.getName(), oldBreaker, breaker)) {
                    return;
                }
            }
        }

    }
}
