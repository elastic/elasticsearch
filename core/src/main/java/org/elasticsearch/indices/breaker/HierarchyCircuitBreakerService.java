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
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;

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

    private static final String CHILD_LOGGER_PREFIX = "org.elasticsearch.indices.breaker.";

    private final ConcurrentMap<String, CircuitBreaker> breakers = new ConcurrentHashMap<>();

    public static final Setting<ByteSizeValue> TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING =
        Setting.byteSizeSetting("indices.breaker.total.limit", "70%", Property.Dynamic, Property.NodeScope);

    public static final Setting<ByteSizeValue> FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING =
        Setting.byteSizeSetting("indices.breaker.fielddata.limit", "60%", Property.Dynamic, Property.NodeScope);
    public static final Setting<Double> FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING =
        Setting.doubleSetting("indices.breaker.fielddata.overhead", 1.03d, 0.0d, Property.Dynamic, Property.NodeScope);
    public static final Setting<CircuitBreaker.Type> FIELDDATA_CIRCUIT_BREAKER_TYPE_SETTING =
        new Setting<>("indices.breaker.fielddata.type", "memory", CircuitBreaker.Type::parseValue, Property.NodeScope);

    public static final Setting<ByteSizeValue> REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING =
        Setting.byteSizeSetting("indices.breaker.request.limit", "40%", Property.Dynamic, Property.NodeScope);
    public static final Setting<Double> REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING =
        Setting.doubleSetting("indices.breaker.request.overhead", 1.0d, 0.0d, Property.Dynamic, Property.NodeScope);
    public static final Setting<CircuitBreaker.Type> REQUEST_CIRCUIT_BREAKER_TYPE_SETTING =
        new Setting<>("indices.breaker.request.type", "memory", CircuitBreaker.Type::parseValue, Property.NodeScope);

    public static final Setting<ByteSizeValue> IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING =
        Setting.byteSizeSetting("network.breaker.inflight_requests.limit", "100%", Property.Dynamic, Property.NodeScope);
    public static final Setting<Double> IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_OVERHEAD_SETTING =
        Setting.doubleSetting("network.breaker.inflight_requests.overhead", 1.0d, 0.0d, Property.Dynamic, Property.NodeScope);
    public static final Setting<CircuitBreaker.Type> IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_TYPE_SETTING =
        new Setting<>("network.breaker.inflight_requests.type", "memory", CircuitBreaker.Type::parseValue, Property.NodeScope);

    private volatile BreakerSettings parentSettings;
    private volatile BreakerSettings fielddataSettings;
    private volatile BreakerSettings inFlightRequestsSettings;
    private volatile BreakerSettings requestSettings;

    // Tripped count for when redistribution was attempted but wasn't successful
    private final AtomicLong parentTripCount = new AtomicLong(0);

    public HierarchyCircuitBreakerService(Settings settings, ClusterSettings clusterSettings) {
        super(settings);
        this.fielddataSettings = new BreakerSettings(CircuitBreaker.FIELDDATA,
                FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING.get(settings).bytes(),
                FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING.get(settings),
                FIELDDATA_CIRCUIT_BREAKER_TYPE_SETTING.get(settings)
        );

        this.inFlightRequestsSettings = new BreakerSettings(CircuitBreaker.IN_FLIGHT_REQUESTS,
                IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING.get(settings).bytes(),
                IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_OVERHEAD_SETTING.get(settings),
                IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_TYPE_SETTING.get(settings)
        );

        this.requestSettings = new BreakerSettings(CircuitBreaker.REQUEST,
                REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.get(settings).bytes(),
                REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.get(settings),
                REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.get(settings)
        );

        this.parentSettings = new BreakerSettings(CircuitBreaker.PARENT, TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING.get(settings).bytes(), 1.0, CircuitBreaker.Type.PARENT);
        if (logger.isTraceEnabled()) {
            logger.trace("parent circuit breaker with settings {}", this.parentSettings);
        }

        registerBreaker(this.requestSettings);
        registerBreaker(this.fielddataSettings);
        registerBreaker(this.inFlightRequestsSettings);

        clusterSettings.addSettingsUpdateConsumer(TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING, this::setTotalCircuitBreakerLimit, this::validateTotalCircuitBreakerLimit);
        clusterSettings.addSettingsUpdateConsumer(FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING, FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING, this::setFieldDataBreakerLimit);
        clusterSettings.addSettingsUpdateConsumer(IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING, IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_OVERHEAD_SETTING, this::setInFlightRequestsBreakerLimit);
        clusterSettings.addSettingsUpdateConsumer(REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING, REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING, this::setRequestBreakerLimit);
    }

    private void setRequestBreakerLimit(ByteSizeValue newRequestMax, Double newRequestOverhead) {
        BreakerSettings newRequestSettings = new BreakerSettings(CircuitBreaker.REQUEST, newRequestMax.bytes(), newRequestOverhead,
                HierarchyCircuitBreakerService.this.requestSettings.getType());
        registerBreaker(newRequestSettings);
        HierarchyCircuitBreakerService.this.requestSettings = newRequestSettings;
        logger.info("Updated breaker settings request: {}", newRequestSettings);
    }

    private void setInFlightRequestsBreakerLimit(ByteSizeValue newInFlightRequestsMax, Double newInFlightRequestsOverhead) {
        BreakerSettings newInFlightRequestsSettings = new BreakerSettings(CircuitBreaker.IN_FLIGHT_REQUESTS, newInFlightRequestsMax.bytes(),
            newInFlightRequestsOverhead, HierarchyCircuitBreakerService.this.inFlightRequestsSettings.getType());
        registerBreaker(newInFlightRequestsSettings);
        HierarchyCircuitBreakerService.this.inFlightRequestsSettings = newInFlightRequestsSettings;
        logger.info("Updated breaker settings for in-flight requests: {}", newInFlightRequestsSettings);
    }

    private void setFieldDataBreakerLimit(ByteSizeValue newFielddataMax, Double newFielddataOverhead) {
        long newFielddataLimitBytes = newFielddataMax == null ? HierarchyCircuitBreakerService.this.fielddataSettings.getLimit() : newFielddataMax.bytes();
        newFielddataOverhead = newFielddataOverhead == null ? HierarchyCircuitBreakerService.this.fielddataSettings.getOverhead() : newFielddataOverhead;
        BreakerSettings newFielddataSettings = new BreakerSettings(CircuitBreaker.FIELDDATA, newFielddataLimitBytes, newFielddataOverhead,
                HierarchyCircuitBreakerService.this.fielddataSettings.getType());
        registerBreaker(newFielddataSettings);
        HierarchyCircuitBreakerService.this.fielddataSettings = newFielddataSettings;
        logger.info("Updated breaker settings field data: {}", newFielddataSettings);

    }

    private boolean validateTotalCircuitBreakerLimit(ByteSizeValue byteSizeValue) {
        BreakerSettings newParentSettings = new BreakerSettings(CircuitBreaker.PARENT, byteSizeValue.bytes(), 1.0, CircuitBreaker.Type.PARENT);
        validateSettings(new BreakerSettings[]{newParentSettings});
        return true;
    }

    private void setTotalCircuitBreakerLimit(ByteSizeValue byteSizeValue) {
        BreakerSettings newParentSettings = new BreakerSettings(CircuitBreaker.PARENT, byteSizeValue.bytes(), 1.0, CircuitBreaker.Type.PARENT);
        this.parentSettings = newParentSettings;
    }

    /**
     * Validate that child settings are valid
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
                    Loggers.getLogger(CHILD_LOGGER_PREFIX + breakerSettings.getName()),
                    this, breakerSettings.getName());

            for (;;) {
                oldBreaker = breakers.putIfAbsent(breakerSettings.getName(), breaker);
                if (oldBreaker == null) {
                    return;
                }
                breaker = new ChildMemoryCircuitBreaker(breakerSettings,
                        (ChildMemoryCircuitBreaker)oldBreaker,
                        Loggers.getLogger(CHILD_LOGGER_PREFIX + breakerSettings.getName()),
                        this, breakerSettings.getName());

                if (breakers.replace(breakerSettings.getName(), oldBreaker, breaker)) {
                    return;
                }
            }
        }

    }
}
