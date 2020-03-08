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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * CircuitBreakerService that attempts to redistribute space between breakers
 * if tripped
 */
public class HierarchyCircuitBreakerService extends CircuitBreakerService {
    private static final Logger logger = LogManager.getLogger(HierarchyCircuitBreakerService.class);

    private static final String CHILD_LOGGER_PREFIX = "org.elasticsearch.indices.breaker.";

    private static final MemoryMXBean MEMORY_MX_BEAN = ManagementFactory.getMemoryMXBean();

    private final ConcurrentMap<String, CircuitBreaker> breakers = new ConcurrentHashMap<>();

    public static final Setting<Boolean> USE_REAL_MEMORY_USAGE_SETTING =
        Setting.boolSetting("indices.breaker.total.use_real_memory", true, Property.NodeScope);

    public static final Setting<ByteSizeValue> TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING =
        Setting.memorySizeSetting("indices.breaker.total.limit", settings -> {
            if (USE_REAL_MEMORY_USAGE_SETTING.get(settings)) {
                return "95%";
            } else {
                return "70%";
            }
        }, Property.Dynamic, Property.NodeScope);

    public static final Setting<ByteSizeValue> FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING =
        Setting.memorySizeSetting("indices.breaker.fielddata.limit", "40%", Property.Dynamic, Property.NodeScope);
    public static final Setting<Double> FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING =
        Setting.doubleSetting("indices.breaker.fielddata.overhead", 1.03d, 0.0d, Property.Dynamic, Property.NodeScope);
    public static final Setting<CircuitBreaker.Type> FIELDDATA_CIRCUIT_BREAKER_TYPE_SETTING =
        new Setting<>("indices.breaker.fielddata.type", "memory", CircuitBreaker.Type::parseValue, Property.NodeScope);

    public static final Setting<ByteSizeValue> REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING =
        Setting.memorySizeSetting("indices.breaker.request.limit", "60%", Property.Dynamic, Property.NodeScope);
    public static final Setting<Double> REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING =
        Setting.doubleSetting("indices.breaker.request.overhead", 1.0d, 0.0d, Property.Dynamic, Property.NodeScope);
    public static final Setting<CircuitBreaker.Type> REQUEST_CIRCUIT_BREAKER_TYPE_SETTING =
        new Setting<>("indices.breaker.request.type", "memory", CircuitBreaker.Type::parseValue, Property.NodeScope);

    public static final Setting<ByteSizeValue> ACCOUNTING_CIRCUIT_BREAKER_LIMIT_SETTING =
        Setting.memorySizeSetting("indices.breaker.accounting.limit", "100%", Property.Dynamic, Property.NodeScope);
    public static final Setting<Double> ACCOUNTING_CIRCUIT_BREAKER_OVERHEAD_SETTING =
        Setting.doubleSetting("indices.breaker.accounting.overhead", 1.0d, 0.0d, Property.Dynamic, Property.NodeScope);
    public static final Setting<CircuitBreaker.Type> ACCOUNTING_CIRCUIT_BREAKER_TYPE_SETTING =
        new Setting<>("indices.breaker.accounting.type", "memory", CircuitBreaker.Type::parseValue, Property.NodeScope);

    public static final Setting<ByteSizeValue> IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING =
        Setting.memorySizeSetting("network.breaker.inflight_requests.limit", "100%", Property.Dynamic, Property.NodeScope);
    public static final Setting<Double> IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_OVERHEAD_SETTING =
        Setting.doubleSetting("network.breaker.inflight_requests.overhead", 2.0d, 0.0d, Property.Dynamic, Property.NodeScope);
    public static final Setting<CircuitBreaker.Type> IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_TYPE_SETTING =
        new Setting<>("network.breaker.inflight_requests.type", "memory", CircuitBreaker.Type::parseValue, Property.NodeScope);

    private final boolean trackRealMemoryUsage;
    private volatile BreakerSettings parentSettings;
    private volatile BreakerSettings fielddataSettings;
    private volatile BreakerSettings inFlightRequestsSettings;
    private volatile BreakerSettings requestSettings;
    private volatile BreakerSettings accountingSettings;

    // Tripped count for when redistribution was attempted but wasn't successful
    private final AtomicLong parentTripCount = new AtomicLong(0);

    public HierarchyCircuitBreakerService(Settings settings, ClusterSettings clusterSettings) {
        super();
        this.fielddataSettings = new BreakerSettings(CircuitBreaker.FIELDDATA,
                FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING.get(settings).getBytes(),
                FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING.get(settings),
                FIELDDATA_CIRCUIT_BREAKER_TYPE_SETTING.get(settings),
                CircuitBreaker.Durability.PERMANENT
        );

        this.inFlightRequestsSettings = new BreakerSettings(CircuitBreaker.IN_FLIGHT_REQUESTS,
                IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING.get(settings).getBytes(),
                IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_OVERHEAD_SETTING.get(settings),
                IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_TYPE_SETTING.get(settings),
                CircuitBreaker.Durability.TRANSIENT
        );

        this.requestSettings = new BreakerSettings(CircuitBreaker.REQUEST,
                REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.get(settings).getBytes(),
                REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.get(settings),
                REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.get(settings),
                CircuitBreaker.Durability.TRANSIENT
        );

        this.accountingSettings = new BreakerSettings(CircuitBreaker.ACCOUNTING,
                ACCOUNTING_CIRCUIT_BREAKER_LIMIT_SETTING.get(settings).getBytes(),
                ACCOUNTING_CIRCUIT_BREAKER_OVERHEAD_SETTING.get(settings),
                ACCOUNTING_CIRCUIT_BREAKER_TYPE_SETTING.get(settings),
                CircuitBreaker.Durability.PERMANENT
        );

        this.parentSettings = new BreakerSettings(CircuitBreaker.PARENT,
                TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING.get(settings).getBytes(), 1.0,
                CircuitBreaker.Type.PARENT, null);

        if (logger.isTraceEnabled()) {
            logger.trace("parent circuit breaker with settings {}", this.parentSettings);
        }

        this.trackRealMemoryUsage = USE_REAL_MEMORY_USAGE_SETTING.get(settings);

        registerBreaker(this.requestSettings);
        registerBreaker(this.fielddataSettings);
        registerBreaker(this.inFlightRequestsSettings);
        registerBreaker(this.accountingSettings);

        clusterSettings.addSettingsUpdateConsumer(TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING, this::setTotalCircuitBreakerLimit,
            this::validateTotalCircuitBreakerLimit);
        clusterSettings.addSettingsUpdateConsumer(FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING, FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING,
            this::setFieldDataBreakerLimit);
        clusterSettings.addSettingsUpdateConsumer(IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING,
            IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_OVERHEAD_SETTING, this::setInFlightRequestsBreakerLimit);
        clusterSettings.addSettingsUpdateConsumer(REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING, REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING,
            this::setRequestBreakerLimit);
        clusterSettings.addSettingsUpdateConsumer(ACCOUNTING_CIRCUIT_BREAKER_LIMIT_SETTING, ACCOUNTING_CIRCUIT_BREAKER_OVERHEAD_SETTING,
            this::setAccountingBreakerLimit);
    }

    private void setRequestBreakerLimit(ByteSizeValue newRequestMax, Double newRequestOverhead) {
        BreakerSettings newRequestSettings = new BreakerSettings(CircuitBreaker.REQUEST, newRequestMax.getBytes(), newRequestOverhead,
                this.requestSettings.getType(), this.requestSettings.getDurability());
        registerBreaker(newRequestSettings);
        this.requestSettings = newRequestSettings;
        logger.info("Updated breaker settings request: {}", newRequestSettings);
    }

    private void setInFlightRequestsBreakerLimit(ByteSizeValue newInFlightRequestsMax, Double newInFlightRequestsOverhead) {
        BreakerSettings newInFlightRequestsSettings = new BreakerSettings(CircuitBreaker.IN_FLIGHT_REQUESTS,
            newInFlightRequestsMax.getBytes(), newInFlightRequestsOverhead, this.inFlightRequestsSettings.getType(),
            this.inFlightRequestsSettings.getDurability());
        registerBreaker(newInFlightRequestsSettings);
        this.inFlightRequestsSettings = newInFlightRequestsSettings;
        logger.info("Updated breaker settings for in-flight requests: {}", newInFlightRequestsSettings);
    }

    private void setFieldDataBreakerLimit(ByteSizeValue newFielddataMax, Double newFielddataOverhead) {
        long newFielddataLimitBytes = newFielddataMax == null ?
            HierarchyCircuitBreakerService.this.fielddataSettings.getLimit() : newFielddataMax.getBytes();
        newFielddataOverhead = newFielddataOverhead == null ?
            HierarchyCircuitBreakerService.this.fielddataSettings.getOverhead() : newFielddataOverhead;
        BreakerSettings newFielddataSettings = new BreakerSettings(CircuitBreaker.FIELDDATA, newFielddataLimitBytes, newFielddataOverhead,
                this.fielddataSettings.getType(), this.fielddataSettings.getDurability());
        registerBreaker(newFielddataSettings);
        HierarchyCircuitBreakerService.this.fielddataSettings = newFielddataSettings;
        logger.info("Updated breaker settings field data: {}", newFielddataSettings);
    }

    private void setAccountingBreakerLimit(ByteSizeValue newAccountingMax, Double newAccountingOverhead) {
        BreakerSettings newAccountingSettings = new BreakerSettings(CircuitBreaker.ACCOUNTING, newAccountingMax.getBytes(),
            newAccountingOverhead, HierarchyCircuitBreakerService.this.accountingSettings.getType(),
            this.accountingSettings.getDurability());
        registerBreaker(newAccountingSettings);
        HierarchyCircuitBreakerService.this.accountingSettings = newAccountingSettings;
        logger.info("Updated breaker settings for accounting requests: {}", newAccountingSettings);
    }

    private boolean validateTotalCircuitBreakerLimit(ByteSizeValue byteSizeValue) {
        BreakerSettings newParentSettings = new BreakerSettings(CircuitBreaker.PARENT, byteSizeValue.getBytes(), 1.0,
            CircuitBreaker.Type.PARENT, null);
        validateSettings(new BreakerSettings[]{newParentSettings});
        return true;
    }

    private void setTotalCircuitBreakerLimit(ByteSizeValue byteSizeValue) {
        BreakerSettings newParentSettings = new BreakerSettings(CircuitBreaker.PARENT, byteSizeValue.getBytes(), 1.0,
            CircuitBreaker.Type.PARENT, null);
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
        List<CircuitBreakerStats> allStats = new ArrayList<>(this.breakers.size());
        // Gather the "estimated" count for the parent breaker by adding the
        // estimations for each individual breaker
        for (CircuitBreaker breaker : this.breakers.values()) {
            allStats.add(stats(breaker.getName()));
        }
        // Manually add the parent breaker settings since they aren't part of the breaker map
        allStats.add(new CircuitBreakerStats(CircuitBreaker.PARENT, parentSettings.getLimit(),
            memoryUsed(0L).totalUsage, 1.0, parentTripCount.get()));
        return new AllCircuitBreakerStats(allStats.toArray(new CircuitBreakerStats[allStats.size()]));
    }

    @Override
    public CircuitBreakerStats stats(String name) {
        CircuitBreaker breaker = this.breakers.get(name);
        return new CircuitBreakerStats(breaker.getName(), breaker.getLimit(), breaker.getUsed(), breaker.getOverhead(),
            breaker.getTrippedCount());
    }

    private static class MemoryUsage {
        final long baseUsage;
        final long totalUsage;
        final long transientChildUsage;
        final long permanentChildUsage;

        MemoryUsage(final long baseUsage, final long totalUsage, final long transientChildUsage, final long permanentChildUsage) {
            this.baseUsage = baseUsage;
            this.totalUsage = totalUsage;
            this.transientChildUsage = transientChildUsage;
            this.permanentChildUsage = permanentChildUsage;
        }
    }

    private MemoryUsage memoryUsed(long newBytesReserved) {
        long transientUsage = 0;
        long permanentUsage = 0;

        for (CircuitBreaker breaker : this.breakers.values()) {
            long breakerUsed = (long)(breaker.getUsed() * breaker.getOverhead());
            if (breaker.getDurability() == CircuitBreaker.Durability.TRANSIENT) {
                transientUsage += breakerUsed;
            } else if (breaker.getDurability() == CircuitBreaker.Durability.PERMANENT) {
                permanentUsage += breakerUsed;
            }
        }
        if (this.trackRealMemoryUsage) {
            final long current = currentMemoryUsage();
            return new MemoryUsage(current, current + newBytesReserved, transientUsage, permanentUsage);
        } else {
            long parentEstimated = transientUsage + permanentUsage;
            return new MemoryUsage(parentEstimated, parentEstimated, transientUsage, permanentUsage);
        }
    }

    //package private to allow overriding it in tests
    long currentMemoryUsage() {
        try {
            return MEMORY_MX_BEAN.getHeapMemoryUsage().getUsed();
        } catch (IllegalArgumentException ex) {
            // This exception can happen (rarely) due to a race condition in the JVM when determining usage of memory pools. We do not want
            // to fail requests because of this and thus return zero memory usage in this case. While we could also return the most
            // recently determined memory usage, we would overestimate memory usage immediately after a garbage collection event.
            assert ex.getMessage().matches("committed = \\d+ should be < max = \\d+");
            logger.info("Cannot determine current memory usage due to JDK-8207200.", ex);
            return 0;
        }
    }

    public long getParentLimit() {
        return this.parentSettings.getLimit();
    }

    /**
     * Checks whether the parent breaker has been tripped
     */
    public void checkParentLimit(long newBytesReserved, String label) throws CircuitBreakingException {
        final MemoryUsage memoryUsed = memoryUsed(newBytesReserved);
        long parentLimit = this.parentSettings.getLimit();
        if (memoryUsed.totalUsage > parentLimit) {
            this.parentTripCount.incrementAndGet();
            final StringBuilder message = new StringBuilder("[parent] Data too large, data for [" + label + "]" +
                    " would be [" + memoryUsed.totalUsage + "/" + new ByteSizeValue(memoryUsed.totalUsage) + "]" +
                    ", which is larger than the limit of [" +
                    parentLimit + "/" + new ByteSizeValue(parentLimit) + "]");
            if (this.trackRealMemoryUsage) {
                final long realUsage = memoryUsed.baseUsage;
                message.append(", real usage: [");
                message.append(realUsage);
                message.append("/");
                message.append(new ByteSizeValue(realUsage));
                message.append("], new bytes reserved: [");
                message.append(newBytesReserved);
                message.append("/");
                message.append(new ByteSizeValue(newBytesReserved));
                message.append("]");
            }
            message.append(", usages [");
            message.append(String.join(", ",
                this.breakers.entrySet().stream().map(e -> {
                    final CircuitBreaker breaker = e.getValue();
                    final long breakerUsed = (long)(breaker.getUsed() * breaker.getOverhead());
                    return e.getKey() + "=" + breakerUsed + "/" + new ByteSizeValue(breakerUsed);
                })
                    .collect(Collectors.toList())));
            message.append("]");
            // derive durability of a tripped parent breaker depending on whether the majority of memory tracked by
            // child circuit breakers is categorized as transient or permanent.
            CircuitBreaker.Durability durability = memoryUsed.transientChildUsage >= memoryUsed.permanentChildUsage ?
                CircuitBreaker.Durability.TRANSIENT : CircuitBreaker.Durability.PERMANENT;
            logger.debug("{}", message);
            throw new CircuitBreakingException(message.toString(), memoryUsed.totalUsage, parentLimit, durability);
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
                    LogManager.getLogger(CHILD_LOGGER_PREFIX + breakerSettings.getName()),
                    this, breakerSettings.getName());

            for (;;) {
                oldBreaker = breakers.putIfAbsent(breakerSettings.getName(), breaker);
                if (oldBreaker == null) {
                    return;
                }
                breaker = new ChildMemoryCircuitBreaker(breakerSettings,
                        (ChildMemoryCircuitBreaker)oldBreaker,
                        LogManager.getLogger(CHILD_LOGGER_PREFIX + breakerSettings.getName()),
                        this, breakerSettings.getName());

                if (breakers.replace(breakerSettings.getName(), oldBreaker, breaker)) {
                    return;
                }
            }
        }

    }
}
