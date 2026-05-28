/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.metric;

import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This wrapper allow us to record metric with APM (via {@link LongGauge}) while also access its current state via {@link AtomicLong}
 */
public record LongGaugeMetric(AtomicLong value, LongGauge gauge, boolean consuming, long noValue) {

    private static final long DEFAULT_NO_VALUE = Long.MIN_VALUE;

    /**
     * Create a "consuming" long gauge, i.e., it will only return a value if the value has been set since it was last polled
     *
     * @param meterRegistry The {@link MeterRegistry} to register the gauge with.
     * @param name The name of the gauge.
     * @param description The description of the gauge.
     * @param unit The unit of the gauge.
     * @return The created gauge.
     */
    public static LongGaugeMetric createConsuming(MeterRegistry meterRegistry, String name, String description, String unit) {
        return createConsuming(meterRegistry, name, description, unit, DEFAULT_NO_VALUE);
    }

    /**
     * Create a "consuming" long gauge, i.e., it will only return a value if the value has been set since it was last polled
     *
     * @param meterRegistry The {@link MeterRegistry} to register the gauge with.
     * @param name The name of the gauge.
     * @param description The description of the gauge.
     * @param unit The unit of the gauge.
     * @param noValue The value to signify that the gauge has not been set.
     * @return The created gauge.
     */
    public static LongGaugeMetric createConsuming(MeterRegistry meterRegistry, String name, String description, String unit, long noValue) {
        final AtomicLong value = new AtomicLong(noValue);
        return new LongGaugeMetric(value, meterRegistry.registerLongsGauge(name, description, unit, () -> {
            final var currentValue = value.getAndSet(noValue);
            return currentValue == noValue ? List.of() : List.of(new LongWithAttributes(currentValue));
        }), true, noValue);
    }

    public static LongGaugeMetric create(MeterRegistry meterRegistry, String name, String description, String unit) {
        final AtomicLong value = new AtomicLong();
        return new LongGaugeMetric(
            value,
            meterRegistry.registerLongGauge(name, description, unit, () -> new LongWithAttributes(value.get())),
            false,
            DEFAULT_NO_VALUE
        );
    }

    public void set(long l) {
        assert consuming == false || l != noValue : "Would set value to NO_VALUE for consuming gauge, specify a different value";
        value.set(l);
    }

    public long get() {
        assert consuming == false : "Consuming gauges should only be accessed via getIfPresent";
        return value.get();
    }

    /**
     * Get the current value, if it's not the {@link #noValue} value.
     */
    public OptionalLong getIfPresent() {
        final long currentValue = value.get();
        return consuming && currentValue == noValue ? OptionalLong.empty() : OptionalLong.of(currentValue);
    }
}
