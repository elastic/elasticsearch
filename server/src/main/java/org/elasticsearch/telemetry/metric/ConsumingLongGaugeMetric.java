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
 * The consuming long gauge only returns a value to APM if a value has been set since it was last polled.
 *
 * @param value The holder of the current value of the gauge.
 * @param gauge The gauge being published to
 * @param noValue The "no value" value, which is used to signify that the gauge has not been set since last polled.
 */
public record ConsumingLongGaugeMetric(AtomicLong value, LongGauge gauge, long noValue) {

    private static final long DEFAULT_NO_VALUE = Long.MIN_VALUE;

    /**
     * Create a "consuming" long gauge
     *
     * @param meterRegistry The {@link MeterRegistry} to register the gauge with.
     * @param name The name of the gauge.
     * @param description The description of the gauge.
     * @param unit The unit of the gauge.
     * @return The created gauge.
     */
    public static ConsumingLongGaugeMetric create(MeterRegistry meterRegistry, String name, String description, String unit) {
        return create(meterRegistry, name, description, unit, DEFAULT_NO_VALUE);
    }

    /**
     * Create a "consuming" long gauge
     *
     * @param meterRegistry The {@link MeterRegistry} to register the gauge with.
     * @param name The name of the gauge.
     * @param description The description of the gauge.
     * @param unit The unit of the gauge.
     * @param noValue The value to signify that the gauge has not been set.
     * @return The created gauge.
     */
    public static ConsumingLongGaugeMetric create(MeterRegistry meterRegistry, String name, String description, String unit, long noValue) {
        final AtomicLong value = new AtomicLong(noValue);
        return new ConsumingLongGaugeMetric(value, meterRegistry.registerLongsGauge(name, description, unit, () -> {
            final var currentValue = value.getAndSet(noValue);
            return currentValue == noValue ? List.of() : List.of(new LongWithAttributes(currentValue));
        }), noValue);
    }

    public void set(long l) {
        assert l != noValue : "Would set value to NO_VALUE for consuming gauge, specify a different value";
        value.set(l);
    }

    /**
     * Get the current value, if it's not the {@link #noValue} value.
     */
    public OptionalLong getIfPresent() {
        final long currentValue = value.get();
        return currentValue == noValue ? OptionalLong.empty() : OptionalLong.of(currentValue);
    }
}
