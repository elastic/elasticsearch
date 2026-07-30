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
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The consuming long gauge only returns a value to APM if a value has been set since it was last polled.
 * Optional attributes may be attached when setting a value and are reported with that value on the next poll.
 * <p>
 * Prefer this type when the value changes infrequently. Each {@link #set} allocates a measurement holder, so for
 * values that are updated frequently use {@link LongGauge} (or {@link LongGaugeMetric}) instead.
 *
 * @param value The holder of the current value of the gauge.
 * @param gauge The gauge being published to
 */
public record ConsumingLongGaugeMetric(AtomicReference<LongWithAttributes> value, LongGauge gauge) {

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
        final AtomicReference<LongWithAttributes> value = new AtomicReference<>();
        return new ConsumingLongGaugeMetric(value, meterRegistry.registerLongsGauge(name, description, unit, () -> {
            final var currentValue = value.getAndSet(null);
            return currentValue == null ? List.of() : List.of(currentValue);
        }));
    }

    /**
     * Set the gauge value with no attributes. The value is reported on the next poll and then cleared.
     */
    public void set(long l) {
        set(l, Map.of());
    }

    /**
     * Set the gauge value and attributes. Both are reported together on the next poll and then cleared.
     * Use an empty map when there are no attributes. The attributes map is copied at set-time so later
     * mutation of the caller's map does not affect the measurement.
     */
    public void set(long l, Map<String, Object> attributes) {
        value.set(new LongWithAttributes(l, Map.copyOf(attributes)));
    }

    // visible for tests
    OptionalLong getValueIfPresent() {
        final LongWithAttributes currentValue = value.get();
        return currentValue == null ? OptionalLong.empty() : OptionalLong.of(currentValue.value());
    }
}
