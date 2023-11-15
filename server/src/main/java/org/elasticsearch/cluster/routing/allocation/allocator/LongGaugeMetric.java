/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.telemetry.metric.LongGauge;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.concurrent.atomic.AtomicLong;

/**
 * This wrapper allow us to record metric with APM (via LongGauge) while also access its current state via AtomicLong
 */
public record LongGaugeMetric(AtomicLong value, LongGauge gauge) {

    public static LongGaugeMetric create(MeterRegistry meterRegistry, String name, String description) {
        return create(meterRegistry, name, description, "");
    }

    public static LongGaugeMetric create(MeterRegistry meterRegistry, String name, String description, String unit) {
        final AtomicLong value = new AtomicLong();
        return new LongGaugeMetric(
            value,
            meterRegistry.registerLongGauge(name, description, unit, () -> new LongWithAttributes(value.get()))
        );
    }

    public void set(long l) {
        value.set(l);
    }

    public long get() {
        return value.get();
    }
}
