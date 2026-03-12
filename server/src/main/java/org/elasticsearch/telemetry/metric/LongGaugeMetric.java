/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.metric;

import java.util.concurrent.atomic.AtomicLong;

/**
 * This wrapper allow us to record metric with APM (via {@link LongGauge}) while also access its current state via {@link AtomicLong}
 */
public record LongGaugeMetric(AtomicLong value, LongGauge gauge) {

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
