/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm;

import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.telemetry.MetricName;
import org.elasticsearch.telemetry.metric.Counter;

public class OtelCounter<T> implements Counter {
    private final LongCounter counter;
    private final MetricName name;
    private final String description;
    private final T unit;

    private OtelCounter(LongCounter counter, MetricName name, String description, T unit) {
        this.counter = counter;
        this.name = name;
        this.description = description;
        this.unit = unit;
    }

    public static <T> OtelCounter<T> build(Meter meter, MetricName name, String description, T unit) {
        return new OtelCounter<>(
            meter.counterBuilder(name.getRawName()).setDescription(description).setUnit(unit.toString()).build(),
            name,
            description,
            unit
        );
    }

    @Override
    public void increment() {
        counter.add(1L);
    }

    @Override
    public void incrementBy(long inc) {
        counter.add(inc);
    }
}
