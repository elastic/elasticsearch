/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.metrics.LongCounter;

import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.telemetry.MetricName;

import java.util.Map;

public class OtelLongCounter<T> extends SwitchableInstrument<LongCounter> implements org.elasticsearch.telemetry.metric.LongCounter {
    private final LazyInitializable<LongCounter, RuntimeException> counter;
    private final MetricName name;
    private final String description;
    private final T unit;

    private OtelLongCounter(LazyInitializable<LongCounter, RuntimeException> lazyCounter, MetricName name, String description, T unit, Meter meter) {
        super(null, null);
        this.counter = lazyCounter;
        this.name = name;
        this.description = description;
        this.unit = unit;
    }

    public static <T> OtelLongCounter<T> build(
        LazyInitializable<LongCounter, RuntimeException> lazyCounter,
        Meter meter,
        MetricName name,
        String description,
        T unit
    ) {
        return new OtelLongCounter<>(lazyCounter, name, description, unit, meter);
    }

    @Override
    public MetricName getName() {
        return name;
    }

    @Override
    public void increment() {
        counter.getOrCompute().add(1L);
    }

    @Override
    public void incrementBy(long inc) {
        assert inc >= 0;
        counter.getOrCompute().add(inc);
    }

    @Override
    public void incrementBy(long inc, Map<String, Object> attributes) {
        assert inc >= 0;
        counter.getOrCompute().add(inc, OtelHelper.fromMap(attributes));
    }

    @Override
    public void incrementBy(double inc, Map<String, Object> attributes, ThreadContext threadContext) {
        throw new UnsupportedOperationException("unimplemented");
    }
}
