/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.metrics.LongUpDownCounter;

import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.telemetry.MetricName;

import java.util.Map;

public class OtelLongUpDownCounter<T> extends SwitchableInstrument<LongUpDownCounter> implements org.elasticsearch.telemetry.metric.LongUpDownCounter {
    private final LazyInitializable<LongUpDownCounter, RuntimeException> counter;
    private final MetricName name;
    private final String description;
    private final T unit;

    private OtelLongUpDownCounter(
        LazyInitializable<LongUpDownCounter, RuntimeException> lazyCounter,
        MetricName name,
        String description,
        T unit
    ) {
        super(null, null);

        this.counter = lazyCounter;
        this.name = name;
        this.description = description;
        this.unit = unit;
    }

    public static <T> OtelLongUpDownCounter<T> build(
        LazyInitializable<LongUpDownCounter, RuntimeException> lazyCounter,
        Meter meter,
        MetricName name,
        String description,
        T unit
    ) {
        return new OtelLongUpDownCounter<>(lazyCounter, name, description, unit);
    }

    @Override
    public MetricName getName() {
        return name;
    }

    @Override
    public void add(long inc) {
        counter.getOrCompute().add(inc);
    }

    @Override
    public void add(long inc, Map<String, Object> attributes) {
        counter.getOrCompute().add(inc, OtelHelper.fromMap(attributes));
    }

    @Override
    public void add(long inc, Map<String, Object> attributes, ThreadContext threadContext) {
        throw new UnsupportedOperationException("unimplemented");
    }
}
