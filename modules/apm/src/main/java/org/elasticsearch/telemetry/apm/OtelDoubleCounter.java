/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm;

import io.opentelemetry.api.metrics.DoubleCounter;

import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.telemetry.MetricName;

import java.util.Map;

public class OtelDoubleCounter<T> implements org.elasticsearch.telemetry.metric.DoubleCounter {
    private final LazyInitializable<DoubleCounter, RuntimeException> counter;
    private final MetricName name;
    private final String description;
    private final T unit;

    private OtelDoubleCounter(LazyInitializable<DoubleCounter, RuntimeException> lazyCounter, MetricName name, String description, T unit) {
        this.counter = lazyCounter;
        this.name = name;
        this.description = description;
        this.unit = unit;
    }

    public static <T> OtelDoubleCounter<T> build(
        LazyInitializable<DoubleCounter, RuntimeException> lazyCounter,
        MetricName name,
        String description,
        T unit
    ) {
        return new OtelDoubleCounter<>(lazyCounter, name, description, unit);
    }

    @Override
    public MetricName getName() {
        return name;
    }

    @Override
    public void increment() {
        counter.getOrCompute().add(1d);
    }

    @Override
    public void incrementBy(double inc) {
        assert inc >= 0;
        counter.getOrCompute().add(inc);
    }

    @Override
    public void incrementBy(double inc, Map<String, Object> attributes) {
        assert inc >= 0;
        counter.getOrCompute().add(inc, OtelHelper.fromMap(attributes));
    }

    @Override
    public void incrementBy(double inc, Map<String, Object> attributes, ThreadContext threadContext) {
        throw new UnsupportedOperationException("unimplemented");
    }
}
