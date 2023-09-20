/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm;

import io.opentelemetry.api.metrics.DoubleUpDownCounter;

import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.telemetry.MetricName;

import java.util.Map;

public class OtelDoubleUpDownCounter<T> implements org.elasticsearch.telemetry.metric.DoubleUpDownCounter {
    private final LazyInitializable<DoubleUpDownCounter, RuntimeException> counter;
    private final MetricName name;
    private final String description;
    private final T unit;

    private OtelDoubleUpDownCounter(
        LazyInitializable<DoubleUpDownCounter, RuntimeException> lazyCounter,
        MetricName name,
        String description,
        T unit
    ) {
        this.counter = lazyCounter;
        this.name = name;
        this.description = description;
        this.unit = unit;
    }

    public static <T> OtelDoubleUpDownCounter<T> build(
        LazyInitializable<DoubleUpDownCounter, RuntimeException> lazyCounter,
        MetricName name,
        String description,
        T unit
    ) {
        return new OtelDoubleUpDownCounter<>(lazyCounter, name, description, unit);
    }

    @Override
    public MetricName getName() {
        return name;
    }

    @Override
    public void add(double inc) {
        counter.getOrCompute().add(inc);
    }

    @Override
    public void add(double inc, Map<String, Object> attributes) {
        counter.getOrCompute().add(inc, OtelHelper.fromMap(attributes));
    }

    @Override
    public void add(double inc, Map<String, Object> attributes, ThreadContext threadContext) {
        throw new UnsupportedOperationException("unimplemented");
    }
}
