/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm;

import io.opentelemetry.api.metrics.LongCounter;

import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.telemetry.MetricName;
import org.elasticsearch.telemetry.metric.Counter;

public class OtelCounter<T> implements Counter {
    private final LazyInitializable<LongCounter, RuntimeException> counter;
    private final MetricName name;
    private final String description;
    private final T unit;

    private OtelCounter(LazyInitializable<LongCounter, RuntimeException> lazyCounter, MetricName name, String description, T unit) {
        this.counter = lazyCounter;
        this.name = name;
        this.description = description;
        this.unit = unit;
    }

    public static <T> OtelCounter<T> build(
        LazyInitializable<LongCounter, RuntimeException> lazyCounter,
        MetricName name,
        String description,
        T unit
    ) {
        return new OtelCounter<>(lazyCounter, name, description, unit);
    }

    @Override
    public void increment() {
        counter.getOrCompute().add(1L);
    }

    @Override
    public void incrementBy(long inc) {
        counter.getOrCompute().add(inc);
    }
}
