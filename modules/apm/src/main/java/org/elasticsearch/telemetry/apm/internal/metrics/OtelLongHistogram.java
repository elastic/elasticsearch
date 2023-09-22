/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.telemetry.MetricName;

import java.util.Map;

public class OtelLongHistogram<T> extends SwitchableInstrument<io.opentelemetry.api.metrics.LongHistogram> implements org.elasticsearch.telemetry.metric.LongHistogram {
    private final LazyInitializable<io.opentelemetry.api.metrics.LongHistogram, RuntimeException> histogram;
    private final MetricName name;
    private final String description;
    private final T unit;

    public OtelLongHistogram(
        LazyInitializable<io.opentelemetry.api.metrics.LongHistogram, RuntimeException> histogram,
        MetricName name,
        String description,
        T unit
    ) {
        super(null, null);

        this.histogram = histogram;
        this.name = name;
        this.description = description;
        this.unit = unit;
    }

    public static <T> OtelLongHistogram<T> build(
        LazyInitializable<io.opentelemetry.api.metrics.LongHistogram, RuntimeException> lazyHistogram,
        Meter meter,
        MetricName name,
        String description,
        T unit
    ) {
        return new OtelLongHistogram<>(lazyHistogram, name, description, unit);
    }

    @Override
    public MetricName getName() {
        return name;
    }

    @Override
    public void record(long value) {
        histogram.getOrCompute().record(value);
    }

    @Override
    public void record(long value, Map<String, Object> attributes) {
        histogram.getOrCompute().record(value, OtelHelper.fromMap(attributes));
    }

    @Override
    public void record(long value, Map<String, Object> attributes, ThreadContext threadContext) {
        throw new UnsupportedOperationException("unimplemented");
    }
}
