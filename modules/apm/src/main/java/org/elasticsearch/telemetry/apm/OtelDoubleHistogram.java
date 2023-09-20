/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm;

import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.telemetry.MetricName;
import org.elasticsearch.telemetry.metric.DoubleHistogram;

import java.util.Map;

public class OtelDoubleHistogram<T> implements DoubleHistogram {
    private final LazyInitializable<io.opentelemetry.api.metrics.DoubleHistogram, RuntimeException> histogram;
    private final MetricName name;
    private final String description;
    private final T unit;

    public OtelDoubleHistogram(
        LazyInitializable<io.opentelemetry.api.metrics.DoubleHistogram, RuntimeException> histogram,
        MetricName name,
        String description,
        T unit
    ) {
        this.histogram = histogram;
        this.name = name;
        this.description = description;
        this.unit = unit;
    }

    public static <T> OtelDoubleHistogram<T> build(
        LazyInitializable<io.opentelemetry.api.metrics.DoubleHistogram, RuntimeException> lazyHistogram,
        MetricName name,
        String description,
        T unit
    ) {
        return new OtelDoubleHistogram<>(lazyHistogram, name, description, unit);
    }

    @Override
    public void record(double value) {
        histogram.getOrCompute().record(value);
    }

    @Override
    public void record(double value, Map<String, Object> attributes) {
        histogram.getOrCompute().record(value, OtelHelper.fromMap(attributes));
    }

    @Override
    public void record(double value, Map<String, Object> attributes, ThreadContext threadContext) {
        throw new UnsupportedOperationException("unimplemented");
    }
}
