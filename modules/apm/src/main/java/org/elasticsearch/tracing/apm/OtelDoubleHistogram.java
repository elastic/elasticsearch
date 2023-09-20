/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tracing.apm;

import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tracing.MetricName;

import java.util.Map;

public class OtelDoubleHistogram<T> implements DoubleHistogram {
    private final io.opentelemetry.api.metrics.DoubleHistogram histogram;
    private final MetricName name;
    private final String description;
    private final T unit;

    public OtelDoubleHistogram(io.opentelemetry.api.metrics.DoubleHistogram histogram, MetricName name, String description, T unit) {
        this.histogram = histogram;
        this.name = name;
        this.description = description;
        this.unit = unit;
    }

    public static <T> OtelDoubleHistogram<T> build(Meter meter, MetricName name, String description, T unit) {
        return new OtelDoubleHistogram<>(
            meter.histogramBuilder(name.getRawName()).setDescription(description).setUnit(unit.toString()).build(),
            name,
            description,
            unit
        );
    }

    @Override
    public void record(double value) {
        histogram.record(value);
    }

    @Override
    public void record(double value, Map<String, Object> attributes) {
        histogram.record(value, OtelHelper.fromMap(attributes));
    }

    @Override
    public void record(double value, Map<String, Object> attributes, ThreadContext threadContext) {
        throw new UnsupportedOperationException("unimplemented");
    }
}
