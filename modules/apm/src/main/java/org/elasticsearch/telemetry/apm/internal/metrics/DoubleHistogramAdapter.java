/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.telemetry.apm.AbstractInstrument;

import java.util.Map;
import java.util.Objects;

/**
 * DoubleHistogramAdapter wraps an otel DoubleHistogram
 */
public class DoubleHistogramAdapter extends AbstractInstrument<DoubleHistogram>
    implements
        org.elasticsearch.telemetry.metric.DoubleHistogram {

    public DoubleHistogramAdapter(Meter meter, String name, String description, String unit) {
        super(meter, name, description, unit);
    }

    @Override
    protected io.opentelemetry.api.metrics.DoubleHistogram buildInstrument(Meter meter) {
        var builder = Objects.requireNonNull(meter).histogramBuilder(getName());
        return builder.setDescription(getDescription()).setUnit(getUnit()).build();
    }

    @Override
    public void record(double value) {
        getInstrument().record(value);
    }

    @Override
    public void record(double value, Map<String, Object> attributes) {
        getInstrument().record(value, OtelHelper.fromMap(attributes));
    }
}
