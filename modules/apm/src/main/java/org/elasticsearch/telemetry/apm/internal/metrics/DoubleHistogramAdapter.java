/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.context.Context;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.telemetry.MetricName;

import java.util.Map;
import java.util.Objects;

public class DoubleHistogramAdapter<T> extends AbstractInstrument<T, DoubleHistogram>
    implements
        org.elasticsearch.telemetry.metric.DoubleHistogram {
    private static final DoubleHistogram NOOP_INSTANCE = new Noop();

    public DoubleHistogramAdapter(MetricName name, String description, T unit) {
        super(name, description, unit);
    }

    @Override
    DoubleHistogram buildInstrument(Meter meter) {
        return Objects.requireNonNull(meter).histogramBuilder(getName()).setDescription(getDescription()).setUnit(getUnit()).build();
    }

    @Override
    protected DoubleHistogram getNOOP() {
        return NOOP_INSTANCE;
    }

    @Override
    public void record(double value) {
        getInstrument().record(value);
    }

    @Override
    public void record(double value, Map<String, Object> attributes) {
        getInstrument().record(value, OtelHelper.fromMap(attributes));
    }

    @Override
    public void record(double value, Map<String, Object> attributes, ThreadContext threadContext) {
        throw new UnsupportedOperationException("unimplemented");
    }

    private static class Noop implements io.opentelemetry.api.metrics.DoubleHistogram {
        @Override
        public void record(double value) {

        }

        @Override
        public void record(double value, Attributes attributes) {

        }

        @Override
        public void record(double value, Attributes attributes, Context context) {

        }
    }
}
