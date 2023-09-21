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
import org.elasticsearch.telemetry.metric.DoubleHistogram;

import java.util.Map;
import java.util.function.Function;

public class OtelDoubleHistogram<T> extends SwitchableInstrument<io.opentelemetry.api.metrics.DoubleHistogram> implements DoubleHistogram {
    private final MetricName name;
    private final String description;
    private final T unit;

    public OtelDoubleHistogram(
        Function<Meter, io.opentelemetry.api.metrics.DoubleHistogram> instrumentProducer,
        Meter meter,
        MetricName name,
        String description,
        T unit
    ) {
        super(instrumentProducer, meter);
        this.name = name;
        this.description = description;
        this.unit = unit;
    }

    public static <T> OtelDoubleHistogram<T> build(
        Meter meter,
        MetricName name,
        String description,
        T unit
    ) {
        return new OtelDoubleHistogram<>(
            (m) -> createInstrument(m, name, description, unit)
            , meter, name, description, unit);
    }

    private static <T> io.opentelemetry.api.metrics.DoubleHistogram createInstrument(Meter meter, MetricName name, String description, T unit) {
        return meter.histogramBuilder(name.getRawName()).setDescription(description).setUnit(unit.toString()).build();
    }

    @Override
    public MetricName getName() {
        return name;
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
}
