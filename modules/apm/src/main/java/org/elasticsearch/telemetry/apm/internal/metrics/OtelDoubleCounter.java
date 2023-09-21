/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.telemetry.MetricName;
import org.elasticsearch.telemetry.metric.DoubleCounter;

import java.util.Map;
import java.util.function.Function;

public class OtelDoubleCounter<T> extends SwitchableInstrument<io.opentelemetry.api.metrics.DoubleCounter> implements DoubleCounter {
    private final MetricName name;
    private final String description;
    private final T unit;

    private OtelDoubleCounter(
        Function<Meter, io.opentelemetry.api.metrics.DoubleCounter> producer,
        Meter meter,
        MetricName name,
        String description,
        T unit
    ) {
        super(producer, meter);
        this.name = name;
        this.description = description;
        this.unit = unit;
    }

    public static <T> OtelDoubleCounter<T> build(Meter meter, MetricName name, String description, T unit) {
        return new OtelDoubleCounter<>((m) -> createInstrument(m, name, description, unit), meter, name, description, unit);
    }

    private static <T> io.opentelemetry.api.metrics.DoubleCounter createInstrument(
        Meter meter,
        MetricName name,
        String description,
        T unit
    ) {
        return meter.counterBuilder(name.getRawName()).ofDoubles().setDescription(description).setUnit(unit.toString()).build();
    }

    @Override
    public MetricName getName() {
        return name;
    }

    @Override
    public void increment() {
        getInstrument().add(1d);
    }

    @Override
    public void incrementBy(double inc) {
        assert inc >= 0;
        getInstrument().add(inc);
    }

    @Override
    public void incrementBy(double inc, Map<String, Object> attributes) {
        assert inc >= 0;
        getInstrument().add(inc, OtelHelper.fromMap(attributes));
    }

    @Override
    public void incrementBy(double inc, Map<String, Object> attributes, ThreadContext threadContext) {
        throw new UnsupportedOperationException("unimplemented");
    }
}
