/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.metrics.DoubleUpDownCounter;
import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.telemetry.MetricName;

import java.util.Map;
import java.util.function.Function;

public class OtelDoubleUpDownCounter<T> extends SwitchableInstrument<DoubleUpDownCounter>
    implements
        org.elasticsearch.telemetry.metric.DoubleUpDownCounter {
    private final MetricName name;
    private final String description;
    private final T unit;

    private OtelDoubleUpDownCounter(
        Function<Meter, DoubleUpDownCounter> instanceProducer,
        Meter metric,
        MetricName name,
        String description,
        T unit
    ) {
        super(instanceProducer, metric);
        this.name = name;
        this.description = description;
        this.unit = unit;
    }

    public static <T> OtelDoubleUpDownCounter<T> build(Meter meter, MetricName name, String description, T unit) {
        return new OtelDoubleUpDownCounter<>((m) -> createInstrument(m, name, description, unit), meter, name, description, unit);
    }

    private static <T> DoubleUpDownCounter createInstrument(Meter meter, MetricName name, String description, T unit) {
        return meter.upDownCounterBuilder(name.getRawName()).ofDoubles().setDescription(description).setUnit(unit.toString()).build();
    }

    @Override
    public MetricName getName() {
        return name;
    }

    @Override
    public void add(double inc) {
        getInstrument().add(inc);
    }

    @Override
    public void add(double inc, Map<String, Object> attributes) {
        getInstrument().add(inc, OtelHelper.fromMap(attributes));
    }

    @Override
    public void add(double inc, Map<String, Object> attributes, ThreadContext threadContext) {
        throw new UnsupportedOperationException("unimplemented");
    }
}
