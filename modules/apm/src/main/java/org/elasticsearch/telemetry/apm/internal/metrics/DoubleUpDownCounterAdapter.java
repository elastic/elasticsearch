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

import org.elasticsearch.telemetry.apm.AbstractInstrument;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * DoubleUpDownCounterAdapter wraps an otel DoubleUpDownCounter
 */
public class DoubleUpDownCounterAdapter extends AbstractInstrument<DoubleUpDownCounter>
    implements
        org.elasticsearch.telemetry.metric.DoubleUpDownCounter {

    public DoubleUpDownCounterAdapter(Meter meter, String name, String description, String unit) {
        super(
            meter,
            name,
            instrumentBuilder(Objects.requireNonNull(name), Objects.requireNonNull(description), Objects.requireNonNull(unit))
        );
    }

    protected static Function<Meter, DoubleUpDownCounter> instrumentBuilder(String name, String description, String unit) {
        return meter -> Objects.requireNonNull(meter)
            .upDownCounterBuilder(name)
            .ofDoubles()
            .setDescription(description)
            .setUnit(unit)
            .build();
    }

    @Override
    public void add(double inc) {
        getInstrument().add(inc);
    }

    @Override
    public void add(double inc, Map<String, Object> attributes) {
        getInstrument().add(inc, OtelHelper.fromMap(attributes));
    }
}
