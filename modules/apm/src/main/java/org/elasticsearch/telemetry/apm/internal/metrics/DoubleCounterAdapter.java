/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.metrics.DoubleCounter;
import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.telemetry.apm.AbstractInstrument;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * DoubleGaugeAdapter wraps an otel ObservableDoubleMeasurement
 */
public class DoubleCounterAdapter extends AbstractInstrument<DoubleCounter> implements org.elasticsearch.telemetry.metric.DoubleCounter {

    public DoubleCounterAdapter(Meter meter, String name, String description, String unit) {
        super(
            meter,
            name,
            instrumentBuilder(Objects.requireNonNull(name), Objects.requireNonNull(description), Objects.requireNonNull(unit))
        );
    }

    protected static Function<Meter, DoubleCounter> instrumentBuilder(String name, String description, String unit) {
        return meter -> Objects.requireNonNull(meter).counterBuilder(name).ofDoubles().setDescription(description).setUnit(unit).build();
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
}
