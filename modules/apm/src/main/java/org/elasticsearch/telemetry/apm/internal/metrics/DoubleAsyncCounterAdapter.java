/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleCounter;

import org.elasticsearch.telemetry.apm.AbstractInstrument;
import org.elasticsearch.telemetry.metric.DoubleAsyncCounter;
import org.elasticsearch.telemetry.metric.DoubleWithAttributes;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

public class DoubleAsyncCounterAdapter extends AbstractInstrument<ObservableDoubleCounter> implements DoubleAsyncCounter {

    public DoubleAsyncCounterAdapter(Meter meter, String name, String description, String unit, Supplier<DoubleWithAttributes> observer) {
        super(
            meter,
            name,
            instrumentBuilder(
                Objects.requireNonNull(name),
                Objects.requireNonNull(description),
                Objects.requireNonNull(unit),
                Objects.requireNonNull(observer)
            )
        );
    }

    protected static Function<Meter, ObservableDoubleCounter> instrumentBuilder(
        String name,
        String description,
        String unit,
        Supplier<DoubleWithAttributes> observer
    ) {
        return meter -> Objects.requireNonNull(meter)
            .counterBuilder(name)
            .setDescription(description)
            .setUnit(unit)
            .ofDoubles()
            .buildWithCallback(OtelHelper.doubleMeasurementCallback(observer));
    }

    @Override
    public void close() throws Exception {
        getInstrument().close();
    }
}
