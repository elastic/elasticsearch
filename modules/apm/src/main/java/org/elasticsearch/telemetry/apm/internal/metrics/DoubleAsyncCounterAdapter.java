/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleCounter;

import org.elasticsearch.telemetry.metric.DoubleAsyncCounter;
import org.elasticsearch.telemetry.metric.DoubleAsyncMeasurement;

import java.util.Objects;
import java.util.function.Consumer;

class DoubleAsyncCounterAdapter extends AbstractAsyncInstrument<ObservableDoubleCounter> implements DoubleAsyncCounter {

    DoubleAsyncCounterAdapter(
        Meter meter,
        String name,
        String description,
        String unit,
        Consumer<DoubleAsyncMeasurement> callback,
        Consumer<AbstractInstrument<?>> deregisterFunc
    ) {
        super(meter, new Builder(name, description, unit, callback), deregisterFunc);
    }

    private static class Builder extends AbstractInstrument.Builder<ObservableDoubleCounter> {
        private final Consumer<DoubleAsyncMeasurement> callback;

        private Builder(String name, String description, String unit, Consumer<DoubleAsyncMeasurement> callback) {
            super(name, description, unit);
            this.callback = Objects.requireNonNull(callback);
        }

        @Override
        public ObservableDoubleCounter build(Meter meter) {
            return Objects.requireNonNull(meter)
                .counterBuilder(name)
                .setDescription(description)
                .setUnit(unit)
                .ofDoubles()
                .buildWithCallback(OtelHelper.doubleCounterMeasurementCallback(name, callback));
        }
    }
}
