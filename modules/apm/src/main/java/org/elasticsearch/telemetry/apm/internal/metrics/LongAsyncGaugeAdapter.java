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
import io.opentelemetry.api.metrics.ObservableLongGauge;

import org.elasticsearch.telemetry.metric.LongAsyncGauge;
import org.elasticsearch.telemetry.metric.LongWithAttributes;

import java.util.Collection;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * LongGaugeAdapter wraps an otel ObservableLongGauge
 */
class LongAsyncGaugeAdapter extends AbstractAsyncInstrument<ObservableLongGauge> implements LongAsyncGauge {

    LongAsyncGaugeAdapter(
        Meter meter,
        String name,
        String description,
        String unit,
        Supplier<Collection<LongWithAttributes>> observer,
        Consumer<AbstractInstrument<?>> deregisterFunc
    ) {
        super(meter, new Builder(name, description, unit, observer), deregisterFunc);
    }

    private static class Builder extends AbstractInstrument.Builder<ObservableLongGauge> {
        private final Supplier<Collection<LongWithAttributes>> observer;

        private Builder(String name, String description, String unit, Supplier<Collection<LongWithAttributes>> observer) {
            super(name, description, unit);
            this.observer = observer;
        }

        @Override
        public ObservableLongGauge build(Meter meter) {
            return Objects.requireNonNull(meter)
                .gaugeBuilder(name)
                .ofLongs()
                .setDescription(description)
                .setUnit(unit)
                .buildWithCallback(OtelHelper.longMeasurementCallback(name, observer));
        }
    }
}
