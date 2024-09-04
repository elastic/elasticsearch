/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;

import org.elasticsearch.telemetry.apm.AbstractInstrument;
import org.elasticsearch.telemetry.metric.LongWithAttributes;

import java.util.Collection;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * LongGaugeAdapter wraps an otel ObservableLongGauge
 */
public class LongGaugeAdapter extends AbstractInstrument<ObservableLongGauge> implements org.elasticsearch.telemetry.metric.LongGauge {
    public LongGaugeAdapter(Meter meter, String name, String description, String unit, Supplier<Collection<LongWithAttributes>> observer) {
        super(meter, new Builder(name, description, unit, observer));
    }

    @Override
    public void close() throws Exception {
        getInstrument().close();
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
                .buildWithCallback(OtelHelper.longMeasurementCallback(observer));
        }
    }
}
