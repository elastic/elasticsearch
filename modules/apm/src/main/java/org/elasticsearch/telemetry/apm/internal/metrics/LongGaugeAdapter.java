/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;

import org.elasticsearch.telemetry.MetricName;

import java.util.Map;
import java.util.Objects;

public class LongGaugeAdapter<T> extends AbstractInstrument<T, ObservableLongMeasurement>
    implements
        org.elasticsearch.telemetry.metric.LongGauge {

    private static final ObservableLongMeasurement NOOP_INSTANCE = new Noop();

    public LongGaugeAdapter(MetricName name, String description, T unit) {
        super(name, description, unit);
    }

    @Override
    ObservableLongMeasurement buildInstrument(Meter meter) {
        return Objects.requireNonNull(meter)
            .gaugeBuilder(getName())
            .ofLongs()
            .setDescription(getDescription())
            .setUnit(getUnit())
            .buildObserver();
    }

    @Override
    protected ObservableLongMeasurement getNOOP() {
        return NOOP_INSTANCE;
    }

    @Override
    public void record(long value) {
        getInstrument().record(value);
    }

    @Override
    public void record(long value, Map<String, Object> attributes) {
        getInstrument().record(value, OtelHelper.fromMap(attributes));
    }

    private static class Noop implements io.opentelemetry.api.metrics.ObservableLongMeasurement {
        @Override
        public void record(long value) {

        }

        @Override
        public void record(long value, Attributes attributes) {

        }
    }
}
