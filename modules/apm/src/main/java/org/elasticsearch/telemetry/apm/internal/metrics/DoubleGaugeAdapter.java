/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.metrics.Meter;

import java.util.Map;
import java.util.Objects;

/**
 * DoubleGaugeAdapter wraps an otel ObservableDoubleMeasurement
 */
public class DoubleGaugeAdapter extends AbstractInstrument<io.opentelemetry.api.metrics.ObservableDoubleMeasurement>
    implements
        org.elasticsearch.telemetry.metric.DoubleGauge {

    public DoubleGaugeAdapter(Meter meter, String name, String description, String unit) {
        super(meter, name, description, unit);
    }

    @Override
    io.opentelemetry.api.metrics.ObservableDoubleMeasurement buildInstrument(Meter meter) {
        var builder = Objects.requireNonNull(meter).gaugeBuilder(getName());
        return builder.setDescription(getDescription()).setUnit(getUnit()).buildObserver();
    }

    @Override
    public void record(double value) {
        getInstrument().record(value);
    }

    @Override
    public void record(double value, Map<String, Object> attributes) {
        getInstrument().record(value, OtelHelper.fromMap(attributes));
    }
}
