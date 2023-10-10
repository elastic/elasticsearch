/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.metrics.Meter;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * DoubleGaugeAdapter wraps an otel ObservableDoubleMeasurement
 */
public class DoubleGaugeAdapter extends AbstractInstrument<io.opentelemetry.api.metrics.ObservableDoubleGauge>
    implements
        org.elasticsearch.telemetry.metric.DoubleGauge {

    private final AtomicReference<Double> value = new AtomicReference<>(0.0);
    private final AtomicReference<Map<String, Object>> attributes = new AtomicReference<>(Collections.emptyMap());

    public DoubleGaugeAdapter(Meter meter, String name, String description, String unit) {
        super(meter, name, description, unit);
    }

    @Override
    io.opentelemetry.api.metrics.ObservableDoubleGauge buildInstrument(Meter meter) {
        var builder = Objects.requireNonNull(meter).gaugeBuilder(getName());
        return builder.setDescription(getDescription())
            .setUnit(getUnit())
            .buildWithCallback(measurment -> measurment.record(value.get(), OtelHelper.fromMap(attributes.get())));
    }

    @Override
    public void record(double value) {
        this.value.set(value);
        this.attributes.set(Collections.emptyMap());
    }

    @Override
    public void record(double value, Map<String, Object> attributes) {
        this.value.set(value);
        this.attributes.set(attributes);
    }
}
