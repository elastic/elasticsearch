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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * LongGaugeAdapter wraps an otel ObservableLongMeasurement
 */
public class LongGaugeAdapter extends AbstractInstrument<io.opentelemetry.api.metrics.ObservableLongGauge>
    implements
        org.elasticsearch.telemetry.metric.LongGauge {
    private final AtomicLong value = new AtomicLong(0);
    private final AtomicReference<Map<String, Object>> attributes = new AtomicReference<>(Collections.emptyMap());

    public LongGaugeAdapter(Meter meter, String name, String description, String unit) {
        super(meter, name, description, unit);
    }

    @Override
    io.opentelemetry.api.metrics.ObservableLongGauge buildInstrument(Meter meter) {
        return Objects.requireNonNull(meter)
            .gaugeBuilder(getName())
            .ofLongs()
            .setDescription(getDescription())
            .setUnit(getUnit())
            .buildWithCallback(measurment -> measurment.record(value.get(), OtelHelper.fromMap(attributes.get())));
    }

    @Override
    public void record(long value) {
        this.value.set(value);
        this.attributes.set(Collections.emptyMap());
    }

    @Override
    public void record(long value, Map<String, Object> attributes) {
        this.value.set(value);
        this.attributes.set(attributes);
    }
}
