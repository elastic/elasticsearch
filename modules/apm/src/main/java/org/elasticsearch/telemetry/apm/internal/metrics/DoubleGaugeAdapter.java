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

/**
 * DoubleGaugeAdapter wraps an otel ObservableDoubleMeasurement
 */
public class DoubleGaugeAdapter extends AbstractGaugeAdapter<io.opentelemetry.api.metrics.ObservableDoubleGauge, Double>
    implements
        org.elasticsearch.telemetry.metric.DoubleGauge {

    public DoubleGaugeAdapter(Meter meter, String name, String description, String unit) {
        super(meter, name, description, unit);
    }

    @Override
    io.opentelemetry.api.metrics.ObservableDoubleGauge buildInstrument(Meter meter) {
        return Objects.requireNonNull(meter)
            .gaugeBuilder(getName())
            .setDescription(getDescription())
            .setUnit(getUnit())
            .buildWithCallback(measurement -> popRecords().forEach((attributes, value) -> measurement.record(value, attributes)));
    }

    @Override
    public void record(double value) {
        record(value, Collections.emptyMap());
    }

    @Override
    public void record(double value, Map<String, Object> attributes) {
        record(value, OtelHelper.fromMap(attributes));
    }
}
