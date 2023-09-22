/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;

import org.elasticsearch.telemetry.metric.MetricName;

import java.util.Map;
import java.util.Objects;

public class DoubleGaugeAdapter<T> extends AbstractInstrument<T, ObservableDoubleMeasurement>
    implements
        org.elasticsearch.telemetry.metric.DoubleGauge {

    public DoubleGaugeAdapter(Meter meter, MetricName name, String description, T unit) {
        super(meter, name, description, unit);
    }

    @Override
    ObservableDoubleMeasurement buildInstrument(Meter meter) {
        return Objects.requireNonNull(meter).gaugeBuilder(getName()).setDescription(getDescription()).setUnit(getUnit()).buildObserver();
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
