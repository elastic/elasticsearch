/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tracing.apm;

import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;

import org.elasticsearch.tracing.MetricName;

import java.util.Map;

public class OtelDoubleGauge<T> implements DoubleGauge {
    private final ObservableDoubleMeasurement gauge;
    private final MetricName name;
    private final String description;
    private final T unit;

    private OtelDoubleGauge(ObservableDoubleMeasurement gauge, MetricName name, String description, T unit) {
        this.gauge = gauge;
        this.name = name;
        this.description = description;
        this.unit = unit;
    }

    public static <T> OtelDoubleGauge<T> build(Meter meter, MetricName name, String description, T unit) {
        return new OtelDoubleGauge<>(
            meter.gaugeBuilder(name.getRawName()).setDescription(description).setUnit(unit.toString()).buildObserver(),
            name,
            description,
            unit
        );
    }

    @Override
    public void record(double value) {
        gauge.record(value);
    }

    @Override
    public void record(double value, Map<String, Object> attributes) {
        gauge.record(value, OtelHelper.fromMap(attributes));
    }
}
