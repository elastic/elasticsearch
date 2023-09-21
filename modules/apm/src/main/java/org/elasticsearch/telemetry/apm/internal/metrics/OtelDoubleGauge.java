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

import org.elasticsearch.telemetry.MetricName;
import org.elasticsearch.telemetry.metric.DoubleGauge;

import java.util.Map;
import java.util.function.Function;

public class OtelDoubleGauge<T> extends SwitchableInstrument<ObservableDoubleMeasurement> implements DoubleGauge {
    private final MetricName name;
    private final String description;
    private final T unit;

    private OtelDoubleGauge(
        Function<Meter, ObservableDoubleMeasurement> instrumentProducer,
        Meter meter,
        MetricName name,
        String description,
        T unit
    ) {
        super(instrumentProducer, meter);
        this.name = name;
        this.description = description;
        this.unit = unit;
    }

    public static <T> OtelDoubleGauge<T> build(Meter meter, MetricName name, String description, T unit) {
        return new OtelDoubleGauge<>((m) -> createInstrument(meter, name, description, unit), meter, name, description, unit);
    }

    private static <T> ObservableDoubleMeasurement createInstrument(Meter meter, MetricName name, String description, T unit) {
        return meter.gaugeBuilder(name.getRawName()).setDescription(description).setUnit(unit.toString()).buildObserver();
    }

    @Override
    public MetricName getName() {
        return name;
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
