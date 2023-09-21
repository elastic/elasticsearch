/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;

import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.telemetry.MetricName;

import java.util.Map;

public class OtelLongGauge<T> extends SwitchableInstrument<ObservableLongMeasurement> implements org.elasticsearch.telemetry.metric.LongGauge {
    private final LazyInitializable<ObservableLongMeasurement, RuntimeException> gauge;
    private final MetricName name;
    private final String description;
    private final T unit;

    private OtelLongGauge(
        LazyInitializable<ObservableLongMeasurement, RuntimeException> gauge,
        MetricName name,
        String description,
        T unit
    ) {
        super(null, null);

        this.gauge = gauge;
        this.name = name;
        this.description = description;
        this.unit = unit;
    }

    public static <T> OtelLongGauge<T> build(
        LazyInitializable<ObservableLongMeasurement, RuntimeException> lazyGauge,
        Meter meter,
        MetricName name,
        String description,
        T unit
    ) {
        return new OtelLongGauge<>(lazyGauge, name, description, unit);
    }

    @Override
    public MetricName getName() {
        return name;
    }

    @Override
    public void record(long value) {
        gauge.getOrCompute().record(value);
    }

    @Override
    public void record(long value, Map<String, Object> attributes) {
        gauge.getOrCompute().record(value, OtelHelper.fromMap(attributes));
    }
}
