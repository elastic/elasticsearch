/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics.internal.metrics;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleGaugeBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;

import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.telemetry.metric.DoubleAttributes;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.DoubleSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * DoubleGaugeObserverAdapter wraps a otel ObservableDoubleGauge and accepts values via callbacks.
 */
public class DoubleGaugeObserverAdapter extends AbstractInstrument<io.opentelemetry.api.metrics.ObservableDoubleGauge>
    implements
        org.elasticsearch.telemetry.metric.DoubleGaugeObserver {
    private final Function<DoubleGaugeBuilder, ObservableDoubleGauge> setCallback;
    private final ReleasableLock closedLock = new ReleasableLock(new ReentrantLock());
    private boolean closed = false;

    public DoubleGaugeObserverAdapter(Meter meter, String name, String description, String unit, DoubleSupplier observe) {
        super(meter, name, description, unit);
        setCallback = b -> b.buildWithCallback(cb -> cb.record(observe.getAsDouble()));
    }

    public DoubleGaugeObserverAdapter(
        Meter meter,
        String name,
        String description,
        String unit,
        DoubleSupplier observe,
        Map<String, Object> attributes
    ) {
        super(meter, name, description, unit);
        if (Objects.requireNonNull(attributes).isEmpty()) {
            throw new IllegalArgumentException("attributes must non-empty");
        }
        Attributes at = OtelHelper.fromMap(attributes);
        setCallback = b -> b.buildWithCallback(cb -> cb.record(observe.getAsDouble(), at));
        setProvider(meter);
    }

    public DoubleGaugeObserverAdapter(Meter meter, String name, String description, String unit, Supplier<DoubleAttributes> observe) {
        super(meter, name, description, unit);
        setCallback = b -> {
            DoubleAttributes da = observe.get();
            return b.buildWithCallback(cb -> cb.record(da.value(), OtelHelper.fromMap(da.attributes())));
        };
    }

    private static DoubleGaugeBuilder builder(Meter meter, String name, String description, String unit) {
        return Objects.requireNonNull(meter).gaugeBuilder(name).setDescription(description).setUnit(unit);
    }

    @Override
    public void close() throws Exception {
        try (ReleasableLock lock = closedLock.acquire()) {
            if (closed == false) {
                getInstrument().close();
            }
            closed = true;
        }
    }

    @Override
    ObservableDoubleGauge buildInstrument(Meter meter) {
        if (closedLock == null) {
            return null;
        }
        try (ReleasableLock lock = closedLock.acquire()) {
            if (closed) {
                return null;
            }
            return setCallback.apply(builder(meter, getName(), getDescription(), getUnit()));
        }
    }
}
