/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;

import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.telemetry.apm.AbstractInstrument;
import org.elasticsearch.telemetry.metric.DoubleAttributes;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * DoubleGaugeObserverAdapter wraps a otel ObservableDoubleGauge and accepts values via callbacks.
 */
public class DoubleGaugeObserverAdapter extends AbstractInstrument<ObservableDoubleGauge>
    implements
        org.elasticsearch.telemetry.metric.DoubleGaugeObserver {
    private final AtomicReference<Supplier<DoubleAttributes>> observer = new AtomicReference<>();
    private final ReleasableLock closedLock = new ReleasableLock(new ReentrantLock());
    private boolean closed = false;

    public DoubleGaugeObserverAdapter(Meter meter, String name, String description, String unit) {
        super(meter, name, description, unit);
    }

    public void setObserver(Supplier<DoubleAttributes> observer) {
        this.observer.set(observer);
    }

    @Override
    protected io.opentelemetry.api.metrics.ObservableDoubleGauge buildInstrument(Meter meter) {
        return Objects.requireNonNull(meter)
            .gaugeBuilder(getName())
            .setDescription(getDescription())
            .setUnit(getUnit())
            .buildWithCallback(measurement -> {
                Supplier<DoubleAttributes> currentObserver = observer.get();
                if (currentObserver == null) {
                    return;
                }
                DoubleAttributes observation = currentObserver.get();
                measurement.record(observation.value(), OtelHelper.fromMap(observation.attributes()));
            });
    }

    @Override
    public void close() throws Exception {
        try (ReleasableLock lock = closedLock.acquire()) {
            if (closed == false) {
                getInstrument().close();
            }
            observer.set(null);
            closed = true;
        }
    }
}
