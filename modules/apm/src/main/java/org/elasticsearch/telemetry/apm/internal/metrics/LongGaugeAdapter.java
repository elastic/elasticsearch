/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;

import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.telemetry.apm.AbstractInstrument;
import org.elasticsearch.telemetry.metric.LongWithAttributes;

import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * LongGaugeAdapter wraps an otel ObservableLongGauge
 */
public class LongGaugeAdapter extends AbstractInstrument<ObservableLongGauge> implements org.elasticsearch.telemetry.metric.LongGauge {
    private final Supplier<LongWithAttributes> observer;
    private final ReleasableLock closedLock = new ReleasableLock(new ReentrantLock());
    private boolean closed = false;

    public LongGaugeAdapter(Meter meter, String name, String description, String unit, Supplier<LongWithAttributes> observer) {
        super(meter, name, description, unit);
        this.observer = observer;
    }

    @Override
    protected io.opentelemetry.api.metrics.ObservableLongGauge buildInstrument(Meter meter) {
        return Objects.requireNonNull(meter)
            .gaugeBuilder(getName())
            .ofLongs()
            .setDescription(getDescription())
            .setUnit(getUnit())
            .buildWithCallback(measurement -> {
                LongWithAttributes observation;
                try {
                    observation = observer.get();
                } catch (RuntimeException err) {
                    assert false : "observer must not throw [" + err.getMessage() + "]";
                    return;
                }
                measurement.record(observation.value(), OtelHelper.fromMap(observation.attributes()));
            });
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
}
