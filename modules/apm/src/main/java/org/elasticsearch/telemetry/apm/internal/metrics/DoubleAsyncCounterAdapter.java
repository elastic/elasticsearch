/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleCounter;

import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.telemetry.apm.AbstractInstrument;
import org.elasticsearch.telemetry.metric.DoubleAsyncCounter;
import org.elasticsearch.telemetry.metric.DoubleWithAttributes;

import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

public class DoubleAsyncCounterAdapter extends AbstractInstrument<ObservableDoubleCounter> implements DoubleAsyncCounter {
    private final Supplier<DoubleWithAttributes> observer;
    private final ReleasableLock closedLock = new ReleasableLock(new ReentrantLock());
    private boolean closed = false;

    public DoubleAsyncCounterAdapter(Meter meter, String name, String description, String unit, Supplier<DoubleWithAttributes> observer) {
        super(meter, name, description, unit);
        this.observer = observer;
    }

    @Override
    protected ObservableDoubleCounter buildInstrument(Meter meter) {
        var builder = Objects.requireNonNull(meter).counterBuilder(getName());
        return builder.setDescription(getDescription()).setUnit(getUnit()).ofDoubles().buildWithCallback(measurement -> {
            DoubleWithAttributes observation;
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
