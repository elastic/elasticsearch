/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.common.util.LazyInitializable;

import java.util.function.Function;

public abstract class SwitchableInstrument<T> {
    private static final Meter NOOP_METER = OpenTelemetry.noop().getMeter("elasticsearch");
    private final LazyInitializable<T, RuntimeException> instrument;
    private final T noopInstrument;
    private volatile boolean enabled;

    public SwitchableInstrument(Function<Meter, T> instrumentProducer, Meter meter) {
        this.instrument = new LazyInitializable<>(() -> instrumentProducer.apply(meter));
        this.noopInstrument = instrumentProducer.apply(NOOP_METER);
    }

    void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public T getInstrument() {
        if (enabled) {
            return instrument.getOrCompute();
        } else {
            return noopInstrument;
        }
    }

}
