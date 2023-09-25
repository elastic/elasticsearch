/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.telemetry.metric.Instrument;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractInstrument<T, I> implements Instrument {
    private final AtomicReference<I> instrumentRef;
    private final String name;
    private final String description;
    private final T unit;

    public AbstractInstrument(Meter meter, String name, String description, T unit) {
        this.name = Objects.requireNonNull(name);
        this.description = Objects.requireNonNull(description);
        this.unit = Objects.requireNonNull(unit);
        this.instrumentRef = new AtomicReference<>(buildInstrument(meter));
    }

    @Override
    public String getName() {
        return name;
    }

    public String getUnit() {
        return unit.toString();
    }

    I getInstrument() {
        return instrumentRef.get();
    }

    String getDescription() {
        return description;
    }

    void setProvider(@Nullable Meter meter) {
        instrumentRef.set(buildInstrument(Objects.requireNonNull(meter)));
    }

    abstract I buildInstrument(Meter meter);
}
