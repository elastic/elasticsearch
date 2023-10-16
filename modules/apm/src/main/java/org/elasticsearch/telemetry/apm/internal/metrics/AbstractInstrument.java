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

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An instrument that contains the name, description and unit.  The delegate may be replaced when
 * the provider is updated.
 * Subclasses should implement the builder, which is used on initialization and provider updates.
 * @param <T> delegated instrument
 */
public abstract class AbstractInstrument<T> implements Instrument {
    private final AtomicReference<T> delegate;
    private final String name;
    private final String description;
    private final String unit;

    public AbstractInstrument(Meter meter, String name, String description, String unit) {
        this.name = Objects.requireNonNull(name);
        this.description = Objects.requireNonNull(description);
        this.unit = Objects.requireNonNull(unit);
        this.delegate = new AtomicReference<>(doBuildInstrument(meter));
    }

    private T doBuildInstrument(Meter meter) {
        return AccessController.doPrivileged((PrivilegedAction<T>) () -> buildInstrument(meter));
    }

    @Override
    public String getName() {
        return name;
    }

    public String getUnit() {
        return unit.toString();
    }

    T getInstrument() {
        return delegate.get();
    }

    String getDescription() {
        return description;
    }

    void setProvider(@Nullable Meter meter) {
        delegate.set(doBuildInstrument(Objects.requireNonNull(meter)));
    }

    abstract T buildInstrument(Meter meter);
}
