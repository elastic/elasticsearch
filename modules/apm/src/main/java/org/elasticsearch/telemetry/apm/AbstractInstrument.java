/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm;

import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.telemetry.metric.Instrument;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * An instrument that contains the name, description and unit.  The delegate may be replaced when
 * the provider is updated.
 * Subclasses should implement the builder, which is used on initialization and provider updates.
 * @param <T> delegated instrument
 */
public abstract class AbstractInstrument<T> implements Instrument {
    private static final int MAX_NAME_LENGTH = 255;
    private final AtomicReference<T> delegate = new AtomicReference<>();
    private final String name;
    private final Function<Meter, T> instrumentBuilder;

    public AbstractInstrument(Meter meter, String name, Function<Meter, T> instrumentBuilder) {
        checkMaxLength(name);
        this.name = Objects.requireNonNull(name);
        this.instrumentBuilder = m -> AccessController.doPrivileged((PrivilegedAction<T>) () -> instrumentBuilder.apply(m));
        this.delegate.set(this.instrumentBuilder.apply(meter));
    }

    protected static void checkMaxLength(String name) {
        if (name.length() > MAX_NAME_LENGTH) {
            throw new IllegalArgumentException(
                "Instrument name [" + name + "] with length [" + name.length() + "] exceeds maximum length [" + MAX_NAME_LENGTH + "]"
            );
        }
    }

    @Override
    public String getName() {
        return name;
    }

    protected T getInstrument() {
        return delegate.get();
    }

    void setProvider(@Nullable Meter meter) {
        delegate.set(instrumentBuilder.apply(Objects.requireNonNull(meter)));
    }
}
