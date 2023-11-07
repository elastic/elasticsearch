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

/**
 * An instrument that contains the name, description and unit.  The delegate may be replaced when
 * the provider is updated.
 * Subclasses should implement the builder, which is used on initialization and provider updates.
 * @param <T> delegated instrument
 */
public abstract class AbstractInstrument<T> implements Instrument {
    private static final int MAX_NAME_LENGTH = 63; // TODO(stu): change to 255 when we upgrade to otel 1.30+, see #101679
    private final AtomicReference<T> delegate;
    private final String name;
    private final String description;
    private final String unit;

    @SuppressWarnings("this-escape")
    public AbstractInstrument(Meter meter, String name, String description, String unit) {
        this.name = Objects.requireNonNull(name);
        if (name.length() > MAX_NAME_LENGTH) {
            throw new IllegalArgumentException(
                "Instrument name [" + name + "] with length [" + name.length() + "] exceeds maximum length [" + MAX_NAME_LENGTH + "]"
            );
        }
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

    protected T getInstrument() {
        return delegate.get();
    }

    protected String getDescription() {
        return description;
    }

    void setProvider(@Nullable Meter meter) {
        delegate.set(doBuildInstrument(Objects.requireNonNull(meter)));
    }

    protected abstract T buildInstrument(Meter meter);
}
