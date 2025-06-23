/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm;

import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.telemetry.apm.internal.MetricNameValidator;
import org.elasticsearch.telemetry.metric.Instrument;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * An instrument that contains the name, description and unit.  The delegate may be replaced when
 * the provider is updated.
 * Subclasses should implement the builder, which is used on initialization and provider updates.
 *
 * @param <T> delegated instrument
 */
public abstract class AbstractInstrument<T> implements Instrument {
    private final AtomicReference<T> delegate = new AtomicReference<>();
    private final String name;
    private final Function<Meter, T> instrumentBuilder;

    public AbstractInstrument(Meter meter, Builder<T> builder) {
        this.name = builder.getName();
        this.instrumentBuilder = m -> builder.build(m);
        this.delegate.set(this.instrumentBuilder.apply(meter));
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

    protected abstract static class Builder<T> {

        protected final String name;
        protected final String description;
        protected final String unit;

        public Builder(String name, String description, String unit) {
            this.name = MetricNameValidator.validate(name);
            this.description = Objects.requireNonNull(description);
            this.unit = Objects.requireNonNull(unit);
        }

        public String getName() {
            return name;
        }

        public abstract T build(Meter meter);
    }
}
