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
import org.elasticsearch.telemetry.MetricName;
import org.elasticsearch.telemetry.metric.Instrument;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractInstrument<T, I> implements Instrument {
    private final AtomicReference<I> instrument = new AtomicReference<>(getNOOP());
    private final MetricName name;
    private final String description;
    private final T unit;

    public AbstractInstrument(MetricName name, String description, T unit) {
        this.name = Objects.requireNonNull(name);
        this.description = Objects.requireNonNull(description);
        this.unit = Objects.requireNonNull(unit);
    }

    public MetricName getMetricName() {
        return name;
    }

    @Override
    public String getName() {
        return name.getRawName();
    }

    public String getUnit() {
        return unit.toString();
    }

    I getInstrument() {
        return instrument.get();
    }

    String getDescription() {
        return description;
    }

    public void setProvider(@Nullable Meter meter) {
        instrument.set(meter != null ? buildInstrument(meter) : getNOOP());
    }

    abstract I buildInstrument(Meter meter);

    protected abstract I getNOOP();
}
