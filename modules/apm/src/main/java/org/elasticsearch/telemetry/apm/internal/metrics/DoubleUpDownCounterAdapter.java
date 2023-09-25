/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.metrics.DoubleUpDownCounter;
import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.Map;
import java.util.Objects;

public class DoubleUpDownCounterAdapter<T> extends AbstractInstrument<T, DoubleUpDownCounter>
    implements
        org.elasticsearch.telemetry.metric.DoubleUpDownCounter {

    public DoubleUpDownCounterAdapter(Meter meter, String name, String description, T unit) {
        super(meter, name, description, unit);
    }

    @Override
    DoubleUpDownCounter buildInstrument(Meter meter) {
        return Objects.requireNonNull(meter)
            .upDownCounterBuilder(getName())
            .ofDoubles()
            .setDescription(getDescription())
            .setUnit(getUnit())
            .build();
    }

    @Override
    public void add(double inc) {
        getInstrument().add(inc);
    }

    @Override
    public void add(double inc, Map<String, Object> attributes) {
        getInstrument().add(inc, OtelHelper.fromMap(attributes));
    }

    @Override
    public void add(double inc, Map<String, Object> attributes, ThreadContext threadContext) {
        throw new UnsupportedOperationException("unimplemented");
    }
}
