/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.telemetry.apm.AbstractInstrument;

import java.util.Map;
import java.util.Objects;

/**
 * LongUpDownCounterAdapter wraps an otel LongUpDownCounter
 */
public class LongUpDownCounterAdapter extends AbstractInstrument<LongUpDownCounter>
    implements
        org.elasticsearch.telemetry.metric.LongUpDownCounter {

    public LongUpDownCounterAdapter(Meter meter, String name, String description, String unit) {
        super(meter, name, description, unit);
    }

    @Override
    protected io.opentelemetry.api.metrics.LongUpDownCounter buildInstrument(Meter meter) {
        var builder = Objects.requireNonNull(meter).upDownCounterBuilder(getName());
        return builder.setDescription(getDescription()).setUnit(getUnit()).build();
    }

    @Override
    public void add(long inc) {
        getInstrument().add(inc);
    }

    @Override
    public void add(long inc, Map<String, Object> attributes) {
        getInstrument().add(inc, OtelHelper.fromMap(attributes));
    }
}
