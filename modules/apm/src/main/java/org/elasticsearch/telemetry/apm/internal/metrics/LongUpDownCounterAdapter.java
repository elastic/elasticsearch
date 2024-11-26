/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
        super(meter, new Builder(name, description, unit));
    }

    @Override
    public void add(long inc) {
        getInstrument().add(inc);
    }

    @Override
    public void add(long inc, Map<String, Object> attributes) {
        getInstrument().add(inc, OtelHelper.fromMap(attributes));
    }

    private static class Builder extends AbstractInstrument.Builder<LongUpDownCounter> {
        private Builder(String name, String description, String unit) {
            super(name, description, unit);
        }

        @Override
        public LongUpDownCounter build(Meter meter) {
            return Objects.requireNonNull(meter).upDownCounterBuilder(name).setDescription(description).setUnit(unit).build();
        }
    }
}
