/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.metrics.DoubleUpDownCounter;
import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.telemetry.apm.AbstractInstrument;

import java.util.Map;
import java.util.Objects;

/**
 * DoubleUpDownCounterAdapter wraps an otel DoubleUpDownCounter
 */
public class DoubleUpDownCounterAdapter extends AbstractInstrument<DoubleUpDownCounter>
    implements
        org.elasticsearch.telemetry.metric.DoubleUpDownCounter {

    public DoubleUpDownCounterAdapter(Meter meter, String name, String description, String unit) {
        super(meter, new Builder(name, description, unit));
    }

    @Override
    public void add(double inc) {
        getInstrument().add(inc);
    }

    @Override
    public void add(double inc, Map<String, Object> attributes) {
        getInstrument().add(inc, OtelHelper.fromMap(attributes));
    }

    private static class Builder extends AbstractInstrument.Builder<DoubleUpDownCounter> {
        private Builder(String name, String description, String unit) {
            super(name, description, unit);
        }

        @Override
        public DoubleUpDownCounter build(Meter meter) {
            return Objects.requireNonNull(meter).upDownCounterBuilder(name).ofDoubles().setDescription(description).setUnit(unit).build();
        }
    }
}
