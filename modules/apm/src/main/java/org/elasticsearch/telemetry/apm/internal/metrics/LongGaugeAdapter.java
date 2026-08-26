/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.metrics.LongGauge;
import io.opentelemetry.api.metrics.Meter;

import java.util.Map;
import java.util.Objects;

class LongGaugeAdapter extends AbstractInstrument<LongGauge> implements org.elasticsearch.telemetry.metric.LongGauge {
    LongGaugeAdapter(Meter meter, String name, String description, String unit) {
        super(meter, new Builder(name, description, unit));
    }

    @Override
    public void set(long value) {
        getInstrument().set(value);
    }

    @Override
    public void set(long value, Map<String, Object> attributes) {
        getInstrument().set(value, OtelHelper.fromMap(getName(), attributes));
    }

    private static class Builder extends AbstractInstrument.Builder<LongGauge> {
        private Builder(String name, String description, String unit) {
            super(name, description, unit);
        }

        @Override
        public LongGauge build(Meter meter) {
            return Objects.requireNonNull(meter).gaugeBuilder(name).ofLongs().setDescription(description).setUnit(unit).build();
        }
    }
}
