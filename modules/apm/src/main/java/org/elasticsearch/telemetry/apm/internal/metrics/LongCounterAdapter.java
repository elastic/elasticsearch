/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.context.Context;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.telemetry.MetricName;

import java.util.Map;
import java.util.Objects;

public class LongCounterAdapter<T> extends AbstractInstrument<T, LongCounter> implements org.elasticsearch.telemetry.metric.LongCounter {
    private static final LongCounter NOOP_INSTANCE = new Noop();

    public LongCounterAdapter(MetricName name, String description, T unit) {
        super(name, description, unit);
    }

    @Override
    LongCounter buildInstrument(Meter meter) {
        return Objects.requireNonNull(meter).counterBuilder(getName()).setDescription(getDescription()).setUnit(getUnit()).build();
    }

    @Override
    protected LongCounter getNOOP() {
        return NOOP_INSTANCE;
    }

    @Override
    public void increment() {
        getInstrument().add(1L);
    }

    @Override
    public void incrementBy(long inc) {
        assert inc >= 0;
        getInstrument().add(inc);
    }

    @Override
    public void incrementBy(long inc, Map<String, Object> attributes) {
        assert inc >= 0;
        getInstrument().add(inc, OtelHelper.fromMap(attributes));
    }

    @Override
    public void incrementBy(double inc, Map<String, Object> attributes, ThreadContext threadContext) {
        throw new UnsupportedOperationException("unimplemented");
    }

    private static class Noop implements io.opentelemetry.api.metrics.LongCounter {
        @Override
        public void add(long value) {

        }

        @Override
        public void add(long value, Attributes attributes) {

        }

        @Override
        public void add(long value, Attributes attributes, Context context) {

        }
    }
}
