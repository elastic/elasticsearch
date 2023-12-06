/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.telemetry.apm.AbstractInstrument;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * LongHistogramAdapter wraps an otel LongHistogram
 */
public class LongHistogramAdapter extends AbstractInstrument<LongHistogram> implements org.elasticsearch.telemetry.metric.LongHistogram {
    public LongHistogramAdapter(Meter meter, String name, String description, String unit) {
        super(
            meter,
            name,
            instrumentBuilder(Objects.requireNonNull(name), Objects.requireNonNull(description), Objects.requireNonNull(unit))
        );
    }

    protected static Function<Meter, LongHistogram> instrumentBuilder(String name, String description, String unit) {
        return meter -> Objects.requireNonNull(meter).histogramBuilder(name).ofLongs().setDescription(description).setUnit(unit).build();
    }

    @Override
    public void record(long value) {
        getInstrument().record(value);
    }

    @Override
    public void record(long value, Map<String, Object> attributes) {
        getInstrument().record(value, OtelHelper.fromMap(attributes));
    }
}
