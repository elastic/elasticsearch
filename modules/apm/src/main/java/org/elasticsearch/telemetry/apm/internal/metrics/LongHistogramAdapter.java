/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.metrics.Meter;

import java.util.Map;
import java.util.Objects;

/**
 * LongHistogramAdapter wraps an otel LongHistogram
 */
public class LongHistogramAdapter extends AbstractInstrument<io.opentelemetry.api.metrics.LongHistogram>
    implements
        org.elasticsearch.telemetry.metric.LongHistogram {

    public LongHistogramAdapter(Meter meter, String name, String description, String unit) {
        super(meter, name, description, unit);
    }

    @Override
    io.opentelemetry.api.metrics.LongHistogram buildInstrument(Meter meter) {
        return Objects.requireNonNull(meter)
            .histogramBuilder(getName())
            .ofLongs()
            .setDescription(getDescription())
            .setUnit(getUnit())
            .build();
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
