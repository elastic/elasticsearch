/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.Meter;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * DoubleHistogramAdapter wraps an otel DoubleHistogram
 */
class DoubleHistogramAdapter extends AbstractInstrument<DoubleHistogram> implements org.elasticsearch.telemetry.metric.DoubleHistogram {

    DoubleHistogramAdapter(Meter meter, String name, String description, String unit) {
        super(meter, new Builder(name, description, unit, HistogramBuckets.APM_DEFAULT));
    }

    DoubleHistogramAdapter(Meter meter, String name, String description, String unit, List<Double> bucketBoundaries) {
        super(meter, new Builder(name, description, unit, bucketBoundaries));
    }

    @Override
    public void record(double value) {
        getInstrument().record(value);
    }

    @Override
    public void record(double value, Map<String, Object> attributes) {
        getInstrument().record(value, OtelHelper.fromMap(getName(), attributes));
    }

    private static class Builder extends AbstractInstrument.Builder<DoubleHistogram> {
        private final List<Double> bucketBoundaries;

        private Builder(String name, String description, String unit, List<Double> bucketBoundaries) {
            super(name, description, unit);
            this.bucketBoundaries = bucketBoundaries;
        }

        @Override
        public DoubleHistogram build(Meter meter) {
            return Objects.requireNonNull(meter)
                .histogramBuilder(name)
                .setDescription(description)
                .setUnit(unit)
                .setExplicitBucketBoundariesAdvice(bucketBoundaries)
                .build();
        }
    }
}
