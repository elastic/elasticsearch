/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.export.MetricExporter;

import java.util.function.LongSupplier;

/**
 * Base class for {@link MetricExporter} wrappers that decorate a delegate with self-instrumentation.
 */
abstract class DelegatingMetricExporter implements MetricExporter {

    protected final MetricExporter delegate;
    private final Meter meter;
    private final String prefix;

    protected DelegatingMetricExporter(MetricExporter delegate, Meter meter, String namespace) {
        this.delegate = delegate;
        this.meter = meter;
        this.prefix = "es.apm.metrics." + namespace + ".";
    }

    @Override
    public final AggregationTemporality getAggregationTemporality(InstrumentType instrumentType) {
        return delegate.getAggregationTemporality(instrumentType);
    }

    @Override
    public final Aggregation getDefaultAggregation(InstrumentType instrumentType) {
        return delegate.getDefaultAggregation(instrumentType);
    }

    protected final LongCounter counter(String suffix, String description) {
        return meter.counterBuilder(prefix + suffix).setDescription(description).setUnit("1").build();
    }

    protected final void longGauge(String suffix, String description, String unit, LongSupplier value) {
        meter.gaugeBuilder(prefix + suffix)
            .ofLongs()
            .setDescription(description)
            .setUnit(unit)
            .buildWithCallback(r -> r.record(value.getAsLong()));
    }
}
