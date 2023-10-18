/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry;

import org.elasticsearch.telemetry.metric.DoubleCounter;
import org.elasticsearch.telemetry.metric.DoubleGauge;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.DoubleUpDownCounter;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongGauge;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.LongUpDownCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

public class DelegatingMeterRegistry implements MeterRegistry {

    private final MeterRegistry delegate;

    public DelegatingMeterRegistry(MeterRegistry delegate) {
        this.delegate = delegate;
    }

    @Override
    public DoubleCounter registerDoubleCounter(String name, String description, String unit) {
        return delegate.registerDoubleCounter(name, description, unit);
    }

    @Override
    public DoubleCounter getDoubleCounter(String name) {
        return delegate.getDoubleCounter(name);
    }

    @Override
    public DoubleUpDownCounter registerDoubleUpDownCounter(String name, String description, String unit) {
        return delegate.registerDoubleUpDownCounter(name, description, unit);
    }

    @Override
    public DoubleUpDownCounter getDoubleUpDownCounter(String name) {
        return delegate.getDoubleUpDownCounter(name);
    }

    @Override
    public DoubleGauge registerDoubleGauge(String name, String description, String unit) {
        return delegate.registerDoubleGauge(name, description, unit);
    }

    @Override
    public DoubleGauge getDoubleGauge(String name) {
        return delegate.getDoubleGauge(name);
    }

    @Override
    public DoubleHistogram registerDoubleHistogram(String name, String description, String unit) {
        return delegate.registerDoubleHistogram(name, description, unit);
    }

    @Override
    public DoubleHistogram getDoubleHistogram(String name) {
        return delegate.getDoubleHistogram(name);
    }

    @Override
    public LongCounter registerLongCounter(String name, String description, String unit) {
        return delegate.registerLongCounter(name, description, unit);
    }

    @Override
    public LongCounter getLongCounter(String name) {
        return delegate.getLongCounter(name);
    }

    @Override
    public LongUpDownCounter registerLongUpDownCounter(String name, String description, String unit) {
        return delegate.registerLongUpDownCounter(name, description, unit);
    }

    @Override
    public LongUpDownCounter getLongUpDownCounter(String name) {
        return delegate.getLongUpDownCounter(name);
    }

    @Override
    public LongGauge registerLongGauge(String name, String description, String unit) {
        return delegate.registerLongGauge(name, description, unit);
    }

    @Override
    public LongGauge getLongGauge(String name) {
        return delegate.getLongGauge(name);
    }

    @Override
    public LongHistogram registerLongHistogram(String name, String description, String unit) {
        return delegate.registerLongHistogram(name, description, unit);
    }

    @Override
    public LongHistogram getLongHistogram(String name) {
        return delegate.getLongHistogram(name);
    }
}
