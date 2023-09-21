/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.telemetry.MetricName;
import org.elasticsearch.telemetry.metric.DoubleCounter;
import org.elasticsearch.telemetry.metric.DoubleGauge;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.DoubleUpDownCounter;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongGauge;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.LongUpDownCounter;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class Instruments {
    private final Registrar<DoubleCounterAdapter<?>> doubleCounters = new Registrar<>();
    private final Registrar<DoubleUpDownCounterAdapter<?>> doubleUpDownCounters = new Registrar<>();
    private final Registrar<DoubleGaugeAdapter<?>> doubleGauges = new Registrar<>();
    private final Registrar<DoubleHistogramAdapter<?>> doubleHistograms = new Registrar<>();
    private final Registrar<LongCounterAdapter<?>> longCounters = new Registrar<>();
    private final Registrar<LongUpDownCounterAdapter<?>> longUpDownCounters = new Registrar<>();
    private final Registrar<LongGaugeAdapter<?>> longGauges = new Registrar<>();
    private final Registrar<LongHistogramAdapter<?>> longHistograms = new Registrar<>();

    private final List<Registrar<?>> registrars = List.of(
        doubleCounters,
        doubleUpDownCounters,
        doubleGauges,
        doubleHistograms,
        longCounters,
        longUpDownCounters,
        longGauges,
        longHistograms
    );

    protected final ReleasableLock registerLock = new ReleasableLock(new ReentrantLock());

    public <T> DoubleCounter registerDoubleCounter(MetricName name, String description, T unit) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return doubleCounters.register(new DoubleCounterAdapter<>(name, description, unit));
        }
    }

    public <T> DoubleUpDownCounter registerDoubleUpDownCounter(MetricName name, String description, T unit) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return doubleUpDownCounters.register(new DoubleUpDownCounterAdapter<>(name, description, unit));
        }
    }

    public <T> DoubleGauge registerDoubleGauge(MetricName name, String description, T unit) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return doubleGauges.register(new DoubleGaugeAdapter<>(name, description, unit));
        }
    }

    public <T> DoubleHistogram registerDoubleHistogram(MetricName name, String description, T unit) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return doubleHistograms.register(new DoubleHistogramAdapter<>(name, description, unit));
        }
    }

    public <T> LongCounter registerLongCounter(MetricName name, String description, T unit) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return longCounters.register(new LongCounterAdapter<>(name, description, unit));
        }
    }

    public <T> LongUpDownCounter registerLongUpDownCounter(MetricName name, String description, T unit) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return longUpDownCounters.register(new LongUpDownCounterAdapter<>(name, description, unit));
        }
    }

    public <T> LongGauge registerLongGauge(MetricName name, String description, T unit) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return longGauges.register(new LongGaugeAdapter<>(name, description, unit));
        }
    }

    public <T> LongHistogram registerLongHistogram(MetricName name, String description, T unit) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return longHistograms.register(new LongHistogramAdapter<>(name, description, unit));
        }
    }

    public void setProvider(Meter meter) {
        try (ReleasableLock lock = registerLock.acquire()) {
            for (Registrar<?> registrar : registrars) {
                registrar.setProvider(meter);
            }
        }
    }

    private static class Registrar<T extends AbstractInstrument<?, ?>> {
        private final Map<MetricName, T> registered = ConcurrentCollections.newConcurrentMap();

        T register(T instrument) {
            registered.compute(instrument.getMetricName(), (k, v) -> {
                if (v != null) {
                    throw new IllegalStateException(
                        instrument.getClass().getSimpleName() + "[" + instrument.getName() + "] already registered"
                    );
                }

                return instrument;
            });
            return instrument;
        }

        T get(MetricName name) {
            return registered.get(name);
        }

        void setProvider(Meter meter) {
            registered.forEach((k, v) -> v.setProvider(meter));
        }
    }
}
