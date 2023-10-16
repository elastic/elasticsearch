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
import org.elasticsearch.telemetry.metric.DoubleAttributes;
import org.elasticsearch.telemetry.metric.DoubleCounter;
import org.elasticsearch.telemetry.metric.DoubleGauge;
import org.elasticsearch.telemetry.metric.DoubleGaugeObserver;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.DoubleUpDownCounter;
import org.elasticsearch.telemetry.metric.LongAttributes;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongGauge;
import org.elasticsearch.telemetry.metric.LongGaugeObserver;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.LongUpDownCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Container for registering and fetching meterRegistrar by type and name.
 * Instrument names must be unique for a given type on registration.
 * {@link #setProvider(Meter)} is used to change the provider for all existing meterRegistrar.
 */
public class APMMeterRegistry implements MeterRegistry {
    private final Registrar<DoubleCounterAdapter> doubleCounters = new Registrar<>();
    private final Registrar<DoubleUpDownCounterAdapter> doubleUpDownCounters = new Registrar<>();
    private final Registrar<DoubleGaugeAdapter> doubleGauges = new Registrar<>();
    private final Registrar<DoubleGaugeObserverAdapter> doubleGaugeObservers = new Registrar<>();
    private final Registrar<DoubleHistogramAdapter> doubleHistograms = new Registrar<>();
    private final Registrar<LongCounterAdapter> longCounters = new Registrar<>();
    private final Registrar<LongUpDownCounterAdapter> longUpDownCounters = new Registrar<>();
    private final Registrar<LongGaugeAdapter> longGauges = new Registrar<>();
    private final Registrar<LongGaugeObserverAdapter> longGaugeObservers = new Registrar<>();
    private final Registrar<LongHistogramAdapter> longHistograms = new Registrar<>();

    private final Meter meter;

    public APMMeterRegistry(Meter meter) {
        this.meter = meter;
    }

    private final List<Registrar<?>> registrars = List.of(
        doubleCounters,
        doubleUpDownCounters,
        doubleGauges,
        doubleGaugeObservers,
        doubleHistograms,
        longCounters,
        longUpDownCounters,
        longGauges,
        longGaugeObservers,
        longHistograms
    );

    // Access to registration has to be restricted when the provider is updated in ::setProvider
    protected final ReleasableLock registerLock = new ReleasableLock(new ReentrantLock());

    public DoubleCounter registerDoubleCounter(String name, String description, String unit) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return doubleCounters.register(new DoubleCounterAdapter(meter, name, description, unit));
        }
    }

    public DoubleCounter getDoubleCounter(String name) {
        return doubleCounters.get(name);
    }

    public DoubleUpDownCounter registerDoubleUpDownCounter(String name, String description, String unit) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return doubleUpDownCounters.register(new DoubleUpDownCounterAdapter(meter, name, description, unit));
        }
    }

    public DoubleUpDownCounter getDoubleUpDownCounter(String name) {
        return doubleUpDownCounters.get(name);
    }

    public DoubleGauge registerDoubleGauge(String name, String description, String unit) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return doubleGauges.register(new DoubleGaugeAdapter(meter, name, description, unit));
        }
    }

    public DoubleGauge getDoubleGauge(String name) {
        return doubleGauges.get(name);
    }

    public DoubleGaugeObserver registerDoubleGaugeObserver(String name, String description, String unit, DoubleSupplier observed) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return doubleGaugeObservers.register(new DoubleGaugeObserverAdapter(meter, name, description, unit, observed));
        }
    }

    public DoubleGaugeObserver registerDoubleGaugeObserver(
        String name,
        String description,
        String unit,
        DoubleSupplier observed,
        Map<String, Object> attributes
    ) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return doubleGaugeObservers.register(new DoubleGaugeObserverAdapter(meter, name, description, unit, observed, attributes));
        }
    }

    public DoubleGaugeObserver registerDoubleGaugeObserver(
        String name,
        String description,
        String unit,
        Supplier<DoubleAttributes> observed
    ) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return doubleGaugeObservers.register(new DoubleGaugeObserverAdapter(meter, name, description, unit, observed));
        }
    }

    public DoubleGaugeObserver getDoubleGaugeObserver(String name) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return doubleGaugeObservers.get(name);
        }
    }

    public DoubleHistogram registerDoubleHistogram(String name, String description, String unit) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return doubleHistograms.register(new DoubleHistogramAdapter(meter, name, description, unit));
        }
    }

    public DoubleHistogram getDoubleHistogram(String name) {
        return doubleHistograms.get(name);
    }

    public LongCounter registerLongCounter(String name, String description, String unit) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return longCounters.register(new LongCounterAdapter(meter, name, description, unit));
        }
    }

    public LongCounter getLongCounter(String name) {
        return longCounters.get(name);
    }

    public LongUpDownCounter registerLongUpDownCounter(String name, String description, String unit) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return longUpDownCounters.register(new LongUpDownCounterAdapter(meter, name, description, unit));
        }
    }

    public LongUpDownCounter getLongUpDownCounter(String name) {
        return longUpDownCounters.get(name);
    }

    public LongGauge registerLongGauge(String name, String description, String unit) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return longGauges.register(new LongGaugeAdapter(meter, name, description, unit));
        }
    }

    public LongGauge getLongGauge(String name) {
        return longGauges.get(name);
    }

    public LongGaugeObserver registerLongGaugeObserver(String name, String description, String unit, LongSupplier observed) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return longGaugeObservers.register(new LongGaugeObserverAdapter(meter, name, description, unit, observed));
        }
    }

    public LongGaugeObserver registerLongGaugeObserver(
        String name,
        String description,
        String unit,
        LongSupplier observed,
        Map<String, Object> attributes
    ) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return longGaugeObservers.register(new LongGaugeObserverAdapter(meter, name, description, unit, observed, attributes));
        }
    }

    public LongGaugeObserver registerLongGaugeObserver(String name, String description, String unit, Supplier<LongAttributes> observed) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return longGaugeObservers.register(new LongGaugeObserverAdapter(meter, name, description, unit, observed));
        }
    }

    public LongGaugeObserver getLongGaugeObserver(String name) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return longGaugeObservers.get(name);
        }
    }

    public LongHistogram registerLongHistogram(String name, String description, String unit) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return longHistograms.register(new LongHistogramAdapter(meter, name, description, unit));
        }
    }

    public LongHistogram getLongHistogram(String name) {
        return longHistograms.get(name);
    }

    public void setProvider(Meter meter) {
        try (ReleasableLock lock = registerLock.acquire()) {
            for (Registrar<?> registrar : registrars) {
                registrar.setProvider(meter);
            }
        }
    }

    /**
     * A typed wrapper for a instrument that
     * @param <T>
     */
    private static class Registrar<T extends AbstractInstrument<?>> {
        private final Map<String, T> registered = ConcurrentCollections.newConcurrentMap();

        T register(T instrument) {
            registered.compute(instrument.getName(), (k, v) -> {
                if (v != null) {
                    throw new IllegalStateException(
                        instrument.getClass().getSimpleName() + "[" + instrument.getName() + "] already registered"
                    );
                }

                return instrument;
            });
            return instrument;
        }

        T get(String name) {
            return registered.get(name);
        }

        void setProvider(Meter meter) {
            registered.forEach((k, v) -> v.setProvider(meter));
        }
    }

    // scope for testing
    Meter getMeter() {
        return meter;
    }
}
