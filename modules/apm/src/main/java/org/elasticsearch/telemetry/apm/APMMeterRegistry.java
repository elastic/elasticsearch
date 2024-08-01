/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm;

import io.opentelemetry.api.metrics.Meter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.telemetry.apm.internal.metrics.DoubleAsyncCounterAdapter;
import org.elasticsearch.telemetry.apm.internal.metrics.DoubleCounterAdapter;
import org.elasticsearch.telemetry.apm.internal.metrics.DoubleGaugeAdapter;
import org.elasticsearch.telemetry.apm.internal.metrics.DoubleHistogramAdapter;
import org.elasticsearch.telemetry.apm.internal.metrics.DoubleUpDownCounterAdapter;
import org.elasticsearch.telemetry.apm.internal.metrics.LongAsyncCounterAdapter;
import org.elasticsearch.telemetry.apm.internal.metrics.LongCounterAdapter;
import org.elasticsearch.telemetry.apm.internal.metrics.LongGaugeAdapter;
import org.elasticsearch.telemetry.apm.internal.metrics.LongHistogramAdapter;
import org.elasticsearch.telemetry.apm.internal.metrics.LongUpDownCounterAdapter;
import org.elasticsearch.telemetry.metric.DoubleAsyncCounter;
import org.elasticsearch.telemetry.metric.DoubleCounter;
import org.elasticsearch.telemetry.metric.DoubleGauge;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.DoubleUpDownCounter;
import org.elasticsearch.telemetry.metric.DoubleWithAttributes;
import org.elasticsearch.telemetry.metric.LongAsyncCounter;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongGauge;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.LongUpDownCounter;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Container for registering and fetching meterRegistrar by type and name.
 * Instrument names must be unique for a given type on registration.
 * {@link #setProvider(Meter)} is used to change the provider for all existing meterRegistrar.
 */
public class APMMeterRegistry implements MeterRegistry {
    private static final Logger logger = LogManager.getLogger(APMMeterRegistry.class);
    private final Registrar<DoubleCounterAdapter> doubleCounters = new Registrar<>();
    private final Registrar<DoubleAsyncCounterAdapter> doubleAsynchronousCounters = new Registrar<>();
    private final Registrar<DoubleUpDownCounterAdapter> doubleUpDownCounters = new Registrar<>();
    private final Registrar<DoubleGaugeAdapter> doubleGauges = new Registrar<>();
    private final Registrar<DoubleHistogramAdapter> doubleHistograms = new Registrar<>();
    private final Registrar<LongCounterAdapter> longCounters = new Registrar<>();
    private final Registrar<LongAsyncCounterAdapter> longAsynchronousCounters = new Registrar<>();
    private final Registrar<LongUpDownCounterAdapter> longUpDownCounters = new Registrar<>();
    private final Registrar<LongGaugeAdapter> longGauges = new Registrar<>();
    private final Registrar<LongHistogramAdapter> longHistograms = new Registrar<>();

    private Meter meter;

    public APMMeterRegistry(Meter meter) {
        this.meter = meter;
    }

    private final List<Registrar<?>> registrars = List.of(
        doubleCounters,
        doubleAsynchronousCounters,
        doubleUpDownCounters,
        doubleGauges,
        doubleHistograms,
        longCounters,
        longAsynchronousCounters,
        longUpDownCounters,
        longGauges,
        longHistograms
    );

    // Access to registration has to be restricted when the provider is updated in ::setProvider
    protected final ReleasableLock registerLock = new ReleasableLock(new ReentrantLock());

    @Override
    public DoubleCounter registerDoubleCounter(String name, String description, String unit) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return register(doubleCounters, new DoubleCounterAdapter(meter, name, description, unit));
        }
    }

    @Override
    public DoubleCounter getDoubleCounter(String name) {
        return doubleCounters.get(name);
    }

    @Override
    public DoubleAsyncCounter registerDoublesAsyncCounter(
        String name,
        String description,
        String unit,
        Supplier<Collection<DoubleWithAttributes>> observer
    ) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return register(doubleAsynchronousCounters, new DoubleAsyncCounterAdapter(meter, name, description, unit, observer));
        }
    }

    @Override
    public DoubleAsyncCounter getDoubleAsyncCounter(String name) {
        return doubleAsynchronousCounters.get(name);
    }

    @Override
    public DoubleUpDownCounter registerDoubleUpDownCounter(String name, String description, String unit) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return register(doubleUpDownCounters, new DoubleUpDownCounterAdapter(meter, name, description, unit));
        }
    }

    @Override
    public DoubleUpDownCounter getDoubleUpDownCounter(String name) {
        return doubleUpDownCounters.get(name);
    }

    @Override
    public DoubleGauge registerDoublesGauge(
        String name,
        String description,
        String unit,
        Supplier<Collection<DoubleWithAttributes>> observer
    ) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return register(doubleGauges, new DoubleGaugeAdapter(meter, name, description, unit, observer));
        }
    }

    @Override
    public DoubleGauge getDoubleGauge(String name) {
        return doubleGauges.get(name);
    }

    @Override
    public DoubleHistogram registerDoubleHistogram(String name, String description, String unit) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return register(doubleHistograms, new DoubleHistogramAdapter(meter, name, description, unit));
        }
    }

    @Override
    public DoubleHistogram getDoubleHistogram(String name) {
        return doubleHistograms.get(name);
    }

    @Override
    public LongCounter registerLongCounter(String name, String description, String unit) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return register(longCounters, new LongCounterAdapter(meter, name, description, unit));
        }
    }

    @Override
    public LongAsyncCounter registerLongsAsyncCounter(
        String name,
        String description,
        String unit,
        Supplier<Collection<LongWithAttributes>> observer
    ) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return register(longAsynchronousCounters, new LongAsyncCounterAdapter(meter, name, description, unit, observer));
        }
    }

    @Override
    public LongAsyncCounter getLongAsyncCounter(String name) {
        return longAsynchronousCounters.get(name);
    }

    @Override
    public LongCounter getLongCounter(String name) {
        return longCounters.get(name);
    }

    @Override
    public LongUpDownCounter registerLongUpDownCounter(String name, String description, String unit) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return register(longUpDownCounters, new LongUpDownCounterAdapter(meter, name, description, unit));
        }
    }

    @Override
    public LongUpDownCounter getLongUpDownCounter(String name) {
        return longUpDownCounters.get(name);
    }

    @Override
    public LongGauge registerLongsGauge(String name, String description, String unit, Supplier<Collection<LongWithAttributes>> observer) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return register(longGauges, new LongGaugeAdapter(meter, name, description, unit, observer));
        }
    }

    @Override
    public LongGauge getLongGauge(String name) {
        return longGauges.get(name);
    }

    @Override
    public LongHistogram registerLongHistogram(String name, String description, String unit) {
        try (ReleasableLock lock = registerLock.acquire()) {
            return register(longHistograms, new LongHistogramAdapter(meter, name, description, unit));
        }
    }

    @Override
    public LongHistogram getLongHistogram(String name) {
        return longHistograms.get(name);
    }

    private <T extends AbstractInstrument<?>> T register(Registrar<T> registrar, T adapter) {
        assert registrars.contains(registrar) : "usage of unknown registrar";
        logger.debug("Registering an instrument with name: " + adapter.getName());
        return registrar.register(adapter);
    }

    public void setProvider(Meter meter) {
        try (ReleasableLock lock = registerLock.acquire()) {
            this.meter = meter;
            for (Registrar<?> registrar : registrars) {
                registrar.setProvider(this.meter);
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
