/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry;

import org.elasticsearch.core.Strings;
import org.elasticsearch.telemetry.metric.Instrument;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Container for registered Instruments (either {@link Instrument} or Otel's versions).
 * Records invocations of the Instruments as {@link Measurement}s.
 * @param <I> The supertype of the registered instrument.
 */
public class MetricRecorder<I> {

    /**
     * Container for Instrument of a given type, such as DoubleGauge, LongHistogram, etc.
     * @param registered - registration records for each named metrics
     * @param called - one instance per invocation of the instance
     * @param instruments - the instrument instance
     */
    private record RegisteredMetric<I>(
        Map<String, Registration> registered,
        Map<String, List<Measurement>> called,
        Map<String, I> instruments,
        List<Runnable> callbacks
    ) {
        void register(String name, String description, String unit, I instrument) {
            assert registered.containsKey(name) == false
                : Strings.format("unexpected [%s]: [%s][%s], already registered[%s]", name, description, unit, registered.get(name));
            registered.put(name, new Registration(name, description, unit));
            instruments.put(name, instrument);
            if (instrument instanceof Runnable callback) {
                callbacks.add(callback);
            }
        }

        void call(String name, Measurement call) {
            assert registered.containsKey(name) : Strings.format("call for unregistered metric [%s]: [%s]", name, call);
            called.computeIfAbsent(Objects.requireNonNull(name), k -> new CopyOnWriteArrayList<>()).add(call);
        }

    }

    /**
     * The containers for each metric type.
     */
    private final Map<InstrumentType, RegisteredMetric<I>> metrics;

    public MetricRecorder() {
        metrics = new ConcurrentHashMap<>(InstrumentType.values().length);
        for (var instrument : InstrumentType.values()) {
            metrics.put(
                instrument,
                new RegisteredMetric<>(
                    new ConcurrentHashMap<>(),
                    new ConcurrentHashMap<>(),
                    new ConcurrentHashMap<>(),
                    new CopyOnWriteArrayList<>()
                )
            );
        }
    }

    /**
     * Register an instrument.  Instruments must be registered before they are used.
     */
    public void register(I instrument, InstrumentType instrumentType, String name, String description, String unit) {
        metrics.get(instrumentType).register(name, description, unit, instrument);
    }

    /**
     * Record a call made to a registered Elasticsearch {@link Instrument}.
     */
    public void call(Instrument instrument, Number value, Map<String, Object> attributes) {
        call(InstrumentType.fromInstrument(instrument), instrument.getName(), value, attributes);
    }

    /**
     * Record a call made to the registered instrument represented by the {@link InstrumentType} enum.
     */
    public void call(InstrumentType instrumentType, String name, Number value, Map<String, Object> attributes) {
        metrics.get(instrumentType).call(name, new Measurement(value, attributes, instrumentType.isDouble));
    }

    /**
     * Get the {@link Measurement}s for each call of the given registered Elasticsearch {@link Instrument}.
     */
    public List<Measurement> getMeasurements(Instrument instrument) {
        return getMeasurements(InstrumentType.fromInstrument(instrument), instrument.getName());
    }

    public List<Measurement> getMeasurements(InstrumentType instrumentType, String name) {
        return metrics.get(instrumentType).called.getOrDefault(Objects.requireNonNull(name), Collections.emptyList());
    }

    public ArrayList<String> getRegisteredMetrics(InstrumentType instrumentType) {
        ArrayList<String> registeredMetrics = new ArrayList<>();
        metrics.get(instrumentType).instruments.forEach((name, registration) -> { registeredMetrics.add(name); });
        return registeredMetrics;
    }

    /**
     * Get the {@link Registration} for a given elasticsearch {@link Instrument}.
     */
    public Registration getRegistration(Instrument instrument) {
        return metrics.get(InstrumentType.fromInstrument(instrument)).registered().get(instrument.getName());
    }

    /**
     * Fetch the instrument instance given the type and registered name.
     */
    public I getInstrument(InstrumentType instrumentType, String name) {
        return metrics.get(instrumentType).instruments.get(name);
    }

    public void resetCalls() {
        metrics.forEach((it, rm) -> rm.called().clear());
    }

    public void collect() {
        metrics.forEach((it, rm) -> rm.callbacks().forEach(Runnable::run));
    }
}
