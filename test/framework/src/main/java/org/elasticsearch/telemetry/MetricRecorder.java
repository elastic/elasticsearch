/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry;

import org.elasticsearch.core.Strings;
import org.elasticsearch.telemetry.metric.Instrument;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MetricRecorder<I> {

    private record RegisteredMetric<I>(
        Map<String, Registration> registered,
        Map<String, List<Measurement>> called,
        Map<String, I> instruments
    ) {
        void register(String name, String description, String unit, I instrument) {
            assert registered.containsKey(name) == false
                : Strings.format("unexpected [{}]: [{}][{}], already registered[{}]", name, description, unit, registered.get(name));
            registered.put(name, new Registration(name, description, unit));
            instruments.put(name, instrument);
        }

        void call(String name, Measurement call) {
            assert registered.containsKey(name) : Strings.format("call for unregistered metric [{}]: [{}]", name, call);
            called.computeIfAbsent(Objects.requireNonNull(name), k -> new ArrayList<>()).add(call);
        }
    }

    private final Map<InstrumentType, RegisteredMetric<I>> metrics;

    MetricRecorder() {
        metrics = new HashMap<>(InstrumentType.values().length);
        for (var instrument : InstrumentType.values()) {
            metrics.put(instrument, new RegisteredMetric<>(new HashMap<>(), new HashMap<>(), new HashMap<>()));
        }
    }

    void register(I instrument, InstrumentType instrumentType, String name, String description, String unit) {
        metrics.get(instrumentType).register(name, description, unit, instrument);
    }

    void call(Instrument instrument, Number value, Map<String, Object> attributes) {
        call(InstrumentType.fromInstrument(instrument), instrument.getName(), value, attributes);
    }

    void call(InstrumentType instrumentType, String name, Number value, Map<String, Object> attributes) {
        metrics.get(instrumentType).call(name, new Measurement(value, attributes, instrumentType.isDouble));
    }

    public List<Measurement> getMetrics(Instrument instrument) {
        return getMetrics(InstrumentType.fromInstrument(instrument), instrument.getName());
    }

    Registration getRegistration(Instrument instrument) {
        return metrics.get(InstrumentType.fromInstrument(instrument)).registered().get(instrument.getName());
    }

    List<Measurement> getMetrics(InstrumentType instrumentType, String name) {
        return metrics.get(instrumentType).called.getOrDefault(Objects.requireNonNull(name), Collections.emptyList());
    }

    I getInstrument(InstrumentType instrumentType, String name) {
        return metrics.get(instrumentType).instruments.get(name);
    }
}
