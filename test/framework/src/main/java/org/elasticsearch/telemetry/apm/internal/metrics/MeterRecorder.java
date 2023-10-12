/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import org.elasticsearch.core.Strings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MeterRecorder {
    enum INSTRUMENT {
        COUNTER,
        UP_DOWN_COUNTER,
        HISTOGRAM,
        GAUGE,
        GAUGE_OBSERVER
    }

    sealed interface MetricCall permits LongMetricCall, DoubleMetricCall {}

    record LongMetricCall(long value, Map<String, Object> attributes) implements MetricCall {}

    record DoubleMetricCall(double value, Map<String, Object> attributes) implements MetricCall {}

    private record Registration(String name, String description, String unit) {
        Registration {
            Objects.requireNonNull(name);
            Objects.requireNonNull(description);
            Objects.requireNonNull(unit);
        }
    };

    private record RegisteredMetric<M extends MetricCall>(Map<String, Registration> registered, Map<String, List<M>> called) {
        void register(String name, String description, String unit) {
            assert registered.containsKey(name) == false
                : Strings.format("unexpected [{}]: [{}][{}], already registered[{}]", name, description, unit, registered.get(name));
            registered.put(name, new Registration(name, description, unit));
        }

        void call(String name, M call) {
            assert registered.containsKey(name) : Strings.format("call for unregistered metric [{}]: [{}]", name, call);
            called.computeIfAbsent(Objects.requireNonNull(name), k -> new ArrayList<>()).add(call);
        }
    }

    private final Map<INSTRUMENT, RegisteredMetric<DoubleMetricCall>> doubles;
    private final Map<INSTRUMENT, RegisteredMetric<LongMetricCall>> longs;

    MeterRecorder() {
        doubles = new HashMap<>(INSTRUMENT.values().length);
        longs = new HashMap<>(INSTRUMENT.values().length);
        for (var instrument : INSTRUMENT.values()) {
            doubles.put(instrument, new RegisteredMetric<>(new HashMap<>(), new HashMap<>()));
            longs.put(instrument, new RegisteredMetric<>(new HashMap<>(), new HashMap<>()));
        }
    }

    void registerDouble(INSTRUMENT instrument, String name, String description, String unit) {
        doubles.get(Objects.requireNonNull(instrument)).register(name, description, unit);
    }

    void registerLong(INSTRUMENT instrument, String name, String description, String unit) {
        longs.get(Objects.requireNonNull(instrument)).register(name, description, unit);
    }

    void call(INSTRUMENT instrument, String name, double value, Map<String, Object> attributes) {
        doubles.get(Objects.requireNonNull(instrument)).call(name, new DoubleMetricCall(value, attributes));
    }

    void call(INSTRUMENT instrument, String name, long value, Map<String, Object> attributes) {
        longs.get(Objects.requireNonNull(instrument)).call(name, new LongMetricCall(value, attributes));
    }

    List<DoubleMetricCall> getDouble(INSTRUMENT instrument, String name) {
        return doubles.get(Objects.requireNonNull(instrument)).called.get(Objects.requireNonNull(name));
    }

    List<LongMetricCall> getLong(INSTRUMENT instrument, String name) {
        return longs.get(Objects.requireNonNull(instrument)).called.get(Objects.requireNonNull(name));
    }
}
