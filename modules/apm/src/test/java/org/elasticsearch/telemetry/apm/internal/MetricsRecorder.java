/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal;

import org.elasticsearch.core.Strings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetricsRecorder {
    public enum INSTRUMENT {
        COUNTER, UP_DOWN_COUNTER, HISTOGRAM, GAUGE, GAUGE_OBSERVER
    }

    public enum NUMERIC {
        LONG, DOUBLE
    }

    public sealed interface MetricCall permits LongMetricCall, DoubleMetricCall {
        NUMERIC getNumeric();
    }

    public record LongMetricCall(long value, Map<String, Object> attributes) implements MetricCall {
        @Override
        public NUMERIC getNumeric() {
            return NUMERIC.LONG;
        }
    }

    public record DoubleMetricCall(double value, Map<String, Object> attributes) implements MetricCall {
        @Override
        public NUMERIC getNumeric() {
            return NUMERIC.DOUBLE;
        }
    }

    private record Registration(String name, String description, String unit) {};
    private record RegisteredMetric<M extends MetricCall>(Map<String, Registration> registered, Map<String, List<M>> called) {
        void register(String name, String description, String unit) {
            assert registered.containsKey(name) == false : Strings.format("unexpected [{}]: [{}][{}], already registered[{}]", name, description, unit, registered.get(name));
            registered.put(name, new Registration(name, description, unit));
        }

        void call(String name, M call) {
            assert registered.containsKey(name) : Strings.format("call for unregistered metric [{}]: [{}]", name, call);
            called.computeIfAbsent(name, k -> new ArrayList<>()).add(call);
        }
    }

    private final Map<INSTRUMENT, RegisteredMetric<DoubleMetricCall>> doubles;
    private final Map<INSTRUMENT, RegisteredMetric<LongMetricCall>> longs;

    MetricsRecorder() {
        doubles = new HashMap<>(INSTRUMENT.values().length);
        longs = new HashMap<>(INSTRUMENT.values().length);
        for (var instrument : INSTRUMENT.values()) {
            doubles.put(instrument, new RegisteredMetric<>(new HashMap<>(), new HashMap<>()));
            longs.put(instrument, new RegisteredMetric<>(new HashMap<>(), new HashMap<>()));
        }
    }

    void registerDouble(NUMERIC numeric, INSTRUMENT instrument, String name, String description, String unit) {
        doubles.get(instrument).register(name, description, unit);
    }
    void registerLong(NUMERIC numeric, INSTRUMENT instrument, String name, String description, String unit) {
        longs.get(instrument).register(name, description, unit);
    }

    void call(INSTRUMENT instrument, String name, double value, Map<String, Object> attributes) {
        doubles.get(instrument).call(name, new DoubleMetricCall(value, attributes));
    }

    void call(INSTRUMENT instrument, String name, long value, Map<String, Object> attributes) {
        longs.get(instrument).call(name, new LongMetricCall(value, attributes));
    }

    List<DoubleMetricCall> getDouble(INSTRUMENT instrument, String name) {
        return doubles.get(instrument).called.get(name);
    }

    List<LongMetricCall> getLong(INSTRUMENT instrument, String name) {
        return longs.get(instrument).called.get(name);
    }
}
