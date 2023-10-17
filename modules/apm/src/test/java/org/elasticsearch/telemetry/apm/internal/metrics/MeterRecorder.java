/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import org.elasticsearch.core.Strings;
import org.elasticsearch.telemetry.metric.DoubleCounter;
import org.elasticsearch.telemetry.metric.DoubleGauge;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.DoubleUpDownCounter;
import org.elasticsearch.telemetry.metric.Instrument;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongGauge;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.LongUpDownCounter;

import java.util.ArrayList;
import java.util.Collections;
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

    private record Registration(String name, String description, String unit) {
        Registration {
            Objects.requireNonNull(name);
            Objects.requireNonNull(description);
            Objects.requireNonNull(unit);
        }
    };

    private record RegisteredMetric(Map<String, Registration> registered, Map<String, List<TestMetric>> called) {
        void register(String name, String description, String unit) {
            assert registered.containsKey(name) == false
                : Strings.format("unexpected [{}]: [{}][{}], already registered[{}]", name, description, unit, registered.get(name));
            registered.put(name, new Registration(name, description, unit));
        }

        void call(String name, TestMetric call) {
            assert registered.containsKey(name) : Strings.format("call for unregistered metric [{}]: [{}]", name, call);
            called.computeIfAbsent(Objects.requireNonNull(name), k -> new ArrayList<>()).add(call);
        }
    }

    private final Map<INSTRUMENT, RegisteredMetric> doubles;
    private final Map<INSTRUMENT, RegisteredMetric> longs;

    MeterRecorder() {
        doubles = new HashMap<>(INSTRUMENT.values().length);
        longs = new HashMap<>(INSTRUMENT.values().length);
        for (var instrument : INSTRUMENT.values()) {
            doubles.put(instrument, new RegisteredMetric(new HashMap<>(), new HashMap<>()));
            longs.put(instrument, new RegisteredMetric(new HashMap<>(), new HashMap<>()));
        }
    }

    public void clearCalls() {
        doubles.forEach((inst, rm) -> rm.called.clear());
        longs.forEach((inst, rm) -> rm.called.clear());
    }

    void registerDouble(INSTRUMENT instrument, String name, String description, String unit) {
        doubles.get(Objects.requireNonNull(instrument)).register(name, description, unit);
    }

    void registerLong(INSTRUMENT instrument, String name, String description, String unit) {
        longs.get(Objects.requireNonNull(instrument)).register(name, description, unit);
    }

    void call(INSTRUMENT instrument, String name, double value, Map<String, Object> attributes) {
        doubles.get(Objects.requireNonNull(instrument)).call(name, new TestMetric(value, attributes));
    }

    void call(INSTRUMENT instrument, String name, long value, Map<String, Object> attributes) {
        longs.get(Objects.requireNonNull(instrument)).call(name, new TestMetric(value, attributes));
    }

    List<TestMetric> getDouble(INSTRUMENT instrument, String name) {
        return doubles.get(Objects.requireNonNull(instrument)).called.getOrDefault(Objects.requireNonNull(name), Collections.emptyList());
    }

    List<TestMetric> getLong(INSTRUMENT instrument, String name) {
        return longs.get(Objects.requireNonNull(instrument)).called.getOrDefault(Objects.requireNonNull(name), Collections.emptyList());
    }

    List<TestMetric> get(Instrument instrument) {
        Objects.requireNonNull(instrument);
        if (instrument instanceof DoubleCounter) {
            return getDouble(MeterRecorder.INSTRUMENT.COUNTER, instrument.getName());
        } else if (instrument instanceof LongCounter) {
            return getLong(MeterRecorder.INSTRUMENT.COUNTER, instrument.getName());
        } else if (instrument instanceof DoubleUpDownCounter) {
            return getDouble(MeterRecorder.INSTRUMENT.UP_DOWN_COUNTER, instrument.getName());
        } else if (instrument instanceof LongUpDownCounter) {
            return getLong(MeterRecorder.INSTRUMENT.UP_DOWN_COUNTER, instrument.getName());
        } else if (instrument instanceof DoubleHistogram) {
            return getDouble(MeterRecorder.INSTRUMENT.HISTOGRAM, instrument.getName());
        } else if (instrument instanceof LongHistogram) {
            return getLong(MeterRecorder.INSTRUMENT.HISTOGRAM, instrument.getName());
        } else if (instrument instanceof DoubleGauge) {
            return getDouble(MeterRecorder.INSTRUMENT.GAUGE_OBSERVER, instrument.getName());
        } else if (instrument instanceof LongGauge) {
            return getLong(MeterRecorder.INSTRUMENT.GAUGE_OBSERVER, instrument.getName());
        } else {
            throw new IllegalArgumentException("unknown instrument [" + instrument.getClass().getName() + "]");
        }
    }
}
