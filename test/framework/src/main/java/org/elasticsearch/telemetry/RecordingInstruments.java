/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry;

import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.telemetry.metric.DoubleCounter;
import org.elasticsearch.telemetry.metric.DoubleGauge;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.DoubleUpDownCounter;
import org.elasticsearch.telemetry.metric.DoubleWithAttributes;
import org.elasticsearch.telemetry.metric.Instrument;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongGauge;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.LongUpDownCounter;
import org.elasticsearch.telemetry.metric.LongWithAttributes;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Recording versions of Elasticsearch {@link Instrument}s.  All invocations are recorded via {@link MetricRecorder}.
 */
public class RecordingInstruments {
    protected abstract static class RecordingInstrument implements Instrument {
        protected final String name;
        private final MetricRecorder<Instrument> recorder;

        public RecordingInstrument(String name, MetricRecorder<Instrument> recorder) {
            this.name = Objects.requireNonNull(name);
            this.recorder = Objects.requireNonNull(recorder);
        }

        protected void call(Number value, Map<String, Object> attributes) {
            recorder.call(this, value, attributes);
        }

        @Override
        public String getName() {
            return name;
        }
    }

    protected interface NumberWithAttributesObserver extends Supplier<Tuple<Number, Map<String, Object>>> {

    }

    protected abstract static class CallbackRecordingInstrument extends RecordingInstrument implements AutoCloseable, Runnable {
        private final NumberWithAttributesObserver observer;
        private boolean closed = false;
        private final ReleasableLock closedLock = new ReleasableLock(new ReentrantLock());

        public CallbackRecordingInstrument(String name, NumberWithAttributesObserver observer, MetricRecorder<Instrument> recorder) {
            super(name, recorder);
            this.observer = observer;
        }

        @Override
        public void run() {
            try (ReleasableLock lock = closedLock.acquire()) {
                if (closed) {
                    return;
                }
                var observation = observer.get();
                call(observation.v1(), observation.v2());
            }
        }

        @Override
        public void close() throws Exception {
            try (ReleasableLock lock = closedLock.acquire()) {
                assert closed == false : "double close";
                closed = true;
            }
        }
    }

    public static class RecordingDoubleCounter extends RecordingInstrument implements DoubleCounter {
        public RecordingDoubleCounter(String name, MetricRecorder<Instrument> recorder) {
            super(name, recorder);
        }

        @Override
        public void increment() {
            incrementBy(1.0, Collections.emptyMap());
        }

        @Override
        public void incrementBy(double inc) {
            incrementBy(inc, Collections.emptyMap());
        }

        @Override
        public void incrementBy(double inc, Map<String, Object> attributes) {
            call(inc, attributes);
        }
    }

    public static class RecordingDoubleGauge extends CallbackRecordingInstrument implements DoubleGauge {
        public RecordingDoubleGauge(String name, Supplier<DoubleWithAttributes> observer, MetricRecorder<Instrument> recorder) {
            super(name, () -> {
                var observation = observer.get();
                return new Tuple<>(observation.value(), observation.attributes());
            }, recorder);
        }
    }

    public static class RecordingDoubleHistogram extends RecordingInstrument implements DoubleHistogram {
        public RecordingDoubleHistogram(String name, MetricRecorder<Instrument> recorder) {
            super(name, recorder);
        }

        @Override
        public void record(double value) {
            record(value, Collections.emptyMap());
        }

        @Override
        public void record(double value, Map<String, Object> attributes) {
            call(value, attributes);
        }
    }

    public static class RecordingDoubleUpDownCounter extends RecordingInstrument implements DoubleUpDownCounter {
        public RecordingDoubleUpDownCounter(String name, MetricRecorder<Instrument> recorder) {
            super(name, recorder);
        }

        @Override
        public void add(double inc) {
            add(inc, Collections.emptyMap());
        }

        @Override
        public void add(double inc, Map<String, Object> attributes) {
            call(inc, attributes);
        }
    }

    public static class RecordingLongCounter extends RecordingInstrument implements LongCounter {
        public RecordingLongCounter(String name, MetricRecorder<Instrument> recorder) {
            super(name, recorder);
        }

        @Override
        public void increment() {
            incrementBy(1L, Collections.emptyMap());
        }

        @Override
        public void incrementBy(long inc) {
            incrementBy(inc, Collections.emptyMap());
        }

        @Override
        public void incrementBy(long inc, Map<String, Object> attributes) {
            call(inc, attributes);
        }
    }

    public static class RecordingLongGauge extends CallbackRecordingInstrument implements LongGauge {

        public RecordingLongGauge(String name, Supplier<LongWithAttributes> observer, MetricRecorder<Instrument> recorder) {
            super(name, () -> {
                var observation = observer.get();
                return new Tuple<>(observation.value(), observation.attributes());
            }, recorder);
        }
    }

    public static class RecordingLongHistogram extends RecordingInstrument implements LongHistogram {
        public RecordingLongHistogram(String name, MetricRecorder<Instrument> recorder) {
            super(name, recorder);
        }

        @Override
        public void record(long value) {
            record(value, Collections.emptyMap());
        }

        @Override
        public void record(long value, Map<String, Object> attributes) {
            call(value, attributes);
        }
    }

    public static class RecordingLongUpDownCounter extends RecordingInstrument implements LongUpDownCounter {
        public RecordingLongUpDownCounter(String name, MetricRecorder<Instrument> recorder) {
            super(name, recorder);
        }

        @Override
        public void add(long inc) {
            add(inc, Collections.emptyMap());
        }

        @Override
        public void add(long inc, Map<String, Object> attributes) {
            call(inc, attributes);
        }
    }
}
