/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry;

import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.telemetry.metric.DoubleAsyncCounter;
import org.elasticsearch.telemetry.metric.DoubleAsyncGauge;
import org.elasticsearch.telemetry.metric.DoubleAsyncMeasurement;
import org.elasticsearch.telemetry.metric.DoubleCounter;
import org.elasticsearch.telemetry.metric.DoubleGauge;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.DoubleUpDownCounter;
import org.elasticsearch.telemetry.metric.Instrument;
import org.elasticsearch.telemetry.metric.LongAsyncCounter;
import org.elasticsearch.telemetry.metric.LongAsyncGauge;
import org.elasticsearch.telemetry.metric.LongAsyncMeasurement;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongGauge;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.LongUpDownCounter;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * Recording versions of Elasticsearch {@link Instrument}s.  All invocations are recorded via {@link MetricRecorder}.
 */
public class RecordingInstruments {
    protected abstract static class RecordingInstrument implements Instrument {
        protected final String name;
        final MetricRecorder<Instrument> recorder;

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

    protected abstract static class CallbackRecordingInstrument<M> extends RecordingInstrument implements AutoCloseable, Runnable {
        private final Consumer<M> callback;
        private final M measurementRecorder;
        private boolean closed = false;
        private final ReleasableLock closedLock = new ReleasableLock(new ReentrantLock());

        public CallbackRecordingInstrument(String name, MetricRecorder<Instrument> recorder, Consumer<M> callback, M measurementRecorder) {
            super(name, recorder);
            this.callback = callback;
            this.measurementRecorder = measurementRecorder;
        }

        @Override
        public void run() {
            try (ReleasableLock lock = closedLock.acquire()) {
                if (closed) {
                    return;
                }
                callback.accept(measurementRecorder);
            }
        }

        @Override
        public void close() {
            recorder.deregister(this);
            try (ReleasableLock lock = closedLock.acquire()) {
                assert closed == false : "double close";
                closed = true;
            }
        }

        static class LongMeasurement implements LongAsyncMeasurement {

            private final MetricRecorder<?> recorder;
            private final InstrumentType instrumentType;
            private final String name;

            LongMeasurement(MetricRecorder<?> recorder, InstrumentType instrumentType, String name) {
                this.recorder = recorder;
                this.instrumentType = instrumentType;
                this.name = name;
            }

            @Override
            public void record(long value, Map<String, Object> attributes) {
                recorder.call(instrumentType, name, value, attributes);
            }
        }

        static class DoubleMeasurement implements DoubleAsyncMeasurement {

            private final MetricRecorder<?> recorder;
            private final InstrumentType instrumentType;
            private final String name;

            DoubleMeasurement(MetricRecorder<?> recorder, InstrumentType instrumentType, String name) {
                this.recorder = recorder;
                this.instrumentType = instrumentType;
                this.name = name;
            }

            @Override
            public void record(double value, Map<String, Object> attributes) {
                recorder.call(instrumentType, name, value, attributes);
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

    public static class RecordingDoubleGauge extends RecordingInstrument implements DoubleGauge {
        public RecordingDoubleGauge(String name, MetricRecorder<Instrument> recorder) {
            super(name, recorder);
        }

        @Override
        public void set(double value) {
            set(value, Collections.emptyMap());
        }

        @Override
        public void set(double value, Map<String, Object> attributes) {
            call(value, attributes);
        }
    }

    public static class RecordingDoubleAsyncGauge extends CallbackRecordingInstrument<DoubleAsyncMeasurement> implements DoubleAsyncGauge {
        public RecordingDoubleAsyncGauge(String name, MetricRecorder<Instrument> recorder, Consumer<DoubleAsyncMeasurement> callback) {
            super(name, recorder, callback, new DoubleMeasurement(recorder, InstrumentType.DOUBLE_ASYNC_GAUGE, name));
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

    public static class RecordingAsyncLongCounter extends CallbackRecordingInstrument<LongAsyncMeasurement> implements LongAsyncCounter {

        public RecordingAsyncLongCounter(String name, MetricRecorder<Instrument> recorder, Consumer<LongAsyncMeasurement> callback) {
            super(name, recorder, callback, new LongMeasurement(recorder, InstrumentType.LONG_ASYNC_COUNTER, name));
        }
    }

    public static class RecordingAsyncDoubleCounter extends CallbackRecordingInstrument<DoubleAsyncMeasurement>
        implements
            DoubleAsyncCounter {

        public RecordingAsyncDoubleCounter(String name, MetricRecorder<Instrument> recorder, Consumer<DoubleAsyncMeasurement> callback) {
            super(name, recorder, callback, new DoubleMeasurement(recorder, InstrumentType.DOUBLE_ASYNC_COUNTER, name));
        }

    }

    public static class RecordingLongGauge extends RecordingInstrument implements LongGauge {
        public RecordingLongGauge(String name, MetricRecorder<Instrument> recorder) {
            super(name, recorder);
        }

        @Override
        public void set(long value) {
            set(value, Collections.emptyMap());
        }

        @Override
        public void set(long value, Map<String, Object> attributes) {
            call(value, attributes);
        }
    }

    public static class RecordingLongAsyncGauge extends CallbackRecordingInstrument<LongAsyncMeasurement> implements LongAsyncGauge {

        public RecordingLongAsyncGauge(String name, MetricRecorder<Instrument> recorder, Consumer<LongAsyncMeasurement> callback) {
            super(name, recorder, callback, new LongMeasurement(recorder, InstrumentType.LONG_ASYNC_GAUGE, name));
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
