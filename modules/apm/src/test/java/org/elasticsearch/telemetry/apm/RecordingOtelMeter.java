/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleCounter;
import io.opentelemetry.api.metrics.DoubleCounterBuilder;
import io.opentelemetry.api.metrics.DoubleGaugeBuilder;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.DoubleHistogramBuilder;
import io.opentelemetry.api.metrics.DoubleUpDownCounter;
import io.opentelemetry.api.metrics.DoubleUpDownCounterBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongCounterBuilder;
import io.opentelemetry.api.metrics.LongGaugeBuilder;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.LongHistogramBuilder;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.LongUpDownCounterBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleCounter;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;
import io.opentelemetry.api.metrics.ObservableDoubleUpDownCounter;
import io.opentelemetry.api.metrics.ObservableLongCounter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import io.opentelemetry.api.metrics.ObservableLongUpDownCounter;
import io.opentelemetry.context.Context;

import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.MetricRecorder;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

public class RecordingOtelMeter implements Meter {

    Queue<Callback> callbacks = new ConcurrentLinkedQueue<>();

    public void collectMetrics() {
        callbacks.forEach(Callback::doCall);
    }

    public MetricRecorder<OtelInstrument> getRecorder() {
        return recorder;
    }

    private final MetricRecorder<OtelInstrument> recorder = new MetricRecorder<>();

    @Override
    public LongCounterBuilder counterBuilder(String name) {
        return new RecordingLongCounterBuilder(name);
    }

    @Override
    public LongUpDownCounterBuilder upDownCounterBuilder(String name) {
        return new RecordingLongUpDownBuilder(name);
    }

    @Override
    public DoubleHistogramBuilder histogramBuilder(String name) {
        return new RecordingDoubleHistogramBuilder(name);
    }

    @Override
    public DoubleGaugeBuilder gaugeBuilder(String name) {
        return new RecordingDoubleGaugeBuilder(name);
    }

    // Counter
    private class RecordingLongCounterBuilder extends AbstractBuilder implements LongCounterBuilder {
        RecordingLongCounterBuilder(String name) {
            super(name);
        }

        @Override
        public LongCounterBuilder setDescription(String description) {
            innerSetDescription(description);
            return this;
        }

        @Override
        public LongCounterBuilder setUnit(String unit) {
            innerSetUnit(unit);
            return this;
        }

        @Override
        public DoubleCounterBuilder ofDoubles() {
            return new RecordingDoubleCounterBuilder(this);
        }

        @Override
        public LongCounter build() {
            LongRecorder counter = new LongRecorder(name);
            recorder.register(counter, counter.getInstrument(), name, description, unit);
            return counter;
        }

        @Override
        public ObservableLongCounter buildWithCallback(Consumer<ObservableLongMeasurement> callback) {
            LongAsyncCounterRecorder longAsyncCounter = new LongAsyncCounterRecorder(name, callback);
            recorder.register(longAsyncCounter, longAsyncCounter.getInstrument(), name, description, unit);
            callbacks.add(longAsyncCounter);
            return longAsyncCounter;
        }

        @Override
        public ObservableLongMeasurement buildObserver() {
            unimplemented();
            return null;
        }
    }

    private class LongAsyncCounterRecorder extends AbstractInstrument implements ObservableLongCounter, Callback, OtelInstrument {
        final Consumer<ObservableLongMeasurement> callback;

        LongAsyncCounterRecorder(String name, Consumer<ObservableLongMeasurement> callback) {
            super(name, InstrumentType.LONG_ASYNC_COUNTER);
            this.callback = callback;
        }

        @Override
        public void close() {
            callbacks.remove(this);
        }

        public void doCall() {
            callback.accept(new LongMeasurementRecorder(name, instrument));
        }
    }

    private class LongRecorder extends LongUpDownRecorder implements LongCounter, OtelInstrument {
        LongRecorder(String name) {
            super(name, InstrumentType.LONG_COUNTER);
        }

        @Override
        public void add(long value) {
            assert value >= 0;
            super.add(value);
        }

        @Override
        public void add(long value, Attributes attributes) {
            assert value >= 0;
            super.add(value, attributes);
        }

        @Override
        public void add(long value, Attributes attributes, Context context) {
            assert value >= 0;
            super.add(value, attributes, context);
        }
    }

    private class RecordingDoubleCounterBuilder extends AbstractBuilder implements DoubleCounterBuilder {

        RecordingDoubleCounterBuilder(AbstractBuilder other) {
            super(other);
        }

        @Override
        public DoubleCounterBuilder setDescription(String description) {
            innerSetDescription(description);
            return this;
        }

        @Override
        public DoubleCounterBuilder setUnit(String unit) {
            innerSetUnit(unit);
            return this;
        }

        @Override
        public DoubleCounter build() {
            DoubleRecorder counter = new DoubleRecorder(name);
            recorder.register(counter, counter.getInstrument(), name, description, unit);
            return counter;
        }

        @Override
        public ObservableDoubleCounter buildWithCallback(Consumer<ObservableDoubleMeasurement> callback) {
            DoubleAsyncCounterRecorder doubleAsyncCounter = new DoubleAsyncCounterRecorder(name, callback);
            recorder.register(doubleAsyncCounter, doubleAsyncCounter.getInstrument(), name, description, unit);
            callbacks.add(doubleAsyncCounter);
            return doubleAsyncCounter;
        }

        @Override
        public ObservableDoubleMeasurement buildObserver() {
            unimplemented();
            return null;
        }
    }

    private class DoubleAsyncCounterRecorder extends AbstractInstrument implements ObservableDoubleCounter, Callback, OtelInstrument {
        final Consumer<ObservableDoubleMeasurement> callback;

        DoubleAsyncCounterRecorder(String name, Consumer<ObservableDoubleMeasurement> callback) {
            super(name, InstrumentType.DOUBLE_ASYNC_COUNTER);
            this.callback = callback;
        }

        @Override
        public void close() {
            callbacks.remove(this);
        }

        public void doCall() {
            callback.accept(new DoubleMeasurementRecorder(name, instrument));
        }
    }

    private class DoubleRecorder extends DoubleUpDownRecorder implements DoubleCounter, OtelInstrument {
        DoubleRecorder(String name) {
            super(name, InstrumentType.DOUBLE_COUNTER);
        }

        @Override
        public void add(double value) {
            assert value >= 0;
            super.add(value);
        }

        @Override
        public void add(double value, Attributes attributes) {
            assert value >= 0;
            super.add(value, attributes);
        }

        @Override
        public void add(double value, Attributes attributes, Context context) {
            assert value >= 0;
            super.add(value, attributes, context);
        }
    }

    private class RecordingLongUpDownBuilder extends AbstractBuilder implements LongUpDownCounterBuilder {
        RecordingLongUpDownBuilder(String name) {
            super(name);
        }

        @Override
        public LongUpDownCounterBuilder setDescription(String description) {
            innerSetDescription(description);
            return this;
        }

        @Override
        public LongUpDownCounterBuilder setUnit(String unit) {
            innerSetUnit(unit);
            return this;
        }

        @Override
        public DoubleUpDownCounterBuilder ofDoubles() {
            return new RecordingDoubleUpDownBuilder(this);
        }

        @Override
        public LongUpDownCounter build() {
            LongUpDownRecorder counter = new LongUpDownRecorder(name);
            recorder.register(counter, counter.getInstrument(), name, description, unit);
            return counter;
        }

        @Override
        public ObservableLongUpDownCounter buildWithCallback(Consumer<ObservableLongMeasurement> callback) {
            unimplemented();
            return null;
        }

        @Override
        public ObservableLongMeasurement buildObserver() {
            unimplemented();
            return null;
        }
    }

    private class LongUpDownRecorder extends AbstractInstrument implements LongUpDownCounter, OtelInstrument {
        LongUpDownRecorder(String name) {
            super(name, InstrumentType.LONG_UP_DOWN_COUNTER);
        }

        protected LongUpDownRecorder(String name, InstrumentType instrument) {
            // used by LongRecorder
            super(name, instrument);
        }

        @Override
        public void add(long value) {
            recorder.call(instrument, name, value, null);
        }

        @Override
        public void add(long value, Attributes attributes) {
            recorder.call(instrument, name, value, toMap(attributes));
        }

        @Override
        public void add(long value, Attributes attributes, Context context) {
            unimplemented();
        }
    }

    private class RecordingDoubleUpDownBuilder extends AbstractBuilder implements DoubleUpDownCounterBuilder {

        RecordingDoubleUpDownBuilder(AbstractBuilder other) {
            super(other);
        }

        @Override
        public DoubleUpDownCounterBuilder setDescription(String description) {
            innerSetDescription(description);
            return this;
        }

        @Override
        public DoubleUpDownCounterBuilder setUnit(String unit) {
            innerSetUnit(unit);
            return this;
        }

        @Override
        public DoubleUpDownCounter build() {
            DoubleUpDownRecorder counter = new DoubleUpDownRecorder(name);
            recorder.register(counter, counter.getInstrument(), name, description, unit);
            return counter;
        }

        @Override
        public ObservableDoubleUpDownCounter buildWithCallback(Consumer<ObservableDoubleMeasurement> callback) {
            unimplemented();
            return null;
        }

        @Override
        public ObservableDoubleMeasurement buildObserver() {
            unimplemented();
            return null;
        }
    }

    private class DoubleUpDownRecorder extends AbstractInstrument implements DoubleUpDownCounter, OtelInstrument {
        DoubleUpDownRecorder(String name) {
            super(name, InstrumentType.DOUBLE_UP_DOWN_COUNTER);
        }

        protected DoubleUpDownRecorder(String name, InstrumentType instrument) {
            // used by DoubleRecorder
            super(name, instrument);
        }

        @Override
        public void add(double value) {
            recorder.call(instrument, name, value, null);
        }

        @Override
        public void add(double value, Attributes attributes) {
            recorder.call(instrument, name, value, toMap(attributes));
        }

        @Override
        public void add(double value, Attributes attributes, Context context) {
            unimplemented();
        }
    }

    interface Callback {
        void doCall();
    }

    abstract static class AbstractInstrument {
        protected final String name;
        protected final InstrumentType instrument;

        AbstractInstrument(String name, InstrumentType instrument) {
            this.name = name;
            this.instrument = instrument;
        }

        public InstrumentType getInstrument() {
            return instrument;
        }

        protected void unimplemented() {
            throw new UnsupportedOperationException("unimplemented");
        }

        Map<String, Object> toMap(Attributes attributes) {
            if (attributes == null) {
                return null;
            }
            if (attributes.isEmpty()) {
                return Collections.emptyMap();
            }
            Map<String, Object> map = new HashMap<>(attributes.size());
            attributes.forEach((k, v) -> map.put(k.getKey(), v));
            return map;
        }
    }

    abstract static class AbstractBuilder {
        protected final String name;
        protected String description;
        protected String unit;

        AbstractBuilder(String name) {
            this.name = name;
        }

        AbstractBuilder(AbstractBuilder other) {
            this.name = other.name;
            this.description = other.description;
            this.unit = other.unit;
        }

        void innerSetDescription(String description) {
            this.description = description;
        }

        void innerSetUnit(String unit) {
            this.unit = unit;
        }

        protected void unimplemented() {
            throw new UnsupportedOperationException("unimplemented");
        }
    }

    interface OtelInstrument {}

    // Gauges
    private class RecordingDoubleGaugeBuilder extends AbstractBuilder implements DoubleGaugeBuilder {
        RecordingDoubleGaugeBuilder(String name) {
            super(name);
        }

        @Override
        public DoubleGaugeBuilder setDescription(String description) {
            innerSetDescription(description);
            return this;
        }

        @Override
        public DoubleGaugeBuilder setUnit(String unit) {
            innerSetUnit(unit);
            return this;
        }

        @Override
        public LongGaugeBuilder ofLongs() {
            return new RecordingLongGaugeBuilder(this);
        }

        @Override
        public ObservableDoubleGauge buildWithCallback(Consumer<ObservableDoubleMeasurement> callback) {
            DoubleGaugeRecorder gauge = new DoubleGaugeRecorder(name, callback);
            recorder.register(gauge, gauge.getInstrument(), name, description, unit);
            callbacks.add(gauge);
            return gauge;
        }

        @Override
        public ObservableDoubleMeasurement buildObserver() {
            DoubleMeasurementRecorder measurement = new DoubleMeasurementRecorder(name);
            recorder.register(measurement, measurement.getInstrument(), name, description, unit);
            return measurement;
        }
    }

    private class DoubleGaugeRecorder extends AbstractInstrument implements ObservableDoubleGauge, Callback, OtelInstrument {
        final Consumer<ObservableDoubleMeasurement> callback;

        DoubleGaugeRecorder(String name, Consumer<ObservableDoubleMeasurement> callback) {
            super(name, InstrumentType.DOUBLE_GAUGE);
            this.callback = callback;
        }

        @Override
        public void close() {
            callbacks.remove(this);
        }

        public void doCall() {
            callback.accept(new DoubleMeasurementRecorder(name, instrument));
        }
    }

    private class DoubleMeasurementRecorder extends AbstractInstrument implements ObservableDoubleMeasurement, OtelInstrument {
        DoubleMeasurementRecorder(String name, InstrumentType instrument) {
            super(name, instrument);
        }

        DoubleMeasurementRecorder(String name) {
            super(name, InstrumentType.DOUBLE_GAUGE);
        }

        @Override
        public void record(double value) {
            recorder.call(instrument, name, value, null);
        }

        @Override
        public void record(double value, Attributes attributes) {
            recorder.call(instrument, name, value, toMap(attributes));
        }
    }

    private class RecordingLongGaugeBuilder extends AbstractBuilder implements LongGaugeBuilder {
        RecordingLongGaugeBuilder(AbstractBuilder other) {
            super(other);
        }

        @Override
        public LongGaugeBuilder setDescription(String description) {
            innerSetDescription(description);
            return this;
        }

        @Override
        public LongGaugeBuilder setUnit(String unit) {
            innerSetUnit(unit);
            return this;
        }

        @Override
        public ObservableLongGauge buildWithCallback(Consumer<ObservableLongMeasurement> callback) {
            LongGaugeRecorder gauge = new LongGaugeRecorder(name, callback);
            recorder.register(gauge, gauge.getInstrument(), name, description, unit);
            callbacks.add(gauge);
            return gauge;
        }

        @Override
        public ObservableLongMeasurement buildObserver() {
            LongMeasurementRecorder measurement = new LongMeasurementRecorder(name);
            recorder.register(measurement, measurement.getInstrument(), name, description, unit);
            return measurement;
        }
    }

    private class LongGaugeRecorder extends AbstractInstrument implements ObservableLongGauge, Callback, OtelInstrument {
        final Consumer<ObservableLongMeasurement> callback;

        LongGaugeRecorder(String name, Consumer<ObservableLongMeasurement> callback) {
            super(name, InstrumentType.LONG_GAUGE);
            this.callback = callback;
        }

        @Override
        public void close() {
            callbacks.remove(this);
        }

        public void doCall() {
            callback.accept(new LongMeasurementRecorder(name, instrument));
        }
    }

    private class LongMeasurementRecorder extends AbstractInstrument implements ObservableLongMeasurement, OtelInstrument {
        LongMeasurementRecorder(String name, InstrumentType instrument) {
            super(name, instrument);
        }

        LongMeasurementRecorder(String name) {
            super(name, InstrumentType.LONG_GAUGE);
        }

        @Override
        public void record(long value) {
            recorder.call(instrument, name, value, null);
        }

        @Override
        public void record(long value, Attributes attributes) {
            recorder.call(instrument, name, value, toMap(attributes));
        }
    }

    // Histograms
    private class RecordingDoubleHistogramBuilder extends AbstractBuilder implements DoubleHistogramBuilder {
        RecordingDoubleHistogramBuilder(String name) {
            super(name);
        }

        @Override
        public DoubleHistogramBuilder setDescription(String description) {
            innerSetDescription(description);
            return this;
        }

        @Override
        public DoubleHistogramBuilder setUnit(String unit) {
            innerSetUnit(unit);
            return this;
        }

        @Override
        public LongHistogramBuilder ofLongs() {
            return new RecordingLongHistogramBuilder(this);
        }

        @Override
        public DoubleHistogram build() {
            DoubleHistogramRecorder histogram = new DoubleHistogramRecorder(name);
            recorder.register(histogram, histogram.getInstrument(), name, description, unit);
            return histogram;
        }
    }

    private class DoubleHistogramRecorder extends AbstractInstrument implements DoubleHistogram, OtelInstrument {
        DoubleHistogramRecorder(String name) {
            super(name, InstrumentType.DOUBLE_HISTOGRAM);
        }

        @Override
        public void record(double value) {
            recorder.call(getInstrument(), name, value, null);
        }

        @Override
        public void record(double value, Attributes attributes) {
            recorder.call(getInstrument(), name, value, toMap(attributes));
        }

        @Override
        public void record(double value, Attributes attributes, Context context) {
            unimplemented();
        }
    }

    private class RecordingLongHistogramBuilder extends AbstractBuilder implements LongHistogramBuilder {

        RecordingLongHistogramBuilder(AbstractBuilder other) {
            super(other);
        }

        @Override
        public LongHistogramBuilder setDescription(String description) {
            innerSetDescription(description);
            return this;
        }

        @Override
        public LongHistogramBuilder setUnit(String unit) {
            innerSetUnit(unit);
            return this;
        }

        @Override
        public LongHistogram build() {
            LongHistogramRecorder histogram = new LongHistogramRecorder(name);
            recorder.register(histogram, histogram.getInstrument(), name, description, unit);
            return histogram;
        }
    }

    private class LongHistogramRecorder extends AbstractInstrument implements LongHistogram, OtelInstrument {
        LongHistogramRecorder(String name) {
            super(name, InstrumentType.LONG_HISTOGRAM);
        }

        @Override
        public void record(long value) {
            recorder.call(getInstrument(), name, value, null);
        }

        @Override
        public void record(long value, Attributes attributes) {
            recorder.call(getInstrument(), name, value, toMap(attributes));
        }

        @Override
        public void record(long value, Attributes attributes, Context context) {
            unimplemented();
        }
    }
}
