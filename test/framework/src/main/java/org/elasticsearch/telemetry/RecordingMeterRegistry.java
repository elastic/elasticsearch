/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry;

import org.elasticsearch.telemetry.metric.DoubleAsyncCounter;
import org.elasticsearch.telemetry.metric.DoubleCounter;
import org.elasticsearch.telemetry.metric.DoubleGauge;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.DoubleUpDownCounter;
import org.elasticsearch.telemetry.metric.DoubleWithAttributes;
import org.elasticsearch.telemetry.metric.Instrument;
import org.elasticsearch.telemetry.metric.LongAsyncCounter;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongGauge;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.LongUpDownCounter;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;

/**
 * A {@link MeterRegistry} that records all instrument invocations.
 * Tests can subclass this class and extend the build[Instrument] methods to do their
 * own validations at instrument registration time and/or provide their own instruments.
 */
public class RecordingMeterRegistry implements MeterRegistry {
    protected final MetricRecorder<Instrument> recorder = new MetricRecorder<>();

    public MetricRecorder<Instrument> getRecorder() {
        return recorder;
    }

    @Override
    public DoubleCounter registerDoubleCounter(String name, String description, String unit) {
        DoubleCounter instrument = buildDoubleCounter(name, description, unit);
        recorder.register(instrument, InstrumentType.fromInstrument(instrument), name, description, unit);
        return instrument;
    }

    @Override
    public DoubleCounter getDoubleCounter(String name) {
        return (DoubleCounter) recorder.getInstrument(InstrumentType.DOUBLE_COUNTER, name);
    }

    protected DoubleCounter buildDoubleCounter(String name, String description, String unit) {
        return new RecordingInstruments.RecordingDoubleCounter(name, recorder);
    }

    @Override
    public DoubleUpDownCounter registerDoubleUpDownCounter(String name, String description, String unit) {
        DoubleUpDownCounter instrument = buildDoubleUpDownCounter(name, description, unit);
        recorder.register(instrument, InstrumentType.fromInstrument(instrument), name, description, unit);
        return instrument;
    }

    @Override
    public DoubleUpDownCounter getDoubleUpDownCounter(String name) {
        return (DoubleUpDownCounter) recorder.getInstrument(InstrumentType.DOUBLE_UP_DOWN_COUNTER, name);
    }

    protected DoubleUpDownCounter buildDoubleUpDownCounter(String name, String description, String unit) {
        return new RecordingInstruments.RecordingDoubleUpDownCounter(name, recorder);
    }

    @Override
    public DoubleGauge registerDoubleGauge(String name, String description, String unit, Supplier<DoubleWithAttributes> observer) {
        return registerDoublesGauge(name, description, unit, () -> Collections.singleton(observer.get()));
    }

    @Override
    public DoubleGauge registerDoublesGauge(
        String name,
        String description,
        String unit,
        Supplier<Collection<DoubleWithAttributes>> observer
    ) {
        DoubleGauge instrument = buildDoubleGauge(name, description, unit, observer);
        recorder.register(instrument, InstrumentType.fromInstrument(instrument), name, description, unit);
        return instrument;
    }

    @Override
    public DoubleGauge getDoubleGauge(String name) {
        return (DoubleGauge) recorder.getInstrument(InstrumentType.DOUBLE_GAUGE, name);
    }

    protected DoubleGauge buildDoubleGauge(
        String name,
        String description,
        String unit,
        Supplier<Collection<DoubleWithAttributes>> observer
    ) {
        return new RecordingInstruments.RecordingDoubleGauge(name, observer, recorder);
    }

    @Override
    public DoubleHistogram registerDoubleHistogram(String name, String description, String unit) {
        DoubleHistogram instrument = buildDoubleHistogram(name, description, unit);
        recorder.register(instrument, InstrumentType.fromInstrument(instrument), name, description, unit);
        return instrument;
    }

    @Override
    public DoubleHistogram getDoubleHistogram(String name) {
        return (DoubleHistogram) recorder.getInstrument(InstrumentType.DOUBLE_HISTOGRAM, name);
    }

    protected DoubleHistogram buildDoubleHistogram(String name, String description, String unit) {
        return new RecordingInstruments.RecordingDoubleHistogram(name, recorder);
    }

    @Override
    public LongCounter registerLongCounter(String name, String description, String unit) {
        LongCounter instrument = buildLongCounter(name, description, unit);
        recorder.register(instrument, InstrumentType.fromInstrument(instrument), name, description, unit);
        return instrument;
    }

    @Override
    public LongAsyncCounter registerLongAsyncCounter(String name, String description, String unit, Supplier<LongWithAttributes> observer) {
        return registerLongsAsyncCounter(name, description, unit, () -> Collections.singleton(observer.get()));
    }

    @Override
    public LongAsyncCounter registerLongsAsyncCounter(
        String name,
        String description,
        String unit,
        Supplier<Collection<LongWithAttributes>> observer
    ) {
        LongAsyncCounter instrument = new RecordingInstruments.RecordingAsyncLongCounter(name, observer, recorder);
        recorder.register(instrument, InstrumentType.fromInstrument(instrument), name, description, unit);
        return instrument;
    }

    @Override
    public LongAsyncCounter getLongAsyncCounter(String name) {
        return (LongAsyncCounter) recorder.getInstrument(InstrumentType.LONG_ASYNC_COUNTER, name);
    }

    @Override
    public DoubleAsyncCounter registerDoubleAsyncCounter(
        String name,
        String description,
        String unit,
        Supplier<DoubleWithAttributes> observer
    ) {
        return registerDoublesAsyncCounter(name, description, unit, () -> Collections.singleton(observer.get()));
    }

    @Override
    public DoubleAsyncCounter registerDoublesAsyncCounter(
        String name,
        String description,
        String unit,
        Supplier<Collection<DoubleWithAttributes>> observer
    ) {
        DoubleAsyncCounter instrument = new RecordingInstruments.RecordingAsyncDoubleCounter(name, observer, recorder);
        recorder.register(instrument, InstrumentType.fromInstrument(instrument), name, description, unit);
        return instrument;
    }

    @Override
    public DoubleAsyncCounter getDoubleAsyncCounter(String name) {
        return (DoubleAsyncCounter) recorder.getInstrument(InstrumentType.DOUBLE_ASYNC_COUNTER, name);

    }

    @Override
    public LongCounter getLongCounter(String name) {
        return (LongCounter) recorder.getInstrument(InstrumentType.LONG_COUNTER, name);
    }

    protected LongCounter buildLongCounter(String name, String description, String unit) {
        return new RecordingInstruments.RecordingLongCounter(name, recorder);
    }

    @Override
    public LongUpDownCounter registerLongUpDownCounter(String name, String description, String unit) {
        LongUpDownCounter instrument = buildLongUpDownCounter(name, description, unit);
        recorder.register(instrument, InstrumentType.fromInstrument(instrument), name, description, unit);
        return instrument;
    }

    @Override
    public LongUpDownCounter getLongUpDownCounter(String name) {
        return (LongUpDownCounter) recorder.getInstrument(InstrumentType.LONG_UP_DOWN_COUNTER, name);
    }

    protected LongUpDownCounter buildLongUpDownCounter(String name, String description, String unit) {
        return new RecordingInstruments.RecordingLongUpDownCounter(name, recorder);
    }

    @Override
    public LongGauge registerLongGauge(String name, String description, String unit, Supplier<LongWithAttributes> observer) {
        return registerLongsGauge(name, description, unit, () -> Collections.singleton(observer.get()));
    }

    @Override
    public LongGauge registerLongsGauge(String name, String description, String unit, Supplier<Collection<LongWithAttributes>> observer) {
        LongGauge instrument = buildLongGauge(name, description, unit, observer);
        recorder.register(instrument, InstrumentType.fromInstrument(instrument), name, description, unit);
        return instrument;
    }

    @Override
    public LongGauge getLongGauge(String name) {
        return (LongGauge) recorder.getInstrument(InstrumentType.LONG_GAUGE, name);
    }

    protected LongGauge buildLongGauge(String name, String description, String unit, Supplier<Collection<LongWithAttributes>> observer) {
        return new RecordingInstruments.RecordingLongGauge(name, observer, recorder);
    }

    @Override
    public LongHistogram registerLongHistogram(String name, String description, String unit) {
        LongHistogram instrument = buildLongHistogram(name, description, unit);
        recorder.register(instrument, InstrumentType.fromInstrument(instrument), name, description, unit);
        return instrument;
    }

    @Override
    public LongHistogram getLongHistogram(String name) {
        return (LongHistogram) recorder.getInstrument(InstrumentType.LONG_HISTOGRAM, name);
    }

    protected LongHistogram buildLongHistogram(String name, String description, String unit) {
        return new RecordingInstruments.RecordingLongHistogram(name, recorder);
    }
}
