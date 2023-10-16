/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.telemetry.apm.internal.metrics.APMMeterRegistry;
import org.elasticsearch.telemetry.metric.DoubleCounter;
import org.elasticsearch.telemetry.metric.DoubleGauge;
import org.elasticsearch.telemetry.metric.DoubleGaugeObserver;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.DoubleUpDownCounter;
import org.elasticsearch.telemetry.metric.Instrument;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongGauge;
import org.elasticsearch.telemetry.metric.LongGaugeObserver;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.LongUpDownCounter;
import org.elasticsearch.telemetry.metric.MeterService;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TestAPMMeterService implements MeterService {
    private final APMMeterRegistry meterRegistry;
    private RecordingMeterProvider recordingMeter;
    private boolean enabled;

    public TestAPMMeterService() {
        this.enabled = true;
        this.recordingMeter = createRecordingMeter();
        this.meterRegistry = new APMMeterRegistry(recordingMeter);
    }

    @Override
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
        if (enabled) {
            recordingMeter = createRecordingMeter();
            meterRegistry.setProvider(recordingMeter);
        } else {
            recordingMeter = null;
            meterRegistry.setProvider(createNoopMeter());
        }
    }

    public Meter getMeter() {
        assert recordingMeter != null;
        return recordingMeter;
    }

    public static class Metric {
        final Number number;
        final Map<String, Object> attributes;

        public Metric(Number number, Map<String, Object> attributes) {
            this.number = number;
            this.attributes = attributes;
        }

        public Number getNumber() {
            return number;
        }

        public Map<String, Object> getAttributes() {
            return attributes;
        }
    }

    public List<Metric> getMetrics(Instrument instrument, String name) {
        Objects.requireNonNull(instrument);
        if (instrument instanceof DoubleCounter) {
            return recordingMeter.getRecorder().getDouble(MeterRecorder.INSTRUMENT.COUNTER, name);
        } else if (instrument instanceof LongCounter) {
            return recordingMeter.getRecorder().getLong(MeterRecorder.INSTRUMENT.COUNTER, name);
        } else if (instrument instanceof DoubleUpDownCounter) {
            return recordingMeter.getRecorder().getDouble(MeterRecorder.INSTRUMENT.UP_DOWN_COUNTER, name);
        } else if (instrument instanceof LongUpDownCounter) {
            return recordingMeter.getRecorder().getLong(MeterRecorder.INSTRUMENT.UP_DOWN_COUNTER, name);
        } else if (instrument instanceof DoubleHistogram) {
            return recordingMeter.getRecorder().getDouble(MeterRecorder.INSTRUMENT.HISTOGRAM, name);
        } else if (instrument instanceof LongHistogram) {
            return recordingMeter.getRecorder().getLong(MeterRecorder.INSTRUMENT.HISTOGRAM, name);
        } else if (instrument instanceof DoubleGauge) {
            return recordingMeter.getRecorder().getDouble(MeterRecorder.INSTRUMENT.GAUGE_OBSERVER, name);
        } else if (instrument instanceof LongGauge) {
            return recordingMeter.getRecorder().getLong(MeterRecorder.INSTRUMENT.GAUGE_OBSERVER, name);
        } else {
            throw new IllegalArgumentException("unknown instrument [" + instrument.getClass().getName() + "]");
        }
    }

    public void collectMetrics() {
        assert recordingMeter != null;
        recordingMeter.collectMetrics();
    }

    private RecordingMeterProvider createRecordingMeter() {
        assert this.enabled;
        return new RecordingMeterProvider();
    }

    private Meter createNoopMeter() {
        return OpenTelemetry.noop().getMeter("noop");
    }
}
