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

import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingOtelMeter;
import org.elasticsearch.telemetry.metric.Instrument;
import org.elasticsearch.telemetry.metric.MeterService;

import java.util.List;

public class TestAPMMeterService implements MeterService {
    private final APMMeterRegistry meterRegistry;
    private RecordingOtelMeter recordingMeter;
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

    public void collectMetrics() {
        assert recordingMeter != null;
        recordingMeter.collectMetrics();
    }

    public List<Measurement> getMetrics(Instrument instrument) {
        return recordingMeter.getRecorder().getMetrics(instrument);
    }

    private RecordingOtelMeter createRecordingMeter() {
        assert this.enabled;
        return new RecordingOtelMeter();
    }

    private Meter createNoopMeter() {
        return OpenTelemetry.noop().getMeter("noop");
    }
}
