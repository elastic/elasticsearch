/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics.internal;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.telemetry.apm.internal.metrics.internal.metrics.APMMeterRegistry;
import org.elasticsearch.telemetry.metric.MeterService;

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

    protected RecordingMeterProvider createRecordingMeter() {
        assert this.enabled;
        return new RecordingMeterProvider();
    }

    protected Meter createNoopMeter() {
        return OpenTelemetry.noop().getMeter("noop");
    }
}
