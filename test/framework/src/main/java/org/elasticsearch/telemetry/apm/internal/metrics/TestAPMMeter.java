/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.telemetry.apm.internal.metrics.internal.metrics.APMMeter;

import java.security.AccessController;
import java.security.PrivilegedAction;

public class TestAPMMeter extends APMMeter {
    protected RecordingMeterProvider recordingMeter;

    public TestAPMMeter() {
        // don't use the existing suppliers
        super(true, () -> null, () -> null);
    }

    @Override
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
        if (enabled) {
            recordingMeter = createOtelMeter();
            instruments.setProvider(recordingMeter);
        } else {
            recordingMeter = null;
            instruments.setProvider(createNoopMeter());
        }
    }

    @Override
    protected RecordingMeterProvider createOtelMeter() {
        assert this.enabled;
        return recordingMeter = AccessController.doPrivileged((PrivilegedAction<RecordingMeterProvider>) RecordingMeterProvider::new);
    }

    @Override
    protected Meter createNoopMeter() {
        recordingMeter = null;
        return APMMeter.noopMeter().get();
    }
}
