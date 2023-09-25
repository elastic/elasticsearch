/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.apm.internal.metrics.APMMeter;
import org.elasticsearch.telemetry.apm.internal.tracing.APMTracer;

public class APMTelemetryProvider implements TelemetryProvider {
    private final Settings settings;
    private final APMTracer apmTracer;
    private final APMMeter apmMeter;

    public APMTelemetryProvider(Settings settings) {
        this.settings = settings;
        apmTracer = new APMTracer(settings);
        apmMeter = new APMMeter(settings);
    }

    @Override
    public APMTracer getTracer() {
        return apmTracer;
    }

    @Override
    public APMMeter getMeter() {
        return apmMeter;
    }
}
