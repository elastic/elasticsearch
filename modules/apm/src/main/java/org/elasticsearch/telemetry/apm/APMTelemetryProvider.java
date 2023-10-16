/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.apm.internal.metrics.APMMeterService;
import org.elasticsearch.telemetry.apm.tracing.APMTracer;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.telemetry.metric.MeterService;

public class APMTelemetryProvider implements TelemetryProvider {
    private final Settings settings;
    private final APMTracer apmTracer;
    private final APMMeterService apmMeterService;

    public APMTelemetryProvider(Settings settings) {
        this.settings = settings;
        apmTracer = new APMTracer(settings);
        apmMeterService = new APMMeterService(settings);
    }

    @Override
    public APMTracer getTracer() {
        return apmTracer;
    }

    @Override
    public MeterRegistry getMeterRegistry() {
        return apmMeterService.getMeterRegistry();
    }

    @Override
    public MeterService getMeterService() {
        return apmMeterService;
    }
}
