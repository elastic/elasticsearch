/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.apm.APMMeterRegistry;
import org.elasticsearch.telemetry.apm.internal.tracing.APMTracer;

public class APMTelemetryProvider implements TelemetryProvider {
    private final APMTracer apmTracer;
    private final APMMeterService apmMeterService;

    public APMTelemetryProvider(Settings settings) {
        apmTracer = new APMTracer(settings);
        apmMeterService = new APMMeterService(settings);
    }

    @Override
    public APMTracer getTracer() {
        return apmTracer;
    }

    public APMMeterService getMeterService() {
        return apmMeterService;
    }

    @Override
    public APMMeterRegistry getMeterRegistry() {
        return apmMeterService.getMeterRegistry();
    }
}
