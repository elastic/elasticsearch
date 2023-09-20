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

public class APMTelemetryProvider implements TelemetryProvider {
    private Settings settings;
    final APMMetric apmMetric;
    final APMTracer apmTracer;

    public APMTelemetryProvider(Settings settings) {
        this.settings = settings;
        apmMetric = new APMMetric(settings);
        apmTracer = new APMTracer(settings);
    }

    @Override
    public APMTracer getTracer() {
        return apmTracer;
    }

    @Override
    public APMMetric getMetric() {
        return apmMetric;
    }
}
