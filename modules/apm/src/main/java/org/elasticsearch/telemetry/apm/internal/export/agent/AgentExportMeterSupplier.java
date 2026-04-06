/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.agent;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.apm.internal.export.MeterSupplier;

import static org.elasticsearch.telemetry.apm.internal.export.agent.AgentExportHelpers.agentFlushWaitTimeMs;
import static org.elasticsearch.telemetry.apm.internal.export.agent.AgentExportHelpers.sleepForAgentExport;

/**
 * A {@link MeterSupplier} that supplies a {@link Meter} from {@link GlobalOpenTelemetry}
 * for metrics export via the Elasticsearch APM Java agent.
 * Application code still uses the OpenTelemetry API to report metrics.
 *
 * @see org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkExportMeterSupplier
 */
public final class AgentExportMeterSupplier implements MeterSupplier {
    private final long agentFlushWaitTime;

    public AgentExportMeterSupplier(Settings settings) {
        agentFlushWaitTime = agentFlushWaitTimeMs(settings);
    }

    @Override
    public Meter get() {
        return GlobalOpenTelemetry.get().getMeter("elasticsearch");
    }

    @Override
    public void attemptFlushMetrics() {
        sleepForAgentExport(agentFlushWaitTime);
    }
}
