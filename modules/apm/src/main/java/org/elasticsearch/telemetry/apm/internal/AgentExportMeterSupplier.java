/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.metrics.Meter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;

import static org.elasticsearch.telemetry.apm.internal.AgentExportMetricsInterval.agentMetricsInterval;

/**
 * Supplies a {@link Meter} from {@link GlobalOpenTelemetry} for metrics export via the
 * Elasticsearch APM Java agent. Application code still uses the OpenTelemetry API; the agent
 * intercepts {@link GlobalOpenTelemetry} and exports to the APM stack (as opposed to the
 * {@link OTelSdkMeterSupplier} path, which uses the OpenTelemetry SDK to export).
 */
public final class AgentExportMeterSupplier implements MeterSupplier {

    private static final Logger logger = LogManager.getLogger(AgentExportMeterSupplier.class);

    private final long agentFlushWaitMs;

    public AgentExportMeterSupplier(Settings settings) {
        this.agentFlushWaitMs = 2 * agentMetricsInterval(settings).millis();
    }

    @Override
    public Meter get() {
        return GlobalOpenTelemetry.get().getMeter("elasticsearch");
    }

    @Override
    public void attemptFlushMetrics() {
        try {
            // The agent offers no flush API, so we do a best-effort pause that exceeds
            // the agent reporting interval, making it extremely likely that all telemetry
            // has been exported.
            //
            // Note that the first intake request to the APM server can still be delayed beyond this window:
            // the Elasticsearch APM Java agent checks for configuration changes only periodically,
            // so the setting changes we made during initialization don't take effect immediately.

            logger.info("Waiting {} ms for Elasticsearch APM Java agent to flush metrics", agentFlushWaitMs);
            Thread.sleep(agentFlushWaitMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
