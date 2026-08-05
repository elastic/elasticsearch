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
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.common.CompletableResultCode;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.apm.internal.export.TraceSupplier;

import static org.elasticsearch.telemetry.apm.internal.export.agent.AgentExportHelpers.agentFlushWaitTimeMs;

/**
 * A {@link TraceSupplier} that returns the {@link GlobalOpenTelemetry} instance for
 * agent-based trace export. Flush sleeps for 2× the agent export interval because the
 * APM agent has no programmatic flush API.
 *
 * @see org.elasticsearch.telemetry.apm.internal.export.agent.AgentExportMeterSupplier
 */
public final class AgentExportTracerSupplier implements TraceSupplier {
    private final Runnable flushFn;

    public AgentExportTracerSupplier(Settings settings) {
        this(() -> AgentExportHelpers.sleepForAgentExport(agentFlushWaitTimeMs(settings)));
    }

    // package-private for testing: allows injecting a recording or no-op flush
    AgentExportTracerSupplier(Runnable flushFn) {
        this.flushFn = flushFn;
    }

    @Override
    public OpenTelemetry get() {
        return GlobalOpenTelemetry.get();
    }

    @Override
    public CompletableResultCode attemptFlushTraces() {
        // Blocks the calling thread: the APM agent has no async flush API, so this sleeps for
        // the configured interval. The result is already complete when this method returns.
        flushFn.run();
        return CompletableResultCode.ofSuccess();
    }
}
