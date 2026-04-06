/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.agent;

/**
 * Best-effort wait for the Elasticsearch APM Java agent to export buffered traces or metrics.
 * The agent does not expose a programmatic flush API, so callers use a bounded sleep derived from
 * {@code telemetry.agent.metrics_interval} (see {@link AgentExportMeterSupplier}).
 */
public final class AgentExportTraceFlush {

    private AgentExportTraceFlush() {}

    /**
     * Sleeps for {@code waitMs} so in-flight export is likely complete. Interrupt-safe.
     */
    public static void sleepForAgentExport(long waitMs) {
        try {
            Thread.sleep(waitMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
