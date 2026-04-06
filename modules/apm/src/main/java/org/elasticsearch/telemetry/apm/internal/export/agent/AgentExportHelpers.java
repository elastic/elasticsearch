/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.agent;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import static org.elasticsearch.core.TimeValue.parseTimeValue;

public final class AgentExportHelpers {
    private static final Logger logger = LogManager.getLogger(AgentExportHelpers.class);
    private static final long DEFAULT_AGENT_FLUSH_WAIT_MS = TimeValue.timeValueSeconds(10).millis();

    private AgentExportHelpers() {}

    /**
     * Best-effort wait for the Elasticsearch APM Java agent to export buffered traces or metrics.
     * The agent does not expose a programmatic flush API, so callers use a bounded sleep derived from
     * {@code telemetry.agent.metrics_interval} (see {@link AgentExportMeterSupplier}).
     * <p>
     * Note that the first intake request to the APM server can still be delayed beyond this window:
     * the Elasticsearch APM Java agent checks for configuration changes only periodically,
     * so the setting changes we made during initialization don't take effect immediately.
     */
    public static void sleepForAgentExport(long agentFlushWaitTime) {
        logger.debug("Waiting {} ms for Elasticsearch APM Java agent to flush telemetry", agentFlushWaitTime);
        try {
            Thread.sleep(agentFlushWaitTime);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Determines the appropriate duration to wait for the APM agent to flush telemetry.
     */
    public static long agentFlushWaitTimeMs(Settings settings) {
        String intervalStr = settings.get("telemetry.agent.metrics_interval");
        if (intervalStr != null && intervalStr.isEmpty() == false) {
            try {
                return 2 * parseTimeValue(intervalStr, "telemetry.agent.metrics_interval").millis();
            } catch (Exception e) {
                logger.debug("Could not parse telemetry.agent.metrics_interval [{}], using default", intervalStr, e);
            }
        }
        return DEFAULT_AGENT_FLUSH_WAIT_MS;
    }

}
