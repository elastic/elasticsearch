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

    /**
     * Default for {@code telemetry.agent.metrics_interval} when unset or invalid: used only to derive
     * {@link #agentFlushWaitTimeMs} as {@code 2 *} this interval, consistent with the parseable setting path.
     */
    static final TimeValue DEFAULT_AGENT_METRICS_INTERVAL = TimeValue.timeValueSeconds(10);

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
     * When {@code telemetry.agent.metrics_interval} is unset, empty, or invalid, uses
     * {@code 2 *} {@link #DEFAULT_AGENT_METRICS_INTERVAL}, matching the formula used when the setting parses.
     * <p>
     * When the value parses to a non-negative interval, returns {@code 2 *} that interval in milliseconds.
     * Negative parsed intervals, values whose doubled wait would overflow {@code long}, or parse failures
     * fall back to {@code 2 *} {@link #DEFAULT_AGENT_METRICS_INTERVAL}.
     */
    public static long agentFlushWaitTimeMs(Settings settings) {
        String intervalStr = settings.get("telemetry.agent.metrics_interval");
        if (intervalStr != null && intervalStr.isEmpty() == false) {
            try {
                long intervalMillis = parseTimeValue(intervalStr, "telemetry.agent.metrics_interval").millis();
                if (intervalMillis < 0) {
                    logger.debug(
                        "telemetry.agent.metrics_interval [{}] parsed to negative millis [{}], using default",
                        intervalStr,
                        intervalMillis
                    );
                } else {
                    return Math.multiplyExact(2, intervalMillis);
                }
            } catch (ArithmeticException e) {
                logger.debug("telemetry.agent.metrics_interval [{}] doubled flush wait overflows long, using Long.MAX_VALUE", intervalStr);
                return Long.MAX_VALUE;
            } catch (Exception e) {
                logger.debug("Could not parse telemetry.agent.metrics_interval [{}], using default", intervalStr, e);
            }
        }
        return 2 * DEFAULT_AGENT_METRICS_INTERVAL.millis();
    }

}
