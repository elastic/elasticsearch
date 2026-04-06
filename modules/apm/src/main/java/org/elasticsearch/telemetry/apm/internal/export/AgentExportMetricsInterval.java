/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

/**
 * Parses {@code telemetry.agent.metrics_interval} for agent-mediated export timing (metrics and traces).
 * Used when the Elasticsearch APM Java agent is exporting telemetry (no Elasticsearch-owned flush API).
 */
public final class AgentExportMetricsInterval {

    private static final Logger logger = LogManager.getLogger(AgentExportMetricsInterval.class);

    private static final TimeValue DEFAULT = TimeValue.timeValueSeconds(10);

    private AgentExportMetricsInterval() {}

    /**
     * Interval derived from {@code telemetry.agent.metrics_interval}, or 10 seconds if unset or invalid.
     */
    public static TimeValue agentMetricsInterval(Settings settings) {
        String intervalStr = settings.get("telemetry.agent.metrics_interval");
        if (intervalStr != null && intervalStr.isEmpty() == false) {
            try {
                return TimeValue.parseTimeValue(intervalStr, "telemetry.agent.metrics_interval");
            } catch (Exception e) {
                logger.debug("Could not parse telemetry.agent.metrics_interval [{}], using default", intervalStr);
            }
        }
        return DEFAULT;
    }
}
