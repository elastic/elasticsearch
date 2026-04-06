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
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.core.TimeValue.parseTimeValue;
import static org.elasticsearch.telemetry.apm.internal.export.agent.AgentExportHelpers.agentFlushWaitTimeMs;
import static org.elasticsearch.telemetry.apm.internal.export.agent.AgentExportHelpers.sleepForAgentExport;
import static org.hamcrest.Matchers.equalTo;

public class AgentExportHelpersTests extends ESTestCase {

    public void testAgentFlushWaitTimeMs_validInterval() {
        for (String intervalStr : new String[] { "1s", "500ms" }) {
            Settings settings = Settings.builder().put("telemetry.agent.metrics_interval", intervalStr).build();
            long expected = 2 * parseTimeValue(intervalStr, "telemetry.agent.metrics_interval").millis();
            assertThat("interval [" + intervalStr + "]", agentFlushWaitTimeMs(settings), equalTo(expected));
        }
    }

    public void testAgentFlushWaitTimeMs_overflow() {
        String intervalStr = Long.MAX_VALUE + "ms";
        Settings settings = Settings.builder().put("telemetry.agent.metrics_interval", intervalStr).build();
        long expected = Long.MAX_VALUE;
        assertThat(agentFlushWaitTimeMs(settings), equalTo(expected));
    }

    public void testSleepForAgentExport_restoresInterruptStatus() {
        sleepForAgentExport(0);
        assertFalse(Thread.interrupted());

        Thread.currentThread().interrupt();
        sleepForAgentExport(0);
        assertTrue(Thread.interrupted());
    }
}
