/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.agent;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.telemetry.apm.internal.export.agent.AgentExportHelpers.agentFlushWaitTimeMs;
import static org.hamcrest.Matchers.equalTo;

/**
 * Parameterized cases where {@link AgentExportHelpers#agentFlushWaitTimeMs} falls back to
 * {@code 2 *} {@link AgentExportHelpers#DEFAULT_AGENT_METRICS_INTERVAL}.
 */
public class AgentExportHelpersAgentFlushWaitFallbackTests extends ESTestCase {

    private final String scenario;
    private final Settings settings;

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return List.of(
            new Object[] { "absent", Settings.EMPTY },
            new Object[] { "empty", Settings.builder().put("telemetry.agent.metrics_interval", "").build() },
            new Object[] { "invalid", Settings.builder().put("telemetry.agent.metrics_interval", "not-a-duration").build() },
            new Object[] { "negativeMillis", Settings.builder().put("telemetry.agent.metrics_interval", "-1").build() }
        );
    }

    public AgentExportHelpersAgentFlushWaitFallbackTests(@Name("scenario") String scenario, Settings settings) {
        this.scenario = scenario;
        this.settings = settings;
    }

    public void testUsesTwiceDefaultInterval() {
        long expected = 2 * AgentExportHelpers.DEFAULT_AGENT_METRICS_INTERVAL.millis();
        assertThat("scenario [" + scenario + "]", agentFlushWaitTimeMs(settings), equalTo(expected));
    }
}
