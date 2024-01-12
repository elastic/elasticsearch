/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class APMAgentSettingsTests extends ESTestCase {

    /**
     * Check that when the tracer is enabled, it also sets the APM agent's recording system property to true.
     */
    public void testEnableTracing() {
        boolean metricsEnabled = randomBoolean();

        APMAgentSettings apmAgentSettings = spy(new APMAgentSettings());
        Settings settings = Settings.builder()
            .put(APMAgentSettings.APM_ENABLED_SETTING.getKey(), true)
            .put(APMAgentSettings.TELEMETRY_METRICS_ENABLED_SETTING.getKey(), metricsEnabled)
            .build();
        apmAgentSettings.initAgentSystemProperties(settings);

        verify(apmAgentSettings).setAgentSetting("recording", "true");
        verify(apmAgentSettings).setAgentSetting("instrument", "true");
    }

    /**
     * Check that when the tracer is disabled, it also sets the APM agent's recording system property to false unless metrics are enabled.
     */
    public void testDisableTracing() {
        boolean metricsEnabled = randomBoolean();

        APMAgentSettings apmAgentSettings = spy(new APMAgentSettings());
        Settings settings = Settings.builder()
            .put(APMAgentSettings.APM_ENABLED_SETTING.getKey(), false)
            .put(APMAgentSettings.TELEMETRY_METRICS_ENABLED_SETTING.getKey(), metricsEnabled)
            .build();
        apmAgentSettings.initAgentSystemProperties(settings);

        verify(apmAgentSettings).setAgentSetting("recording", Boolean.toString(metricsEnabled));
        verify(apmAgentSettings).setAgentSetting("instrument", "false");
    }

    /**
     * Check that when cluster settings are synchronised with the system properties, agent settings are set.
     */
    public void testSetAgentSettings() {
        APMAgentSettings apmAgentSettings = spy(new APMAgentSettings());
        Settings settings = Settings.builder()
            .put(APMAgentSettings.APM_ENABLED_SETTING.getKey(), true)
            .put(APMAgentSettings.APM_AGENT_SETTINGS.getKey() + "span_compression_enabled", "true")
            .build();
        apmAgentSettings.initAgentSystemProperties(settings);

        verify(apmAgentSettings).setAgentSetting("span_compression_enabled", "true");
    }

    /**
     * Check that invalid or forbidden APM agent settings are rejected.
     */
    public void testRejectForbiddenOrUnknownSettings() {
        Settings settings = Settings.builder()
            .put(APMAgentSettings.APM_ENABLED_SETTING.getKey(), true)
            .put(APMAgentSettings.APM_AGENT_SETTINGS.getKey() + "unknown", "true")
            .build();

        Exception exception = expectThrows(IllegalArgumentException.class, () -> APMAgentSettings.APM_AGENT_SETTINGS.getAsMap(settings));
        assertThat(exception.getMessage(), containsString("[tracing.apm.agent.unknown]"));
    }

    /**
     * Check that invalid or forbidden APM agent settings are rejected if their last part resembles an allowed setting.
     */
    public void testRejectUnknownSettingResemblingAnAllowedOne() {
        Settings settings = Settings.builder()
            .put(APMAgentSettings.APM_ENABLED_SETTING.getKey(), true)
            .put(APMAgentSettings.APM_AGENT_SETTINGS.getKey() + "unknown.service_name", "true")
            .build();

        Exception exception = expectThrows(IllegalArgumentException.class, () -> APMAgentSettings.APM_AGENT_SETTINGS.getAsMap(settings));
        assertThat(exception.getMessage(), containsString("[tracing.apm.agent.unknown.service_name]"));
    }
}
