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
    public void testEnableRecording() {
        APMAgentSettings apmAgentSettings = spy(new APMAgentSettings());
        Settings settings = Settings.builder().put(APMAgentSettings.APM_ENABLED_SETTING.getKey(), true).build();
        apmAgentSettings.syncAgentSystemProperties(settings);

        verify(apmAgentSettings).setAgentSetting("recording", "true");
    }

    /**
     * Check that when the tracer is disabled, it also sets the APM agent's recording system property to false.
     */
    public void testDisableRecording() {
        APMAgentSettings apmAgentSettings = spy(new APMAgentSettings());
        Settings settings = Settings.builder().put(APMAgentSettings.APM_ENABLED_SETTING.getKey(), false).build();
        apmAgentSettings.syncAgentSystemProperties(settings);

        verify(apmAgentSettings).setAgentSetting("recording", "false");
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
        apmAgentSettings.syncAgentSystemProperties(settings);

        verify(apmAgentSettings).setAgentSetting("span_compression_enabled", "true");
    }

    public void testSetAgentsSettings2() {
        APMAgentSettings apmAgentSettings = spy(new APMAgentSettings());
        // TODO (stu): keystore 'tracing.apm.secret_token', 'XXXYYYZZZ'
        Settings settings = Settings.builder()
            .put("tracing.apm.enabled", true)
            .put("telemetry.metrics.enabled", true)
            .put("telemetry.agent.server_url", "https://296a15aa5f8d4fbfa73124bf4ce50417.apm.us-central1.gcp.cloud.es.io:443")
            .put("telemetry.agent.metrics_interval", "1s")
            .build();
        apmAgentSettings.syncAgentSystemProperties(settings);

        verify(apmAgentSettings).setAgentSetting("recording", "true");
        verify(apmAgentSettings).setAgentSetting("metrics_interval", "1s");
        verify(apmAgentSettings).setAgentSetting("server_url", "https://296a15aa5f8d4fbfa73124bf4ce50417.apm.us-central1.gcp.cloud.es.io:443");
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
}
