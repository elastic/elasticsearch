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

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class APMAgentSettingsTests extends ESTestCase {

    /**
     * Check that when the tracer is enabled, it also sets the APM agent's recording system property to true.
     */
    public void test_whenTracerEnabled_setsRecordingProperty() {
        APMAgentSettings apmAgentSettings = spy(new APMAgentSettings());
        Settings settings = Settings.builder().put(APMAgentSettings.APM_ENABLED_SETTING.getKey(), true).build();
        apmAgentSettings.syncAgentSystemProperties(settings);

        verify(apmAgentSettings).setAgentSetting("recording", "true");
    }

    /**
     * Check that when the tracer is disabled, it also sets the APM agent's recording system property to false.
     */
    public void test_whenTracerDisabled_setsRecordingProperty() {
        APMAgentSettings apmAgentSettings = spy(new APMAgentSettings());
        Settings settings = Settings.builder().put(APMAgentSettings.APM_ENABLED_SETTING.getKey(), false).build();
        apmAgentSettings.syncAgentSystemProperties(settings);

        verify(apmAgentSettings).setAgentSetting("recording", "false");
    }

    /**
     * Check that when cluster settings are synchronised with the system properties, agent settings are set.
     */
    public void test_whenTracerCreated_clusterSettingsAlsoApplied() {
        APMAgentSettings apmAgentSettings = spy(new APMAgentSettings());
        Settings settings = Settings.builder()
            .put(APMAgentSettings.APM_ENABLED_SETTING.getKey(), true)
            .put(APMAgentSettings.APM_AGENT_SETTINGS.getKey() + "span_compression_enabled", "true")
            .build();
        apmAgentSettings.syncAgentSystemProperties(settings);

        verify(apmAgentSettings).setAgentSetting("span_compression_enabled", "true");
    }
}
