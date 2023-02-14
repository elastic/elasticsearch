/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tracing.apm;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.tracing.apm.APMAgentSettings.APM_AGENT_SETTINGS;
import static org.elasticsearch.tracing.apm.APMAgentSettings.APM_ENABLED_SETTING;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class APMAgentSettingsTests extends ESTestCase {

    /**
     * Check that when the tracer is enabled, it also sets the APM agent's recording system property to true.
     */
    public void test_whenTracerEnabled_setsRecordingProperty() {
        APMAgentSettings apmAgentSettings = spy(new APMAgentSettings());
        Settings settings = Settings.builder().put(APM_ENABLED_SETTING.getKey(), true).build();
        apmAgentSettings.syncAgentSystemProperties(settings);

        verify(apmAgentSettings).setAgentSetting("recording", "true");
    }

    /**
     * Check that when the tracer is disabled, it also sets the APM agent's recording system property to false.
     */
    public void test_whenTracerDisabled_setsRecordingProperty() {
        APMAgentSettings apmAgentSettings = spy(new APMAgentSettings());
        Settings settings = Settings.builder().put(APM_ENABLED_SETTING.getKey(), false).build();
        apmAgentSettings.syncAgentSystemProperties(settings);

        verify(apmAgentSettings).setAgentSetting("recording", "false");
    }

    /**
     * Check that when cluster settings are synchronised with the system properties, default values are
     * applied.
     */
    public void test_whenTracerCreated_defaultSettingsApplied() {
        APMAgentSettings apmAgentSettings = spy(new APMAgentSettings());
        Settings settings = Settings.builder().put(APM_ENABLED_SETTING.getKey(), true).build();
        apmAgentSettings.syncAgentSystemProperties(settings);

        verify(apmAgentSettings).setAgentSetting("transaction_sample_rate", "0.2");
    }

    /**
     * Check that when cluster settings are synchronised with the system properties, values in the settings
     * are reflected in the system properties, overwriting default values.
     */
    public void test_whenTracerCreated_clusterSettingsOverrideDefaults() {
        APMAgentSettings apmAgentSettings = spy(new APMAgentSettings());
        Settings settings = Settings.builder()
            .put(APM_ENABLED_SETTING.getKey(), true)
            .put(APM_AGENT_SETTINGS.getKey() + "transaction_sample_rate", "0.75")
            .build();
        apmAgentSettings.syncAgentSystemProperties(settings);

        // This happens twice because we first apply the default settings, whose values are overridden
        // from the cluster settings, then we apply all the APM-agent related settings, not just the
        // ones with default values. Although there is some redundancy here, it only happens at startup
        // for a very small number of settings.
        verify(apmAgentSettings, times(2)).setAgentSetting("transaction_sample_rate", "0.75");
    }

    /**
     * Check that when cluster settings are synchronised with the system properties, agent settings other
     * than those with default values are set.
     */
    public void test_whenTracerCreated_clusterSettingsAlsoApplied() {
        APMAgentSettings apmAgentSettings = spy(new APMAgentSettings());
        Settings settings = Settings.builder()
            .put(APM_ENABLED_SETTING.getKey(), true)
            .put(APM_AGENT_SETTINGS.getKey() + "span_compression_enabled", "true")
            .build();
        apmAgentSettings.syncAgentSystemProperties(settings);

        verify(apmAgentSettings).setAgentSetting("span_compression_enabled", "true");
    }
}
