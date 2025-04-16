/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mockito;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.telemetry.apm.internal.APMAgentSettings.APM_AGENT_SETTINGS;
import static org.elasticsearch.telemetry.apm.internal.APMAgentSettings.TELEMETRY_METRICS_ENABLED_SETTING;
import static org.elasticsearch.telemetry.apm.internal.APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING;
import static org.elasticsearch.telemetry.apm.internal.APMAgentSettings.TELEMETRY_TRACING_NAMES_EXCLUDE_SETTING;
import static org.elasticsearch.telemetry.apm.internal.APMAgentSettings.TELEMETRY_TRACING_NAMES_INCLUDE_SETTING;
import static org.elasticsearch.telemetry.apm.internal.APMAgentSettings.TELEMETRY_TRACING_SANITIZE_FIELD_NAMES;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class APMAgentSettingsTests extends ESTestCase {
    APMAgentSettings apmAgentSettings = spy(new APMAgentSettings());
    APMTelemetryProvider apmTelemetryProvider = mock(Mockito.RETURNS_DEEP_STUBS);

    /**
     * Check that when the tracer is enabled, it also sets the APM agent's recording system property to true.
     */
    public void testEnableTracing() {
        for (boolean metricsEnabled : List.of(true, false)) {
            clearInvocations(apmAgentSettings, apmTelemetryProvider.getTracer());

            Settings update = Settings.builder()
                .put(TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true)
                .put(TELEMETRY_METRICS_ENABLED_SETTING.getKey(), metricsEnabled)
                .build();
            apmAgentSettings.initAgentSystemProperties(update);

            verify(apmAgentSettings).setAgentSetting("recording", "true");
            clearInvocations(apmAgentSettings);

            Settings initial = Settings.builder().put(update).put(TELEMETRY_TRACING_ENABLED_SETTING.getKey(), false).build();
            triggerUpdateConsumer(initial, update);
            verify(apmAgentSettings).setAgentSetting("recording", "true");
            verify(apmTelemetryProvider.getTracer()).setEnabled(true);
        }
    }

    public void testEnableMetrics() {
        for (boolean tracingEnabled : List.of(true, false)) {
            clearInvocations(apmAgentSettings, apmTelemetryProvider.getMeterService());

            Settings update = Settings.builder()
                .put(TELEMETRY_METRICS_ENABLED_SETTING.getKey(), true)
                .put(TELEMETRY_TRACING_ENABLED_SETTING.getKey(), tracingEnabled)
                .build();
            apmAgentSettings.initAgentSystemProperties(update);

            verify(apmAgentSettings).setAgentSetting("recording", "true");
            clearInvocations(apmAgentSettings);

            Settings initial = Settings.builder().put(update).put(TELEMETRY_METRICS_ENABLED_SETTING.getKey(), false).build();
            triggerUpdateConsumer(initial, update);
            verify(apmAgentSettings).setAgentSetting("recording", "true");
            verify(apmTelemetryProvider.getMeterService()).setEnabled(true);
        }
    }

    /**
     * Check that when the tracer is disabled, it also sets the APM agent's recording system property to false unless metrics are enabled.
     */
    public void testDisableTracing() {
        for (boolean metricsEnabled : List.of(true, false)) {
            clearInvocations(apmAgentSettings, apmTelemetryProvider.getTracer());

            Settings update = Settings.builder()
                .put(TELEMETRY_TRACING_ENABLED_SETTING.getKey(), false)
                .put(TELEMETRY_METRICS_ENABLED_SETTING.getKey(), metricsEnabled)
                .build();
            apmAgentSettings.initAgentSystemProperties(update);

            verify(apmAgentSettings).setAgentSetting("recording", Boolean.toString(metricsEnabled));
            clearInvocations(apmAgentSettings);

            Settings initial = Settings.builder().put(update).put(TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
            triggerUpdateConsumer(initial, update);
            verify(apmAgentSettings).setAgentSetting("recording", Boolean.toString(metricsEnabled));
            verify(apmTelemetryProvider.getTracer()).setEnabled(false);
        }
    }

    public void testDisableMetrics() {
        for (boolean tracingEnabled : List.of(true, false)) {
            clearInvocations(apmAgentSettings, apmTelemetryProvider.getMeterService());

            Settings update = Settings.builder()
                .put(TELEMETRY_TRACING_ENABLED_SETTING.getKey(), tracingEnabled)
                .put(TELEMETRY_METRICS_ENABLED_SETTING.getKey(), false)
                .build();
            apmAgentSettings.initAgentSystemProperties(update);

            verify(apmAgentSettings).setAgentSetting("recording", Boolean.toString(tracingEnabled));
            clearInvocations(apmAgentSettings);

            Settings initial = Settings.builder().put(update).put(TELEMETRY_METRICS_ENABLED_SETTING.getKey(), true).build();
            triggerUpdateConsumer(initial, update);
            verify(apmAgentSettings).setAgentSetting("recording", Boolean.toString(tracingEnabled));
            verify(apmTelemetryProvider.getMeterService()).setEnabled(false);
        }
    }

    private void triggerUpdateConsumer(Settings initial, Settings update) {
        ClusterService clusterService = mock();
        ClusterSettings clusterSettings = new ClusterSettings(
            initial,
            Set.of(
                TELEMETRY_TRACING_ENABLED_SETTING,
                TELEMETRY_METRICS_ENABLED_SETTING,
                TELEMETRY_TRACING_NAMES_INCLUDE_SETTING,
                TELEMETRY_TRACING_NAMES_EXCLUDE_SETTING,
                TELEMETRY_TRACING_SANITIZE_FIELD_NAMES,
                APM_AGENT_SETTINGS
            )
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        apmAgentSettings.addClusterSettingsListeners(clusterService, apmTelemetryProvider);
        clusterSettings.applySettings(update);
    }

    /**
     * Check that when cluster settings are synchronised with the system properties, agent settings are set.
     */
    public void testSetAgentSettings() {
        Settings settings = Settings.builder()
            .put(TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true)
            .put(APM_AGENT_SETTINGS.getKey() + "span_compression_enabled", "true")
            .build();
        apmAgentSettings.initAgentSystemProperties(settings);

        verify(apmAgentSettings).setAgentSetting("recording", "true");
        verify(apmAgentSettings).setAgentSetting("span_compression_enabled", "true");
    }

    /**
     * Check that invalid or forbidden APM agent settings are rejected.
     */
    public void testRejectForbiddenOrUnknownAgentSettings() {
        String prefix = APM_AGENT_SETTINGS.getKey();
        Settings settings = Settings.builder().put(prefix + "unknown", "true").build();
        Exception exception = expectThrows(IllegalArgumentException.class, () -> APM_AGENT_SETTINGS.getAsMap(settings));
        assertThat(exception.getMessage(), containsString("[" + prefix + "unknown]"));

        // though, accept / ignore nested global_labels
        var map = APMAgentSettings.APM_AGENT_SETTINGS.getAsMap(Settings.builder().put(prefix + "global_labels.abc", "123").build());
        assertThat(map, hasEntry("global_labels.abc", "123"));
    }

    public void testTelemetryTracingSanitizeFieldNamesFallbackDefault() {
        List<String> included = TELEMETRY_TRACING_SANITIZE_FIELD_NAMES.get(Settings.EMPTY);
        assertThat(included, hasItem("password")); // and more defaults
    }

    /**
     * Check that invalid or forbidden APM agent settings are rejected if their last part resembles an allowed setting.
     */
    public void testRejectUnknownSettingResemblingAnAllowedOne() {
        Settings settings = Settings.builder().put(APM_AGENT_SETTINGS.getKey() + "unknown.service_name", "true").build();

        Exception exception = expectThrows(IllegalArgumentException.class, () -> APM_AGENT_SETTINGS.getAsMap(settings));
        assertThat(exception.getMessage(), containsString("[telemetry.agent.unknown.service_name]"));
    }
}
