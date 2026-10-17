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
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkSettings;
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
import static org.hamcrest.Matchers.hasItem;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class APMAgentSettingsTests extends ESTestCase {
    APMAgentSettings apmAgentSettings = new APMAgentSettings();
    APMTelemetryProvider apmTelemetryProvider = mock(Mockito.RETURNS_DEEP_STUBS);

    public void testEnableTracing() {
        Settings initial = Settings.builder().put(TELEMETRY_TRACING_ENABLED_SETTING.getKey(), false).build();
        Settings update = Settings.builder().put(TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        triggerUpdateConsumer(initial, update);
        verify(apmTelemetryProvider.getTracer()).setEnabled(true);
    }

    public void testDisableTracing() {
        Settings initial = Settings.builder().put(TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        Settings update = Settings.builder().put(TELEMETRY_TRACING_ENABLED_SETTING.getKey(), false).build();
        triggerUpdateConsumer(initial, update);
        verify(apmTelemetryProvider.getTracer()).setEnabled(false);
    }

    public void testEnableMetrics() {
        Settings initial = Settings.builder().put(TELEMETRY_METRICS_ENABLED_SETTING.getKey(), false).build();
        Settings update = Settings.builder().put(TELEMETRY_METRICS_ENABLED_SETTING.getKey(), true).build();
        triggerUpdateConsumer(initial, update);
        verify(apmTelemetryProvider.getMeterService()).setEnabled(true);
    }

    public void testDisableMetrics() {
        Settings initial = Settings.builder().put(TELEMETRY_METRICS_ENABLED_SETTING.getKey(), true).build();
        Settings update = Settings.builder().put(TELEMETRY_METRICS_ENABLED_SETTING.getKey(), false).build();
        triggerUpdateConsumer(initial, update);
        verify(apmTelemetryProvider.getMeterService()).setEnabled(false);
    }

    public void testUpdateMaxTraceDepthPropagatesToTracer() {
        int depth = randomIntBetween(1, 100);
        Settings update = Settings.builder().put(OtelSdkSettings.TELEMETRY_TRACING_MAX_DEPTH.getKey(), depth).build();
        triggerUpdateConsumer(Settings.EMPTY, update);
        verify(apmTelemetryProvider.getTracer()).setMaxTraceDepth(depth);
    }

    public void testUpdateRecordExceptionStacksPropagatesToTracer() {
        Settings update = Settings.builder().put(OtelSdkSettings.TELEMETRY_TRACING_RECORD_EXCEPTION_STACKS.getKey(), true).build();
        triggerUpdateConsumer(Settings.EMPTY, update);
        verify(apmTelemetryProvider.getTracer()).setRecordExceptionStacks(true);
    }

    public void testTracingSampleRateIsNotDynamicallyUpdatable() {
        assertTrue(OtelSdkSettings.TELEMETRY_TRACING_MAX_DEPTH.isDynamic());
        assertTrue(OtelSdkSettings.TELEMETRY_TRACING_RECORD_EXCEPTION_STACKS.isDynamic());
        assertFalse(OtelSdkSettings.TELEMETRY_TRACING_SAMPLE_RATE.isDynamic());
    }

    public void testUpdateInstrumentTimingPropagatesToMeterRegistry() {
        assertTrue(OtelSdkSettings.TELEMETRY_METRICS_INSTRUMENT_TIMING_ENABLED.isDynamic());
        Settings update = Settings.builder().put(OtelSdkSettings.TELEMETRY_METRICS_INSTRUMENT_TIMING_ENABLED.getKey(), true).build();
        triggerUpdateConsumer(Settings.EMPTY, update);
        verify(apmTelemetryProvider.getMeterService().getMeterRegistry()).setInstrumentTimingEnabled(true);
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
                OtelSdkSettings.TELEMETRY_TRACING_MAX_DEPTH,
                OtelSdkSettings.TELEMETRY_TRACING_RECORD_EXCEPTION_STACKS,
                OtelSdkSettings.TELEMETRY_METRICS_INSTRUMENT_TIMING_ENABLED,
                APM_AGENT_SETTINGS
            )
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        apmAgentSettings.addClusterSettingsListeners(clusterService, apmTelemetryProvider);
        clusterSettings.applySettings(update);
    }

    public void testLeftoverAgentSettingsAreAcceptedAndDeprecated() {
        Setting<String> setting = APM_AGENT_SETTINGS.getConcreteSettingForNamespace("some_key_that_never_existed");
        Settings settings = Settings.builder().put(setting.getKey(), "value").build();

        assertEquals("value", setting.get(settings));
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { setting });
    }

    public void testTelemetryTracingSanitizeFieldNamesFallbackDefault() {
        List<String> included = TELEMETRY_TRACING_SANITIZE_FIELD_NAMES.get(Settings.EMPTY);
        assertThat(included, hasItem("password")); // and more defaults
    }
}
