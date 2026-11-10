/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkSettings;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.telemetry.apm.internal.APMAgentSettings.APM_AGENT_SETTINGS;
import static org.elasticsearch.telemetry.apm.internal.APMAgentSettings.TELEMETRY_TRACING_SANITIZE_FIELD_NAMES;
import static org.hamcrest.Matchers.hasItem;

public class APMAgentSettingsTests extends ESTestCase {
    public void testTracingSampleRateIsNotDynamicallyUpdatable() {
        assertTrue(OtelSdkSettings.TELEMETRY_TRACING_MAX_DEPTH.isDynamic());
        assertTrue(OtelSdkSettings.TELEMETRY_TRACING_RECORD_EXCEPTION_STACKS.isDynamic());
        assertFalse(OtelSdkSettings.TELEMETRY_TRACING_SAMPLE_RATE.isDynamic());
    }

    public void testInstrumentTimingIsDynamicallyUpdatable() {
        assertTrue(OtelSdkSettings.TELEMETRY_METRICS_INSTRUMENT_TIMING_ENABLED.isDynamic());
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
