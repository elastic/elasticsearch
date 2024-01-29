/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class APMAgentSettingsTests extends ESTestCase {

    /**
     * Check that when the tracer is enabled, it also sets the APM agent's recording system property to true.
     */
    public void testEnableTracing() {
        APMAgentSettings apmAgentSettings = spy(new APMAgentSettings());
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        apmAgentSettings.syncAgentSystemProperties(settings);

        verify(apmAgentSettings).setAgentSetting("recording", "true");
    }

    public void testEnableTracingUsingLegacySetting() {
        APMAgentSettings apmAgentSettings = spy(new APMAgentSettings());
        Settings settings = Settings.builder().put(APMAgentSettings.TRACING_APM_ENABLED_SETTING.getKey(), true).build();
        apmAgentSettings.syncAgentSystemProperties(settings);

        verify(apmAgentSettings).setAgentSetting("recording", "true");
        assertWarnings("[tracing.apm.enabled] setting was deprecated in Elasticsearch and will be removed in a future release.");
    }

    /**
     * Check that when the tracer is disabled, it also sets the APM agent's recording system property to false.
     */
    public void testDisableTracing() {
        APMAgentSettings apmAgentSettings = spy(new APMAgentSettings());
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), false).build();
        apmAgentSettings.syncAgentSystemProperties(settings);

        verify(apmAgentSettings).setAgentSetting("recording", "false");
    }

    public void testDisableTracingUsingLegacySetting() {
        APMAgentSettings apmAgentSettings = spy(new APMAgentSettings());
        Settings settings = Settings.builder().put(APMAgentSettings.TRACING_APM_ENABLED_SETTING.getKey(), false).build();
        apmAgentSettings.syncAgentSystemProperties(settings);

        verify(apmAgentSettings).setAgentSetting("recording", "false");
        assertWarnings("[tracing.apm.enabled] setting was deprecated in Elasticsearch and will be removed in a future release.");
    }

    /**
     * Check that when cluster settings are synchronised with the system properties, agent settings are set.
     */
    public void testSetAgentSettings() {
        APMAgentSettings apmAgentSettings = spy(new APMAgentSettings());
        Settings settings = Settings.builder()
            .put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true)
            .put(APMAgentSettings.APM_AGENT_SETTINGS.getKey() + "span_compression_enabled", "true")
            .build();
        apmAgentSettings.syncAgentSystemProperties(settings);

        verify(apmAgentSettings).setAgentSetting("recording", "true");
        verify(apmAgentSettings).setAgentSetting("span_compression_enabled", "true");
    }

    public void testSetAgentsSettingsWithLegacyPrefix() {
        APMAgentSettings apmAgentSettings = spy(new APMAgentSettings());
        Settings settings = Settings.builder()
            .put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true)
            .put("tracing.apm.agent.span_compression_enabled", "true")
            .build();
        apmAgentSettings.syncAgentSystemProperties(settings);

        verify(apmAgentSettings).setAgentSetting("recording", "true");
        verify(apmAgentSettings).setAgentSetting("span_compression_enabled", "true");
    }

    /**
     * Check that invalid or forbidden APM agent settings are rejected.
     */
    public void testRejectForbiddenOrUnknownAgentSettings() {
        List<String> prefixes = List.of(APMAgentSettings.APM_AGENT_SETTINGS.getKey(), "tracing.apm.agent.");
        for (String prefix : prefixes) {
            Settings settings = Settings.builder().put(prefix + "unknown", "true").build();
            Exception exception = expectThrows(
                IllegalArgumentException.class,
                () -> APMAgentSettings.APM_AGENT_SETTINGS.getAsMap(settings)
            );
            assertThat(exception.getMessage(), containsString("[" + prefix + "unknown]"));
        }
        // though, accept / ignore nested global_labels
        for (String prefix : prefixes) {
            Settings settings = Settings.builder().put(prefix + "global_labels." + randomAlphaOfLength(5), "123").build();
            APMAgentSettings.APM_AGENT_SETTINGS.getAsMap(settings);
        }
    }

    public void testTelemetryTracingNamesIncludeFallback() {
        Settings settings = Settings.builder().put(APMAgentSettings.TRACING_APM_NAMES_INCLUDE_SETTING.getKey(), "abc,xyz").build();

        List<String> included = APMAgentSettings.TELEMETRY_TRACING_NAMES_INCLUDE_SETTING.get(settings);

        assertThat(included, containsInAnyOrder("abc", "xyz"));
        assertWarnings("[tracing.apm.names.include] setting was deprecated in Elasticsearch and will be removed in a future release.");
    }

    public void testTelemetryTracingNamesExcludeFallback() {
        Settings settings = Settings.builder().put(APMAgentSettings.TRACING_APM_NAMES_EXCLUDE_SETTING.getKey(), "abc,xyz").build();

        List<String> included = APMAgentSettings.TELEMETRY_TRACING_NAMES_EXCLUDE_SETTING.get(settings);

        assertThat(included, containsInAnyOrder("abc", "xyz"));
        assertWarnings("[tracing.apm.names.exclude] setting was deprecated in Elasticsearch and will be removed in a future release.");
    }

    public void testTelemetryTracingSanitizeFieldNamesFallback() {
        Settings settings = Settings.builder().put(APMAgentSettings.TRACING_APM_SANITIZE_FIELD_NAMES.getKey(), "abc,xyz").build();

        List<String> included = APMAgentSettings.TELEMETRY_TRACING_SANITIZE_FIELD_NAMES.get(settings);

        assertThat(included, containsInAnyOrder("abc", "xyz"));
        assertWarnings(
            "[tracing.apm.sanitize_field_names] setting was deprecated in Elasticsearch and will be removed in a future release."
        );
    }

    public void testTelemetryTracingSanitizeFieldNamesFallbackDefault() {
        List<String> included = APMAgentSettings.TELEMETRY_TRACING_SANITIZE_FIELD_NAMES.get(Settings.EMPTY);
        assertThat(included, hasItem("password")); // and more defaults
    }

    public void testTelemetrySecretTokenFallback() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(APMAgentSettings.TRACING_APM_SECRET_TOKEN_SETTING.getKey(), "verysecret");
        Settings settings = Settings.builder().setSecureSettings(secureSettings).build();

        try (SecureString secureString = APMAgentSettings.TELEMETRY_SECRET_TOKEN_SETTING.get(settings)) {
            assertEquals("verysecret", secureString.toString());

        }
        ;
        assertWarnings("[tracing.apm.secret_token] setting was deprecated in Elasticsearch and will be removed in a future release.");
    }

    public void testTelemetryApiKeyFallback() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(APMAgentSettings.TRACING_APM_API_KEY_SETTING.getKey(), "abc");
        Settings settings = Settings.builder().setSecureSettings(secureSettings).build();

        try (SecureString secureString = APMAgentSettings.TELEMETRY_API_KEY_SETTING.get(settings)) {
            assertEquals("abc", secureString.toString());

        }
        ;
        assertWarnings("[tracing.apm.api_key] setting was deprecated in Elasticsearch and will be removed in a future release.");
    }
}
