/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackSettings;

import static org.hamcrest.Matchers.is;

public class SecurityHttpSettingsTests extends ESTestCase {

    public void testDisablesCompressionByDefaultForSsl() {
        Settings settings = Settings.builder()
            .put(XPackSettings.HTTP_SSL_ENABLED.getKey(), true).build();

        Settings.Builder pluginSettingsBuilder = Settings.builder();
        SecurityHttpSettings.overrideSettings(pluginSettingsBuilder, settings);
        assertThat(HttpTransportSettings.SETTING_HTTP_COMPRESSION.get(pluginSettingsBuilder.build()), is(false));
    }

    public void testLeavesCompressionOnIfNotSsl() {
        Settings settings = Settings.builder()
            .put(XPackSettings.HTTP_SSL_ENABLED.getKey(), false).build();
        Settings.Builder pluginSettingsBuilder = Settings.builder();
        SecurityHttpSettings.overrideSettings(pluginSettingsBuilder, settings);
        assertThat(pluginSettingsBuilder.build().isEmpty(), is(true));
    }

    public void testDoesNotChangeExplicitlySetCompression() {
        Settings settings = Settings.builder()
            .put(XPackSettings.HTTP_SSL_ENABLED.getKey(), true)
            .put(HttpTransportSettings.SETTING_HTTP_COMPRESSION.getKey(), true)
            .build();

        Settings.Builder pluginSettingsBuilder = Settings.builder();
        SecurityHttpSettings.overrideSettings(pluginSettingsBuilder, settings);
        assertThat(pluginSettingsBuilder.build().isEmpty(), is(true));
    }
}
