/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.oauth2;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.inference.Utils.mockClusterService;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.common.oauth2.OAuth2ClusterSettings.DEFAULT_CONNECT_TIMEOUT;
import static org.elasticsearch.xpack.inference.common.oauth2.OAuth2ClusterSettings.DEFAULT_READ_TIMEOUT;
import static org.hamcrest.Matchers.is;

public class OAuth2ClusterSettingsTests extends ESTestCase {

    private static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = Math.toIntExact(DEFAULT_CONNECT_TIMEOUT.getMillis());
    private static final int DEFAULT_READ_TIMEOUT_MILLIS = Math.toIntExact(DEFAULT_READ_TIMEOUT.getMillis());

    public void testDefaults() {
        var settings = new OAuth2ClusterSettings(Settings.EMPTY, mockClusterServiceEmpty());

        assertThat(settings.connectTimeoutMillis(), is(DEFAULT_CONNECT_TIMEOUT_MILLIS));
        assertThat(settings.readTimeoutMillis(), is(DEFAULT_READ_TIMEOUT_MILLIS));
    }

    public void testConnectTimeout_DynamicSettingUpdate() {
        var clusterService = mockClusterServiceEmpty();
        var settings = new OAuth2ClusterSettings(Settings.EMPTY, clusterService);

        assertThat(settings.connectTimeoutMillis(), is(DEFAULT_CONNECT_TIMEOUT_MILLIS));

        var newTimeout = TimeValue.timeValueSeconds(15);
        clusterService.getClusterSettings()
            .applySettings(Settings.builder().put(OAuth2ClusterSettings.CONNECT_TIMEOUT.getKey(), newTimeout).build());

        assertThat(settings.connectTimeoutMillis(), is(Math.toIntExact(newTimeout.getMillis())));
    }

    public void testReadTimeout_DynamicSettingUpdate() {
        var clusterService = mockClusterServiceEmpty();
        var settings = new OAuth2ClusterSettings(Settings.EMPTY, clusterService);

        assertThat(settings.readTimeoutMillis(), is(DEFAULT_READ_TIMEOUT_MILLIS));

        var newTimeout = TimeValue.timeValueSeconds(30);
        clusterService.getClusterSettings()
            .applySettings(Settings.builder().put(OAuth2ClusterSettings.READ_TIMEOUT.getKey(), newTimeout).build());

        assertThat(settings.readTimeoutMillis(), is(Math.toIntExact(newTimeout.getMillis())));
    }

    public void testCustomInitialValues() {
        var connectTimeout = TimeValue.timeValueSeconds(2);
        var readTimeout = TimeValue.timeValueSeconds(20);

        var initialSettings = Settings.builder()
            .put(OAuth2ClusterSettings.CONNECT_TIMEOUT.getKey(), connectTimeout)
            .put(OAuth2ClusterSettings.READ_TIMEOUT.getKey(), readTimeout)
            .build();

        var settings = new OAuth2ClusterSettings(initialSettings, mockClusterService(initialSettings));

        assertThat(settings.connectTimeoutMillis(), is(Math.toIntExact(connectTimeout.getMillis())));
        assertThat(settings.readTimeoutMillis(), is(Math.toIntExact(readTimeout.getMillis())));
    }

    public void testGetSettingsDefinitions_ContainsBothSettings() {
        var definitions = OAuth2ClusterSettings.getSettingsDefinitions();

        assertTrue(definitions.contains(OAuth2ClusterSettings.CONNECT_TIMEOUT));
        assertTrue(definitions.contains(OAuth2ClusterSettings.READ_TIMEOUT));
    }
}
