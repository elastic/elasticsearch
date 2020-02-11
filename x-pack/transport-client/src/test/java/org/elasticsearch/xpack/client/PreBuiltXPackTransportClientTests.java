/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.client;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.elasticsearch.bootstrap.JavaVersion;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for the {@link PreBuiltXPackTransportClient}
 */
public class PreBuiltXPackTransportClientTests extends RandomizedTest {

    @Test
    public void testPluginInstalled() {
        Settings.Builder builder = Settings.builder();
        if (Boolean.parseBoolean(System.getProperty("tests.fips.enabled")) && JavaVersion.current().getVersion().get(0) == 8) {
            builder.put(XPackSettings.DIAGNOSE_TRUST_EXCEPTIONS_SETTING.getKey(), false);
        }
        try (TransportClient client = new PreBuiltXPackTransportClient(builder.build())) {
            Settings settings = client.settings();
            assertEquals(SecurityField.NAME4, NetworkModule.TRANSPORT_TYPE_SETTING.get(settings));
        }
    }

}
