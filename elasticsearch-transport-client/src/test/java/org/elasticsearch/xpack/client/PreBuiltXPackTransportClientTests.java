/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.client;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.apache.lucene.util.Constants;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.security.Security;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for the {@link PreBuiltXPackTransportClient}
 */
public class PreBuiltXPackTransportClientTests extends RandomizedTest {

    @Test
    public void testPluginInstalled() {
        try (TransportClient client = new PreBuiltXPackTransportClient(Settings.EMPTY)) {
            Settings settings = client.settings();
            assertEquals(Security.NAME4, NetworkModule.TRANSPORT_TYPE_SETTING.get(settings));
        }
    }

    // dummy test so that the tests pass on JDK9 as the only test in this module is disabled on JDK9
    @Test
    public void testDummy() {

    }
}
