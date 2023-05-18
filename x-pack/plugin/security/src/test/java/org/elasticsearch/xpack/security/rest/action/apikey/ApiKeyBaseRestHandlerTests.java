/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.apikey;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xpack.core.XPackSettings;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.mock;

public class ApiKeyBaseRestHandlerTests extends ESTestCase {

    public void testCheckFeatureAvailableChecksSettings() throws Exception {
        final boolean securityEnabled = randomBoolean();
        final boolean serviceEnabled = randomBoolean();
        final boolean requiredSettingsEnabled = securityEnabled && serviceEnabled;
        final var settings = Settings.builder()
            .put(XPackSettings.SECURITY_ENABLED.getKey(), securityEnabled)
            .put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), serviceEnabled)
            .build();
        final var consumerCalled = new AtomicBoolean(false);
        final var handler = new ApiKeyBaseRestHandler(settings, mock(XPackLicenseState.class)) {

            @Override
            public String getName() {
                return "test_xpack_security_api_key_base_action";
            }

            @Override
            public List<Route> routes() {
                return Collections.emptyList();
            }

            @Override
            protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) {
                return channel -> {
                    if (consumerCalled.compareAndSet(false, true) == false) {
                        fail("consumerCalled was not false");
                    }
                };
            }
        };
        final var fakeRestRequest = new FakeRestRequest();
        final var fakeRestChannel = new FakeRestChannel(fakeRestRequest, randomBoolean(), requiredSettingsEnabled ? 0 : 1);

        try (NodeClient client = new NoOpNodeClient(this.getTestName())) {
            assertFalse(consumerCalled.get());
            handler.handleRequest(fakeRestRequest, fakeRestChannel, client);

            if (requiredSettingsEnabled) {
                assertTrue(consumerCalled.get());
                assertEquals(0, fakeRestChannel.responses().get());
                assertEquals(0, fakeRestChannel.errors().get());
            } else {
                assertFalse(consumerCalled.get());
                assertEquals(0, fakeRestChannel.responses().get());
                assertEquals(1, fakeRestChannel.errors().get());
            }
        }
    }

}
