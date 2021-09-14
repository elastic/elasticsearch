/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xpack.core.XPackSettings;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class SecurityBaseRestHandlerTests extends ESTestCase {

    public void testSecurityBaseRestHandlerChecksLicenseState() throws Exception {
        final boolean securityEnabled = randomBoolean();
        Settings settings = Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), securityEnabled).build();
        final AtomicBoolean consumerCalled = new AtomicBoolean(false);
        final XPackLicenseState licenseState = mock(XPackLicenseState.class);
        when(licenseState.getOperationMode()).thenReturn(
            randomFrom(License.OperationMode.BASIC, License.OperationMode.STANDARD, License.OperationMode.GOLD));
        SecurityBaseRestHandler handler = new SecurityBaseRestHandler(settings, licenseState) {

            @Override
            public String getName() {
                return "test_xpack_security_base_action";
            }

            @Override
            public List<Route> routes() {
                return Collections.emptyList();
            }

            @Override
            protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
                return channel -> {
                    if (consumerCalled.compareAndSet(false, true) == false) {
                        fail("consumerCalled was not false");
                    }
                };
            }
        };
        FakeRestRequest fakeRestRequest = new FakeRestRequest();
        FakeRestChannel fakeRestChannel = new FakeRestChannel(fakeRestRequest, randomBoolean(), securityEnabled ? 0 : 1);

        try (NodeClient client = new NoOpNodeClient(this.getTestName())) {
            assertFalse(consumerCalled.get());
            verifyZeroInteractions(licenseState);
            handler.handleRequest(fakeRestRequest, fakeRestChannel, client);

            if (securityEnabled) {
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
