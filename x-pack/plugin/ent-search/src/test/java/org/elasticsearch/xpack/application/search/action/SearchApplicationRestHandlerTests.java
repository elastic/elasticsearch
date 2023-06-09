/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xpack.application.utils.LicenseUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class SearchApplicationRestHandlerTests extends ESTestCase {
    public void testLicenseEnforcement() throws Exception {
        MockLicenseState licenseState = MockLicenseState.createMock();
        final boolean licensedFeature = randomBoolean();

        when(licenseState.isAllowed(LicenseUtils.LICENSED_ENT_SEARCH_FEATURE)).thenReturn(licensedFeature);
        when(licenseState.isActive()).thenReturn(licensedFeature);

        final AtomicBoolean consumerCalled = new AtomicBoolean(false);
        SearchApplicationRestHandler handler = new SearchApplicationRestHandler(licenseState) {

            @Override
            protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
                return channel -> {
                    if (consumerCalled.compareAndSet(false, true) == false) {
                        fail("consumerCalled was not false");
                    }
                };
            }

            @Override
            public String getName() {
                return "test_search_application_base_action";
            }

            @Override
            public List<Route> routes() {
                return Collections.emptyList();
            }
        };

        FakeRestRequest fakeRestRequest = new FakeRestRequest();
        FakeRestChannel fakeRestChannel = new FakeRestChannel(fakeRestRequest, randomBoolean(), licensedFeature ? 0 : 1);

        try (NodeClient client = new NoOpNodeClient(this.getTestName())) {
            assertFalse(consumerCalled.get());
            verifyNoMoreInteractions(licenseState);
            handler.handleRequest(fakeRestRequest, fakeRestChannel, client);

            if (licensedFeature) {
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
