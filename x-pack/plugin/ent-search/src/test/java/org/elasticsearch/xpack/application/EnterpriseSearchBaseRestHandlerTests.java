/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xpack.application.utils.LicenseUtils;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class EnterpriseSearchBaseRestHandlerTests extends ESTestCase {
    public void testLicenseEnforcement() throws Exception {
        final boolean isLicensed = randomBoolean();
        MockLicenseState enterpriseLicenseState = mockLicenseState(LicenseUtils.ENTERPRISE_LICENSED_FEATURE, isLicensed);
        MockLicenseState platinumLicenseState = mockLicenseState(LicenseUtils.PLATINUM_LICENSED_FEATURE, isLicensed);

        testHandler(enterpriseLicenseState, isLicensed);
        testHandler(platinumLicenseState, isLicensed);
    }

    private void testHandler(MockLicenseState licenseState, boolean isLicensed) throws Exception {
        final LicenseUtils.Product product = LicenseUtils.Product.QUERY_RULES;

        final AtomicBoolean consumerCalled = new AtomicBoolean(false);
        EnterpriseSearchBaseRestHandler handler = new EnterpriseSearchBaseRestHandler(licenseState, product) {

            @Override
            protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) {
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
        FakeRestChannel fakeRestChannel = new FakeRestChannel(fakeRestRequest, randomBoolean(), isLicensed ? 0 : 1);

        try (var threadPool = createThreadPool()) {
            final var client = new NoOpNodeClient(threadPool);
            assertFalse(consumerCalled.get());
            verifyNoMoreInteractions(licenseState);
            handler.handleRequest(fakeRestRequest, fakeRestChannel, client);

            if (isLicensed) {
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

    private MockLicenseState mockLicenseState(LicensedFeature licensedFeature, boolean isLicensed) {
        MockLicenseState licenseState = MockLicenseState.createMock();

        when(licenseState.isAllowed(licensedFeature)).thenReturn(isLicensed);
        when(licenseState.isActive()).thenReturn(isLicensed);
        return licenseState;
    }
}
