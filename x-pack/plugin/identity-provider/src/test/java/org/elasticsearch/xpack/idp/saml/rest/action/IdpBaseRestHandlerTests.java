/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.idp.saml.rest.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class IdpBaseRestHandlerTests extends ESTestCase {

    public void testIdpAvailableOnTrialOrEnterprise() {
        final IdpBaseRestHandler handler = buildHandler(randomFrom(License.OperationMode.ENTERPRISE, License.OperationMode.TRIAL));
        assertThat(handler.isIdpFeatureAllowed(), equalTo(true));
    }

    public void testIdpNotAvailableOnOtherLicenses() {
        License.OperationMode mode =
            randomValueOtherThanMany(m -> m == License.OperationMode.ENTERPRISE || m == License.OperationMode.TRIAL,
                () -> randomFrom(License.OperationMode.values()));
        final IdpBaseRestHandler handler = buildHandler(mode);
        assertThat(handler.isIdpFeatureAllowed(), equalTo(false));
    }

    private IdpBaseRestHandler buildHandler(License.OperationMode licenseMode) {
        final Settings settings = Settings.builder()
            .put("xpack.idp.enabled", true)
            .build();
        final TestUtils.UpdatableLicenseState licenseState = new TestUtils.UpdatableLicenseState(settings);
        licenseState.update(licenseMode, true, null);
        return new IdpBaseRestHandler(licenseState) {
            @Override
            protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
                return null;
            }

            @Override
            public String getName() {
                return "idp-rest-test";
            }

            @Override
            public List<Route> routes() {
                return List.of();
            }
        };
    }
}
