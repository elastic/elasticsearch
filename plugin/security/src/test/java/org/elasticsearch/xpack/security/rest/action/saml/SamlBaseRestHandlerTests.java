/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action.saml;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.hamcrest.Matchers;

public class SamlBaseRestHandlerTests extends ESTestCase {

    public void testSamlAvailableOnTrialAndPlatinum() {
        final SamlBaseRestHandler handler = buildHandler(randomFrom(License.OperationMode.TRIAL, License.OperationMode.PLATINUM));
        assertThat(handler.checkLicensedFeature(new FakeRestRequest()), Matchers.nullValue());
    }

    public void testSecurityNotAvailableOnBasic() {
        final SamlBaseRestHandler handler = buildHandler(License.OperationMode.BASIC);
        assertThat(handler.checkLicensedFeature(new FakeRestRequest()), Matchers.equalTo("security"));
    }

    public void testSamlNotAvailableOnStandardOrGold() {
        final SamlBaseRestHandler handler = buildHandler(randomFrom(License.OperationMode.STANDARD, License.OperationMode.GOLD));
        assertThat(handler.checkLicensedFeature(new FakeRestRequest()), Matchers.equalTo("saml"));
    }

    private SamlBaseRestHandler buildHandler(License.OperationMode licenseMode) {
        final TestUtils.UpdatableLicenseState licenseState = new TestUtils.UpdatableLicenseState();
        licenseState.update(licenseMode, true);

        return new SamlBaseRestHandler(Settings.EMPTY, licenseState) {

            @Override
            public String getName() {
                return "saml_test";
            }

            @Override
            protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) {
                return null;
            }
        };
    }

}