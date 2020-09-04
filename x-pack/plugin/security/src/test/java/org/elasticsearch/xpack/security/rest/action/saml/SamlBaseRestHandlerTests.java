/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action.saml;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xpack.core.XPackSettings;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class SamlBaseRestHandlerTests extends ESTestCase {

    private static final long FIXED_MILLIS = 99L;
    private TestUtils.UpdatableLicenseState licenseState;

    public void testSamlAvailableOnTrialAndPlatinum() {
        final SamlBaseRestHandler handler = buildHandler(randomFrom(
            License.OperationMode.TRIAL, License.OperationMode.PLATINUM, License.OperationMode.ENTERPRISE));
        assertNoRecordedFeatureUsage();
        assertThat(handler.checkFeatureAvailable(new FakeRestRequest()), nullValue());
        assertFeatureUsageRecorded();
    }

    public void testSamlNotAvailableOnBasicStandardOrGold() {
        final SamlBaseRestHandler handler = buildHandler(randomFrom(License.OperationMode.BASIC, License.OperationMode.STANDARD,
            License.OperationMode.GOLD));
        assertNoRecordedFeatureUsage();
        Exception e = handler.checkFeatureAvailable(new FakeRestRequest());
        assertThat(e, instanceOf(ElasticsearchException.class));
        ElasticsearchException elasticsearchException = (ElasticsearchException) e;
        assertThat(elasticsearchException.getMetadata(LicenseUtils.EXPIRED_FEATURE_METADATA), contains("saml"));
        assertFeatureUsageRecorded();
    }

    protected void assertFeatureUsageRecorded() {
        assertThat(licenseState.getLastUsed().get(XPackLicenseState.Feature.SECURITY_SAML_REALM), is(FIXED_MILLIS));
    }

    protected void assertNoRecordedFeatureUsage() {
        assertThat(licenseState.getLastUsed().get(XPackLicenseState.Feature.SECURITY_SAML_REALM), nullValue());
    }

    private SamlBaseRestHandler buildHandler(License.OperationMode licenseMode) {
        final Settings settings = Settings.builder()
                .put(XPackSettings.SECURITY_ENABLED.getKey(), true)
                .build();
        licenseState = new TestUtils.UpdatableLicenseState(settings, () -> FIXED_MILLIS);
        licenseState.update(licenseMode, true, null);

        return new SamlBaseRestHandler(settings, licenseState) {

            @Override
            public String getName() {
                return "saml_test";
            }

            @Override
            public List<Route> routes() {
                return Collections.emptyList();
            }

            @Override
            protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) {
                return null;
            }
        };
    }

}
