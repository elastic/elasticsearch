/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action.saml;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xpack.core.XPackSettings;
import org.hamcrest.Matchers;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;

public class SamlBaseRestHandlerTests extends ESTestCase {

    public void testSamlAvailableOnTrialAndPlatinum() {
        final SamlBaseRestHandler handler = buildHandler(randomFrom(
            License.OperationMode.TRIAL, License.OperationMode.PLATINUM, License.OperationMode.ENTERPRISE));
        assertThat(handler.checkFeatureAvailable(new FakeRestRequest()), Matchers.nullValue());
    }

    public void testSamlNotAvailableOnBasicStandardOrGold() {
        final SamlBaseRestHandler handler = buildHandler(randomFrom(License.OperationMode.BASIC, License.OperationMode.STANDARD,
            License.OperationMode.GOLD));
        Exception e = handler.checkFeatureAvailable(new FakeRestRequest());
        assertThat(e, instanceOf(ElasticsearchException.class));
        ElasticsearchException elasticsearchException = (ElasticsearchException) e;
        assertThat(elasticsearchException.getMetadata(LicenseUtils.EXPIRED_FEATURE_METADATA), contains("saml"));
    }

    private SamlBaseRestHandler buildHandler(License.OperationMode licenseMode) {
        final Settings settings = Settings.builder()
                .put(XPackSettings.SECURITY_ENABLED.getKey(), true)
                .build();
        final TestUtils.UpdatableLicenseState licenseState = new TestUtils.UpdatableLicenseState(settings);
        licenseState.update(licenseMode, true, Long.MAX_VALUE, null);

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
