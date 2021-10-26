/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.enrollment;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xpack.core.XPackSettings;
import org.hamcrest.Matchers;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;

public class EnrollmentBaseRestHandlerTests extends ESTestCase {

    public void testInEnrollmentMode() {
        final Settings settings = Settings.builder()
            .put(XPackSettings.ENROLLMENT_ENABLED.getKey(), true)
            .build();
        final EnrollmentBaseRestHandler handler = buildHandler(settings);
        assertThat(handler.checkFeatureAvailable(new FakeRestRequest()), Matchers.nullValue());
    }

    public void testNotInEnrollmentMode() {
        final Settings settings = Settings.builder()
            .put(XPackSettings.ENROLLMENT_ENABLED.getKey(), false)
            .build();
        final EnrollmentBaseRestHandler handler = buildHandler(settings);
        Exception ex = handler.checkFeatureAvailable(new FakeRestRequest());
        assertThat(ex, instanceOf(ElasticsearchSecurityException.class));
        assertThat(ex.getMessage(), Matchers.containsString("Enrollment mode is not enabled. Set [xpack.security.enrollment.enabled] " +
            "to true, in order to use this API."));
        assertThat(((ElasticsearchSecurityException)ex).status(), Matchers.equalTo(RestStatus.FORBIDDEN));
    }

    public void testSecurityExplicitlyDisabled() {
        final Settings settings = Settings.builder()
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
            .put(XPackSettings.ENROLLMENT_ENABLED.getKey(), true)
            .build();
        final EnrollmentBaseRestHandler handler = buildHandler(settings);
        Exception ex = handler.checkFeatureAvailable(new FakeRestRequest());
        assertThat(ex, instanceOf(IllegalStateException.class));
        assertThat(ex.getMessage(), Matchers.containsString("Security is not enabled but a security rest handler is registered"));
    }

    private EnrollmentBaseRestHandler buildHandler(Settings settings) {
        return new EnrollmentBaseRestHandler(settings, new XPackLicenseState(() -> 0)) {

            @Override
            public String getName() {
                return "enrollment_test";
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
