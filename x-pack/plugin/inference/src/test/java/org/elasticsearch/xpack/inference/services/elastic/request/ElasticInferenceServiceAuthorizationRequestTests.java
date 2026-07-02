/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.request;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.regionpolicy.CspRegion;
import org.elasticsearch.xpack.core.inference.regionpolicy.RegionPolicy;
import org.elasticsearch.xpack.inference.common.InferencePreferences;
import org.elasticsearch.xpack.inference.external.request.RequestTests;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMAuthenticationApplierFactory;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;
import org.junit.Before;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import static org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceAuthorizationRequest.AUTHORIZATION_PATH;
import static org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceRequest.X_ELASTIC_INFERENCE_ALLOWED_REGIONS_HEADER;
import static org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceRequestTests.randomElasticInferenceServiceRequestMetadata;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class ElasticInferenceServiceAuthorizationRequestTests extends ESTestCase {

    private static final String CSP = "aws";
    private static final String REGION = "us-east-1";

    private TraceContext traceContext;

    @Before
    public void init() {
        traceContext = new TraceContext("dummyTraceParent", "dummyTraceState");
    }

    public void testCreateUriThrowsForInvalidBaseUrl() {
        String invalidUrl = "http://invalid-url^";

        ElasticsearchStatusException exception = assertThrows(
            ElasticsearchStatusException.class,
            () -> new ElasticInferenceServiceAuthorizationRequest(
                invalidUrl,
                traceContext,
                randomElasticInferenceServiceRequestMetadata(),
                null,
                CCMAuthenticationApplierFactory.NOOP_APPLIER
            )
        );

        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(exception.getMessage(), containsString("Failed to create URI for service"));
    }

    public void testCreateUri_CreatesUri() throws URISyntaxException {
        String url = "https://inference.cloud";

        var request = new ElasticInferenceServiceAuthorizationRequest(
            url,
            traceContext,
            randomElasticInferenceServiceRequestMetadata(),
            null,
            CCMAuthenticationApplierFactory.NOOP_APPLIER
        );
        assertThat(request.getURI(), is(new URI(url + AUTHORIZATION_PATH)));
    }

    public void testCreateHttpRequest_ForwardsRegionPolicyHeaders_WhenPreferencesProvided() {
        String url = "https://inference.cloud";
        var preferences = new InferencePreferences(new RegionPolicy(null, List.of(new CspRegion(CSP, REGION)), null));

        var request = new ElasticInferenceServiceAuthorizationRequest(
            url,
            traceContext,
            randomElasticInferenceServiceRequestMetadata(),
            preferences,
            CCMAuthenticationApplierFactory.NOOP_APPLIER
        );

        var httpRequest = RequestTests.getHttpRequestSync(request);

        assertThat(httpRequest.httpRequestBase().getHeaders(X_ELASTIC_INFERENCE_ALLOWED_REGIONS_HEADER).length, is(1));
        assertThat(
            httpRequest.httpRequestBase().getFirstHeader(X_ELASTIC_INFERENCE_ALLOWED_REGIONS_HEADER).getValue(),
            is(CSP + ":" + REGION)
        );
    }
}
