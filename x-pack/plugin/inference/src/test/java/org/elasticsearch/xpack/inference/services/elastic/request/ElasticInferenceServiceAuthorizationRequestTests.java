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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ElasticInferenceServiceAuthorizationRequestTests extends ESTestCase {

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
                CCMAuthenticationApplierFactory.NOOP_APPLIER,
                null
            )
        );

        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(exception.getMessage(), containsString("Failed to create URI for service"));
    }

    public void testCreateUri_CreatesUri() throws URISyntaxException {
        String url = "https://inference.us-east-1.aws.svc.elastic.cloud";

        var request = new ElasticInferenceServiceAuthorizationRequest(
            url,
            traceContext,
            randomElasticInferenceServiceRequestMetadata(),
            CCMAuthenticationApplierFactory.NOOP_APPLIER,
            null
        );
        assertThat(request.getURI(), is(new URI(url + AUTHORIZATION_PATH)));
    }

    public void testCreateHttpRequest_IncludesRegionPolicyHeaders_WhenPreferencesProvided() {
        String url = "https://inference.us-east-1.aws.svc.elastic.cloud";
        var regionPolicy = new RegionPolicy(null, List.of(new CspRegion("aws", "eu-west-1")));

        var request = new ElasticInferenceServiceAuthorizationRequest(
            url,
            traceContext,
            randomElasticInferenceServiceRequestMetadata(),
            CCMAuthenticationApplierFactory.NOOP_APPLIER,
            new InferencePreferences(regionPolicy)
        );

        var httpRequest = RequestTests.getHttpRequestSync(request);
        var header = httpRequest.httpRequestBase().getFirstHeader(X_ELASTIC_INFERENCE_ALLOWED_REGIONS_HEADER);

        assertThat(header.getValue(), equalTo("aws:eu-west-1"));
    }
}
