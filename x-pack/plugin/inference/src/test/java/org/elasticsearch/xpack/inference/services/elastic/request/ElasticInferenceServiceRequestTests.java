/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.telemetry.InferenceProductContext;
import org.elasticsearch.inference.telemetry.InferenceProductContextTests;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.regionpolicy.CspRegion;
import org.elasticsearch.xpack.core.inference.regionpolicy.RegionPolicy;
import org.elasticsearch.xpack.inference.common.InferencePreferences;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.external.request.RequestTests;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMAuthenticationApplierFactory;

import java.net.URI;
import java.util.List;

import static org.elasticsearch.inference.telemetry.InferenceProductContext.X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER;
import static org.elasticsearch.xpack.inference.InferencePlugin.X_ELASTIC_ES_VERSION;
import static org.elasticsearch.xpack.inference.external.request.RequestUtils.apiKey;
import static org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceRequest.X_ELASTIC_INFERENCE_ALLOWED_GEOS_HEADER;
import static org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceRequest.X_ELASTIC_INFERENCE_ALLOWED_REGIONS_HEADER;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ElasticInferenceServiceRequestTests extends ESTestCase {

    private static final String CSP_AWS = "aws";
    private static final String REGION_US_EAST_1 = "us-east-1";
    private static final String CSP_GCP = "gcp";
    private static final String REGION_EUROPE_WEST_1 = "europe-west1";
    private static final String GEO_US = "us";
    private static final String GEO_EU = "eu";

    public void testElasticInferenceServiceRequestSubclasses_Decorate_HttpRequest_WithAuthorizationHeader() {
        var secret = "secret";
        var productOrigin = "elastic";
        var elasticInferenceServiceRequestWrapper = getDummyElasticInferenceServiceRequest(
            new ElasticInferenceServiceRequestMetadata(new InferenceProductContext(null, productOrigin), null),
            new CCMAuthenticationApplierFactory.AuthenticationHeaderApplier(new SecureString(secret.toCharArray()))
        );
        var httpRequest = RequestTests.getHttpRequestSync(elasticInferenceServiceRequestWrapper);

        assertThat(httpRequest.httpRequestBase().getHeaders(HttpHeaders.AUTHORIZATION).length, equalTo(1));
        assertThat(httpRequest.httpRequestBase().getFirstHeader(HttpHeaders.AUTHORIZATION).getValue(), is(apiKey(secret)));
    }

    public void testElasticInferenceServiceRequestSubclasses_Decorate_HttpRequest_WithProductOrigin() {
        var productOrigin = "elastic";
        var elasticInferenceServiceRequestWrapper = getDummyElasticInferenceServiceRequest(
            new ElasticInferenceServiceRequestMetadata(new InferenceProductContext(null, productOrigin), null)
        );
        var httpRequest = RequestTests.getHttpRequestSync(elasticInferenceServiceRequestWrapper);
        var productOriginHeader = httpRequest.httpRequestBase().getFirstHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER);

        // Make sure the product origin header only exists once
        assertThat(httpRequest.httpRequestBase().getHeaders(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER).length, equalTo(1));
        assertThat(productOriginHeader.getValue(), equalTo(productOrigin));
    }

    public void testElasticInferenceServiceRequestSubclasses_Decorate_HttpRequest_WithProductUseCase() {
        var productUseCase = "ai assistant";
        var elasticInferenceServiceRequestWrapper = getDummyElasticInferenceServiceRequest(
            new ElasticInferenceServiceRequestMetadata(new InferenceProductContext(productUseCase, null), null)
        );
        var httpRequest = RequestTests.getHttpRequestSync(elasticInferenceServiceRequestWrapper);
        var productUseCaseHeader = httpRequest.httpRequestBase().getFirstHeader(X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER);

        // Make sure the product use case header only exists once
        assertThat(httpRequest.httpRequestBase().getHeaders(X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER).length, equalTo(1));
        assertThat(productUseCaseHeader.getValue(), equalTo(productUseCase));
    }

    public void testElasticInferenceServiceRequestSubclasses_Decorate_HttpRequest_WithEsVersion() {
        var esVersion = "1.2.3";
        var elasticInferenceServiceRequestWrapper = getDummyElasticInferenceServiceRequest(
            new ElasticInferenceServiceRequestMetadata(InferenceProductContext.EMPTY, esVersion)
        );
        var httpRequest = RequestTests.getHttpRequestSync(elasticInferenceServiceRequestWrapper);
        var productUseCaseHeader = httpRequest.httpRequestBase().getFirstHeader(X_ELASTIC_ES_VERSION);

        // Make sure the product use case header only exists once
        assertThat(httpRequest.httpRequestBase().getHeaders(X_ELASTIC_ES_VERSION).length, equalTo(1));
        assertThat(productUseCaseHeader.getValue(), equalTo(esVersion));
    }

    public void testCreateHttpRequest_AddsAllowedRegionsHeader() {
        var regionPolicy = new RegionPolicy(
            null,
            List.of(new CspRegion(CSP_AWS, REGION_US_EAST_1), new CspRegion(CSP_GCP, REGION_EUROPE_WEST_1)),
            null
        );
        var request = getDummyElasticInferenceServiceRequest(
            randomElasticInferenceServiceRequestMetadata(),
            new InferencePreferences(regionPolicy)
        );
        var httpRequest = RequestTests.getHttpRequestSync(request);

        assertThat(httpRequest.httpRequestBase().getHeaders(X_ELASTIC_INFERENCE_ALLOWED_REGIONS_HEADER).length, equalTo(1));
        assertThat(
            httpRequest.httpRequestBase().getFirstHeader(X_ELASTIC_INFERENCE_ALLOWED_REGIONS_HEADER).getValue(),
            equalTo(CSP_AWS + ":" + REGION_US_EAST_1 + "," + CSP_GCP + ":" + REGION_EUROPE_WEST_1)
        );
        assertThat(httpRequest.httpRequestBase().getHeaders(X_ELASTIC_INFERENCE_ALLOWED_GEOS_HEADER).length, equalTo(0));
    }

    public void testCreateHttpRequest_AddsAllowedGeosHeader() {
        var regionPolicy = new RegionPolicy(List.of(GEO_US, GEO_EU), null, null);
        var request = getDummyElasticInferenceServiceRequest(
            randomElasticInferenceServiceRequestMetadata(),
            new InferencePreferences(regionPolicy)
        );
        var httpRequest = RequestTests.getHttpRequestSync(request);

        assertThat(httpRequest.httpRequestBase().getHeaders(X_ELASTIC_INFERENCE_ALLOWED_GEOS_HEADER).length, equalTo(1));
        assertThat(
            httpRequest.httpRequestBase().getFirstHeader(X_ELASTIC_INFERENCE_ALLOWED_GEOS_HEADER).getValue(),
            equalTo(GEO_US + "," + GEO_EU)
        );
        assertThat(httpRequest.httpRequestBase().getHeaders(X_ELASTIC_INFERENCE_ALLOWED_REGIONS_HEADER).length, equalTo(0));
    }

    public void testCreateHttpRequest_PrefersAllowedRegions_OverAllowedGeos() {
        var regionPolicy = new RegionPolicy(List.of(GEO_US), List.of(new CspRegion(CSP_AWS, REGION_US_EAST_1)), null);
        var request = getDummyElasticInferenceServiceRequest(
            randomElasticInferenceServiceRequestMetadata(),
            new InferencePreferences(regionPolicy)
        );
        var httpRequest = RequestTests.getHttpRequestSync(request);

        assertThat(httpRequest.httpRequestBase().getHeaders(X_ELASTIC_INFERENCE_ALLOWED_REGIONS_HEADER).length, equalTo(1));
        assertThat(
            httpRequest.httpRequestBase().getFirstHeader(X_ELASTIC_INFERENCE_ALLOWED_REGIONS_HEADER).getValue(),
            equalTo(CSP_AWS + ":" + REGION_US_EAST_1)
        );
        assertThat(httpRequest.httpRequestBase().getHeaders(X_ELASTIC_INFERENCE_ALLOWED_GEOS_HEADER).length, equalTo(0));
    }

    public void testCreateHttpRequest_AddsNoRegionHeaders_WhenPreferencesNull() {
        var request = getDummyElasticInferenceServiceRequest(
            randomElasticInferenceServiceRequestMetadata(),
            null,
            CCMAuthenticationApplierFactory.NOOP_APPLIER
        );
        var httpRequest = RequestTests.getHttpRequestSync(request);

        assertThat(httpRequest.httpRequestBase().getHeaders(X_ELASTIC_INFERENCE_ALLOWED_REGIONS_HEADER).length, equalTo(0));
        assertThat(httpRequest.httpRequestBase().getHeaders(X_ELASTIC_INFERENCE_ALLOWED_GEOS_HEADER).length, equalTo(0));
    }

    public void testCreateHttpRequest_AddsNoRegionHeaders_WhenRegionPolicyNull() {
        var request = getDummyElasticInferenceServiceRequest(randomElasticInferenceServiceRequestMetadata(), InferencePreferences.EMPTY);
        var httpRequest = RequestTests.getHttpRequestSync(request);

        assertThat(httpRequest.httpRequestBase().getHeaders(X_ELASTIC_INFERENCE_ALLOWED_REGIONS_HEADER).length, equalTo(0));
        assertThat(httpRequest.httpRequestBase().getHeaders(X_ELASTIC_INFERENCE_ALLOWED_GEOS_HEADER).length, equalTo(0));
    }

    public void testCreateHttpRequest_AddsNoRegionHeaders_WhenListsEmpty() {
        var regionPolicy = new RegionPolicy(List.of(), List.of(), null);
        var request = getDummyElasticInferenceServiceRequest(
            randomElasticInferenceServiceRequestMetadata(),
            new InferencePreferences(regionPolicy)
        );
        var httpRequest = RequestTests.getHttpRequestSync(request);

        assertThat(httpRequest.httpRequestBase().getHeaders(X_ELASTIC_INFERENCE_ALLOWED_REGIONS_HEADER).length, equalTo(0));
        assertThat(httpRequest.httpRequestBase().getHeaders(X_ELASTIC_INFERENCE_ALLOWED_GEOS_HEADER).length, equalTo(0));
    }

    private static ElasticInferenceServiceRequest getDummyElasticInferenceServiceRequest(
        ElasticInferenceServiceRequestMetadata requestMetadata
    ) {
        return getDummyElasticInferenceServiceRequest(requestMetadata, CCMAuthenticationApplierFactory.NOOP_APPLIER);
    }

    private static ElasticInferenceServiceRequest getDummyElasticInferenceServiceRequest(
        ElasticInferenceServiceRequestMetadata requestMetadata,
        CCMAuthenticationApplierFactory.AuthApplier authApplier
    ) {
        return getDummyElasticInferenceServiceRequest(requestMetadata, null, authApplier);
    }

    private static ElasticInferenceServiceRequest getDummyElasticInferenceServiceRequest(
        ElasticInferenceServiceRequestMetadata requestMetadata,
        @Nullable InferencePreferences preferences
    ) {
        return getDummyElasticInferenceServiceRequest(requestMetadata, preferences, CCMAuthenticationApplierFactory.NOOP_APPLIER);
    }

    private static ElasticInferenceServiceRequest getDummyElasticInferenceServiceRequest(
        ElasticInferenceServiceRequestMetadata requestMetadata,
        @Nullable InferencePreferences preferences,
        CCMAuthenticationApplierFactory.AuthApplier authApplier
    ) {
        return new ElasticInferenceServiceRequest(requestMetadata, preferences, authApplier) {
            @Override
            protected HttpRequestBase createHttpRequestBase() {
                return new HttpGet("http://localhost:8080");
            }

            @Override
            public URI getURI() {
                return null;
            }

            @Override
            public OutboundRequest truncate() {
                return null;
            }

            @Override
            public boolean[] getTruncationInfo() {
                return new boolean[0];
            }

            @Override
            public String getInferenceEntityId() {
                return "";
            }

            @Override
            public TaskType getTaskType() {
                return null;
            }
        };
    }

    public static ElasticInferenceServiceRequestMetadata randomElasticInferenceServiceRequestMetadata() {
        return new ElasticInferenceServiceRequestMetadata(
            InferenceProductContextTests.randomInferenceProductContext(),
            randomFrom(randomAlphaOfLength(10), null)
        );
    }
}
