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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.inference.InferenceRequestMetadata;
import org.elasticsearch.inference.TaskType;
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

import static org.elasticsearch.inference.InferenceRequestMetadata.Field.INTERACTION_ID;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.PRODUCT_FEATURE;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.PRODUCT_SOLUTION;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.PRODUCT_USE_CASE;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.SPACE_ID;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.TRACE_ID;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.USER_ID;
import static org.elasticsearch.xpack.inference.InferencePlugin.X_ELASTIC_ES_VERSION;
import static org.elasticsearch.xpack.inference.external.request.RequestUtils.apiKey;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ElasticInferenceServiceRequestTests extends ESTestCase {

    public void testElasticInferenceServiceRequestSubclasses_Decorate_HttpRequest_WithAuthorizationHeader() {
        var secret = "secret";
        var productOrigin = "elastic";
        var elasticInferenceServiceRequestWrapper = getDummyElasticInferenceServiceRequest(
            new ElasticInferenceServiceRequestMetadata(InferenceRequestMetadata.EMPTY, productOrigin, null),
            null,
            new CCMAuthenticationApplierFactory.AuthenticationHeaderApplier(new SecureString(secret.toCharArray()))
        );
        var httpRequest = RequestTests.getHttpRequestSync(elasticInferenceServiceRequestWrapper);

        assertThat(httpRequest.httpRequestBase().getHeaders(HttpHeaders.AUTHORIZATION).length, equalTo(1));
        assertThat(httpRequest.httpRequestBase().getFirstHeader(HttpHeaders.AUTHORIZATION).getValue(), is(apiKey(secret)));
    }

    public void testElasticInferenceServiceRequestSubclasses_Decorate_HttpRequest_WithProductOrigin() {
        var productOrigin = "elastic";
        var elasticInferenceServiceRequestWrapper = getDummyElasticInferenceServiceRequest(
            new ElasticInferenceServiceRequestMetadata(InferenceRequestMetadata.EMPTY, productOrigin, null)
        );
        var httpRequest = RequestTests.getHttpRequestSync(elasticInferenceServiceRequestWrapper);
        var productOriginHeader = httpRequest.httpRequestBase().getFirstHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER);

        // Make sure the product origin header only exists once
        assertThat(httpRequest.httpRequestBase().getHeaders(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER).length, equalTo(1));
        assertThat(productOriginHeader.getValue(), equalTo(productOrigin));
    }

    public void testElasticInferenceServiceRequestSubclasses_Decorate_HttpRequest_WithAttributionHeaders() {
        record Case(InferenceRequestMetadata.Field field, String expectedValue) {}

        for (var testCase : List.of(
            new Case(PRODUCT_USE_CASE, "ai assistant"),
            new Case(PRODUCT_SOLUTION, "security"),
            new Case(PRODUCT_FEATURE, "attack_discovery"),
            new Case(INTERACTION_ID, "interaction-id"),
            new Case(TRACE_ID, "trace-id"),
            new Case(USER_ID, "user-id"),
            new Case(SPACE_ID, "space-id")
        )) {
            var metadata = new ElasticInferenceServiceRequestMetadata(
                InferenceRequestMetadata.builder().put(testCase.field(), testCase.expectedValue()).build(),
                null,
                null
            );
            var httpRequest = RequestTests.getHttpRequestSync(getDummyElasticInferenceServiceRequest(metadata));
            var header = httpRequest.httpRequestBase().getFirstHeader(testCase.field().httpHeader());

            assertThat(httpRequest.httpRequestBase().getHeaders(testCase.field().httpHeader()).length, equalTo(1));
            assertThat(header.getValue(), equalTo(testCase.expectedValue()));
        }
    }

    public void testElasticInferenceServiceRequestSubclasses_Decorate_HttpRequest_WithoutOptionalAttributionHeaders() {
        var elasticInferenceServiceRequestWrapper = getDummyElasticInferenceServiceRequest(
            new ElasticInferenceServiceRequestMetadata(InferenceRequestMetadata.EMPTY, null, null)
        );
        var httpRequest = RequestTests.getHttpRequestSync(elasticInferenceServiceRequestWrapper);

        assertNull(httpRequest.httpRequestBase().getFirstHeader(PRODUCT_SOLUTION.httpHeader()));
        assertNull(httpRequest.httpRequestBase().getFirstHeader(PRODUCT_FEATURE.httpHeader()));
        assertNull(httpRequest.httpRequestBase().getFirstHeader(INTERACTION_ID.httpHeader()));
        assertNull(httpRequest.httpRequestBase().getFirstHeader(TRACE_ID.httpHeader()));
        assertNull(httpRequest.httpRequestBase().getFirstHeader(USER_ID.httpHeader()));
        assertNull(httpRequest.httpRequestBase().getFirstHeader(SPACE_ID.httpHeader()));
    }

    public void testElasticInferenceServiceRequestSubclasses_Decorate_HttpRequest_WithEsVersion() {
        var esVersion = "1.2.3";
        var elasticInferenceServiceRequestWrapper = getDummyElasticInferenceServiceRequest(
            new ElasticInferenceServiceRequestMetadata(InferenceRequestMetadata.EMPTY, null, esVersion)
        );
        var httpRequest = RequestTests.getHttpRequestSync(elasticInferenceServiceRequestWrapper);
        var productUseCaseHeader = httpRequest.httpRequestBase().getFirstHeader(X_ELASTIC_ES_VERSION);

        // Make sure the product use case header only exists once
        assertThat(httpRequest.httpRequestBase().getHeaders(X_ELASTIC_ES_VERSION).length, equalTo(1));
        assertThat(productUseCaseHeader.getValue(), equalTo(esVersion));
    }

    public void testElasticInferenceServiceRequestSubclasses_Decorate_HttpRequest_WithAllowedRegionsHeader() {
        var regionPolicy = new RegionPolicy(null, List.of(new CspRegion("aws", "eu-west-1"), new CspRegion("aws", "us-east-1")));
        var elasticInferenceServiceRequestWrapper = getDummyElasticInferenceServiceRequest(
            new ElasticInferenceServiceRequestMetadata(InferenceRequestMetadata.EMPTY, null, null),
            new InferencePreferences(regionPolicy),
            CCMAuthenticationApplierFactory.NOOP_APPLIER
        );
        var httpRequest = RequestTests.getHttpRequestSync(elasticInferenceServiceRequestWrapper);
        var header = httpRequest.httpRequestBase()
            .getFirstHeader(ElasticInferenceServiceRequest.X_ELASTIC_INFERENCE_ALLOWED_REGIONS_HEADER);

        assertThat(header.getValue(), equalTo("aws:eu-west-1,aws:us-east-1"));
        assertNull(httpRequest.httpRequestBase().getFirstHeader(ElasticInferenceServiceRequest.X_ELASTIC_INFERENCE_ALLOWED_GEOS_HEADER));
    }

    public void testElasticInferenceServiceRequestSubclasses_Decorate_HttpRequest_WithAllowedGeosHeader() {
        var regionPolicy = new RegionPolicy(List.of("eu", "us"), null);
        var elasticInferenceServiceRequestWrapper = getDummyElasticInferenceServiceRequest(
            new ElasticInferenceServiceRequestMetadata(InferenceRequestMetadata.EMPTY, null, null),
            new InferencePreferences(regionPolicy),
            CCMAuthenticationApplierFactory.NOOP_APPLIER
        );
        var httpRequest = RequestTests.getHttpRequestSync(elasticInferenceServiceRequestWrapper);
        var header = httpRequest.httpRequestBase().getFirstHeader(ElasticInferenceServiceRequest.X_ELASTIC_INFERENCE_ALLOWED_GEOS_HEADER);

        assertThat(header.getValue(), equalTo("eu,us"));
        assertNull(httpRequest.httpRequestBase().getFirstHeader(ElasticInferenceServiceRequest.X_ELASTIC_INFERENCE_ALLOWED_REGIONS_HEADER));
    }

    public void testElasticInferenceServiceRequestSubclasses_Decorate_HttpRequest_WithoutRegionPolicy_NoHeaders() {
        var elasticInferenceServiceRequestWrapper = getDummyElasticInferenceServiceRequest(
            new ElasticInferenceServiceRequestMetadata(InferenceRequestMetadata.EMPTY, null, null),
            InferencePreferences.EMPTY,
            CCMAuthenticationApplierFactory.NOOP_APPLIER
        );
        var httpRequest = RequestTests.getHttpRequestSync(elasticInferenceServiceRequestWrapper);

        assertNull(httpRequest.httpRequestBase().getFirstHeader(ElasticInferenceServiceRequest.X_ELASTIC_INFERENCE_ALLOWED_REGIONS_HEADER));
        assertNull(httpRequest.httpRequestBase().getFirstHeader(ElasticInferenceServiceRequest.X_ELASTIC_INFERENCE_ALLOWED_GEOS_HEADER));
    }

    private static ElasticInferenceServiceRequest getDummyElasticInferenceServiceRequest(
        ElasticInferenceServiceRequestMetadata requestMetadata
    ) {
        return getDummyElasticInferenceServiceRequest(requestMetadata, null, CCMAuthenticationApplierFactory.NOOP_APPLIER);
    }

    private static ElasticInferenceServiceRequest getDummyElasticInferenceServiceRequest(
        ElasticInferenceServiceRequestMetadata requestMetadata,
        InferencePreferences preferences,
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
            InferenceRequestMetadata.builder().put(PRODUCT_USE_CASE, randomAlphaOfLength(10)).build(),
            randomFrom(randomAlphaOfLength(10), null),
            randomFrom(randomAlphaOfLength(10), null)
        );
    }

    public void testExtractRequestMetadataFromThreadContext() {
        var threadContext = new ThreadContext(Settings.EMPTY);
        var productUseCase = randomAlphaOfLength(10);
        var productOrigin = randomAlphaOfLength(10);
        var productSolution = randomAlphaOfLength(10);
        var productFeature = randomAlphaOfLength(10);
        var interactionId = randomAlphaOfLength(10);
        var traceId = randomAlphaOfLength(10);
        var userId = randomAlphaOfLength(10);
        var spaceId = randomAlphaOfLength(10);
        threadContext.putHeader(PRODUCT_USE_CASE.httpHeader(), productUseCase);
        threadContext.putHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER, productOrigin);
        threadContext.putHeader(PRODUCT_SOLUTION.httpHeader(), productSolution);
        threadContext.putHeader(PRODUCT_FEATURE.httpHeader(), productFeature);
        threadContext.putHeader(INTERACTION_ID.httpHeader(), interactionId);
        threadContext.putHeader(TRACE_ID.httpHeader(), traceId);
        threadContext.putHeader(USER_ID.httpHeader(), userId);
        threadContext.putHeader(SPACE_ID.httpHeader(), spaceId);

        var metadata = ElasticInferenceServiceRequest.extractRequestMetadataFromThreadContext(threadContext);

        assertThat(
            metadata.context(),
            equalTo(
                InferenceRequestMetadata.builder()
                    .put(PRODUCT_USE_CASE, productUseCase)
                    .put(PRODUCT_SOLUTION, productSolution)
                    .put(PRODUCT_FEATURE, productFeature)
                    .put(INTERACTION_ID, interactionId)
                    .put(TRACE_ID, traceId)
                    .put(USER_ID, userId)
                    .put(SPACE_ID, spaceId)
                    .build()
            )
        );
        assertThat(metadata.productOrigin(), equalTo(productOrigin));
        assertNotNull(metadata.esVersion());
    }

    public void testExtractRequestMetadataFromThreadContextWithoutOptionalHeaders() {
        var metadata = ElasticInferenceServiceRequest.extractRequestMetadataFromThreadContext(new ThreadContext(Settings.EMPTY));

        assertThat(metadata.context(), equalTo(InferenceRequestMetadata.EMPTY));
        assertThat(metadata.productOrigin(), equalTo(null));
    }

    public void testExtractRequestMetadataSnapshotsThreadContext() {
        var threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader(PRODUCT_USE_CASE.httpHeader(), "captured");

        var metadata = ElasticInferenceServiceRequest.extractRequestMetadataFromThreadContext(threadContext);
        threadContext.putHeader(PRODUCT_SOLUTION.httpHeader(), "added-after-capture");

        assertThat(metadata.context().get(PRODUCT_USE_CASE), equalTo("captured"));
        assertThat(metadata.context().get(PRODUCT_SOLUTION), equalTo(null));
    }
}
