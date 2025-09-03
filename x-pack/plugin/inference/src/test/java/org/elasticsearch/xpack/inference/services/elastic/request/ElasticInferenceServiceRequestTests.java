/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.request;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.net.URI;

import static org.elasticsearch.xpack.inference.InferencePlugin.X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER;
import static org.hamcrest.Matchers.equalTo;

public class ElasticInferenceServiceRequestTests extends ESTestCase {

    public void testElasticInferenceServiceRequestSubclasses_Decorate_HttpRequest_WithProductOrigin() {
        var productOrigin = "elastic";
        var elasticInferenceServiceRequestWrapper = getDummyElasticInferenceServiceRequest(
            new ElasticInferenceServiceRequestMetadata(productOrigin, null)
        );
        var httpRequest = elasticInferenceServiceRequestWrapper.createHttpRequest();
        var productOriginHeader = httpRequest.httpRequestBase().getFirstHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER);

        // Make sure the product origin header only exists once
        assertThat(httpRequest.httpRequestBase().getHeaders(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER).length, equalTo(1));
        assertThat(productOriginHeader.getValue(), equalTo(productOrigin));
    }

    public void testElasticInferenceServiceRequestSubclasses_Decorate_HttpRequest_WithProductUseCase() {
        var productUseCase = "ai assistant";
        var elasticInferenceServiceRequestWrapper = getDummyElasticInferenceServiceRequest(
            new ElasticInferenceServiceRequestMetadata(null, productUseCase)
        );
        var httpRequest = elasticInferenceServiceRequestWrapper.createHttpRequest();
        var productUseCaseHeader = httpRequest.httpRequestBase().getFirstHeader(X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER);

        // Make sure the product use case header only exists once
        assertThat(httpRequest.httpRequestBase().getHeaders(X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER).length, equalTo(1));
        assertThat(productUseCaseHeader.getValue(), equalTo(productUseCase));
    }

    private static ElasticInferenceServiceRequest getDummyElasticInferenceServiceRequest(
        ElasticInferenceServiceRequestMetadata requestMetadata
    ) {
        return new ElasticInferenceServiceRequest(requestMetadata) {
            @Override
            protected HttpRequestBase createHttpRequestBase() {
                return new HttpGet("http://localhost:8080");
            }

            @Override
            public URI getURI() {
                return null;
            }

            @Override
            public Request truncate() {
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
        };
    }

    public static ElasticInferenceServiceRequestMetadata randomElasticInferenceServiceRequestMetadata() {
        return new ElasticInferenceServiceRequestMetadata(
            randomFrom(new String[] { null, randomAlphaOfLength(10) }),
            randomFrom(new String[] { null, randomAlphaOfLength(10) })
        );
    }
}
