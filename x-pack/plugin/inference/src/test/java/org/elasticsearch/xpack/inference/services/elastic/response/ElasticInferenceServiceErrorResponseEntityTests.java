/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;
import org.hamcrest.Matchers;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class ElasticInferenceServiceErrorResponseEntityTests extends ESTestCase {

    public void testFromResponse() {
        String responseJson = """
            {
                "error": "error"
            }
            """;

        var errorResponseEntity = ElasticInferenceServiceErrorResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertNotNull(errorResponseEntity);
        assertThat(errorResponseEntity.getErrorMessage(), is("error"));
    }

    public void testFromResponse_NoErrorMessagePresent() {
        String responseJson = """
            {
                "not_error": "error"
            }
            """;

        var errorResponseEntity = ElasticInferenceServiceErrorResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(errorResponseEntity, Matchers.sameInstance(ErrorResponse.UNDEFINED_ERROR));
    }

    public void testFromResponse_InvalidJson() {
        String invalidResponseJson = """
            {
            """;

        var errorResponseEntity = ElasticInferenceServiceErrorResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), invalidResponseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(errorResponseEntity, Matchers.sameInstance(ErrorResponse.UNDEFINED_ERROR));
    }
}
