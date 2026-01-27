/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;
import org.hamcrest.Matchers;

import java.nio.charset.StandardCharsets;

import static org.mockito.Mockito.mock;

public class HuggingFaceErrorResponseEntityTests extends ESTestCase {
    public void testFromResponse() {
        String responseJson = """
            {
                "error": "A valid user token is required"
            }
            """;

        var errorMessage = HuggingFaceErrorResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );
        assertNotNull(errorMessage);
        assertEquals("A valid user token is required", errorMessage.getErrorMessage());
    }

    public void testFromResponse_noMessage() {
        String responseJson = """
            {
                "error": {
                    "type": "invalid_request_error"
                }
            }
            """;

        var errorMessage = HuggingFaceErrorResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );
        assertThat(errorMessage, Matchers.sameInstance(ErrorResponse.UNDEFINED_ERROR));
    }

    public void testFromResponse_noError() {
        String responseJson = """
            {
                "something": {
                    "not": "relevant"
                }
            }
            """;

        var errorMessage = HuggingFaceErrorResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );
        assertThat(errorMessage, Matchers.sameInstance(ErrorResponse.UNDEFINED_ERROR));
    }
}
