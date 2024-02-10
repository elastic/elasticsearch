/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.openai;

import org.apache.http.HttpResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.nio.charset.StandardCharsets;

import static org.mockito.Mockito.mock;

public class OpenAiErrorResponseEntityTests extends ESTestCase {
    public void testFromResponse() {
        String responseJson = """
            {
                "error": {
                    "message": "You didn't provide an API key",
                    "type": "invalid_request_error",
                    "param": null,
                    "code": null
                }
            }
            """;

        OpenAiErrorResponseEntity errorMessage = OpenAiErrorResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );
        assertEquals("You didn't provide an API key", errorMessage.getErrorMessage());
    }

    public void testFromResponse_noMessage() {
        String responseJson = """
            {
                "error": {
                    "type": "invalid_request_error"
                }
            }
            """;

        OpenAiErrorResponseEntity errorMessage = OpenAiErrorResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );
        assertNull(errorMessage);
    }

    public void testFromResponse_noError() {
        String responseJson = """
            {
                "something": {
                    "not": "relevant"
                }
            }
            """;

        OpenAiErrorResponseEntity errorMessage = OpenAiErrorResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );
        assertNull(errorMessage);
    }
}
