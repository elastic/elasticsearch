/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.anthropic;

import org.apache.http.HttpResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.nio.charset.StandardCharsets;

import static org.mockito.Mockito.mock;

public class AnthropicErrorResponseEntityTests extends ESTestCase {
    public void testFromResponse() {
        String responseJson = """
            {
              "type": "error",
              "error": {
                "type": "not_found_error",
                "message": "The requested resource could not be found."
              }
            }
            """;

        var errorMessage = AnthropicErrorResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );
        assertNotNull(errorMessage);
        assertEquals("The requested resource could not be found.", errorMessage.getErrorMessage());
    }

    public void testFromResponse_noMessage() {
        String responseJson = """
            {
              "type": "error",
              "error": {
                "type": "not_found_error",
              }
            }
            """;

        var errorMessage = AnthropicErrorResponseEntity.fromResponse(
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

        var errorMessage = AnthropicErrorResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );
        assertNull(errorMessage);
    }
}
