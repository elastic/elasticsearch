/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;

public class ErrorMessageResponseEntityTests extends ESTestCase {

    private static HttpResult getMockResult(String jsonString) {
        var response = mock(HttpResponse.class);
        return new HttpResult(response, Strings.toUTF8Bytes(jsonString));
    }

    public void testErrorResponse_ExtractsError() {
        var result = getMockResult("""
            {"error":{"message":"test_error_message"}}""");

        var error = ErrorMessageResponseEntity.fromResponse(result);
        assertNotNull(error);
        assertThat(error.getErrorMessage(), is("test_error_message"));
    }

    public void testFromResponse_WithOtherFieldsPresent() {
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

        ErrorResponse errorMessage = ErrorMessageResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );
        assertEquals("You didn't provide an API key", errorMessage.getErrorMessage());
    }

    public void testFromResponse_noMessage() {
        String responseJson = """
            {
              "error": {
                "type": "not_found_error"
              }
            }
            """;

        var errorMessage = ErrorMessageResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );
        assertThat(errorMessage.getErrorMessage(), is(""));
        assertTrue(errorMessage.errorStructureFound());
    }

    public void testErrorResponse_ReturnsUndefinedObjectIfNoError() {
        var result = getMockResult("""
            {"noerror":true}""");

        var error = ErrorMessageResponseEntity.fromResponse(result);
        assertThat(error, sameInstance(ErrorResponse.UNDEFINED_ERROR));
    }

    public void testErrorResponse_ReturnsUndefinedObjectIfNotJson() {
        var result = getMockResult("not a json string");

        var error = ErrorMessageResponseEntity.fromResponse(result);
        assertThat(error, sameInstance(ErrorResponse.UNDEFINED_ERROR));
    }
}
