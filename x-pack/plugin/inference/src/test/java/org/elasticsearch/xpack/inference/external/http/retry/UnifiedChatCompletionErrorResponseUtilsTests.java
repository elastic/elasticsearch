/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.apache.http.HttpResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class UnifiedChatCompletionErrorResponseUtilsTests extends ESTestCase {

    public void testCreateErrorParserWithStringify() {
        var parser = UnifiedChatCompletionErrorResponseUtils.createErrorParserWithStringify("test");
        var errorResponseJson = """
            {
                "error": "A valid user token is required"
            }
            """;
        var errorResponse = parser.parse(new HttpResult(mock(HttpResponse.class), errorResponseJson.getBytes(StandardCharsets.UTF_8)));
        assertNotNull(errorResponse);
        assertEquals(errorResponseJson, errorResponse.getErrorMessage());
    }

    public void testCreateErrorParserWithObjectParser() {
        var objectParser = UnifiedChatCompletionErrorResponse.ERROR_OBJECT_PARSER;

        var parser = UnifiedChatCompletionErrorResponseUtils.createErrorParserWithObjectParser(objectParser);
        var errorResponseJson = """
            {
                "error": {
                    "message": "A valid user token is required",
                    "type": "invalid_request_error",
                    "code": "code",
                    "param": "param"
                }
            }
            """;
        var errorResponse = parser.parse(new HttpResult(mock(HttpResponse.class), errorResponseJson.getBytes(StandardCharsets.UTF_8)));
        assertThat(errorResponse.getErrorMessage(), is("A valid user token is required"));
        assertThat(errorResponse.code(), is("code"));
        assertThat(errorResponse.param(), is("param"));
    }

    public void testCreateErrorParserWithGenericParser() {
        var parser = UnifiedChatCompletionErrorResponseUtils.createErrorParserWithGenericParser(
            UnifiedChatCompletionErrorResponseUtilsTests::doParse
        );
        var errorResponseJson = """
            {
                "error": {
                    "message": "A valid user token is required",
                    "type": "invalid_request_error",
                    "code": "code",
                    "param": "param"
                }
            }
            """;
        var errorResponse = parser.parse(errorResponseJson);
        assertThat(errorResponse.getErrorMessage(), is("A valid user token is required"));
        assertThat(errorResponse.code(), is("code"));
        assertThat(errorResponse.param(), is("param"));
    }

    private static Optional<UnifiedChatCompletionErrorResponse> doParse(XContentParser parser) throws IOException {
        var responseMap = parser.map();
        @SuppressWarnings("unchecked")
        var error = (Map<String, Object>) responseMap.get("error");
        if (error != null) {
            var message = (String) error.get("message");
            return Optional.of(
                new UnifiedChatCompletionErrorResponse(
                    Objects.requireNonNullElse(message, ""),
                    "test",
                    (String) error.get("code"),
                    (String) error.get("param")
                )
            );
        }

        return Optional.of(UnifiedChatCompletionErrorResponse.UNDEFINED_ERROR);
    }
}
