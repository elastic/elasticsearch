/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.elasticsearch.ExceptionsHelper.unwrapCause;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OpenAiUnifiedChatCompletionResponseHandlerTests extends ESTestCase {
    private final OpenAiUnifiedChatCompletionResponseHandler responseHandler = new OpenAiUnifiedChatCompletionResponseHandler(
        "chat completions",
        (a, b) -> mock()
    );

    public void testFailValidationWithAllFields() throws IOException {
        var responseJson = """
            {
              "error": {
                "type": "not_found_error",
                "message": "a message",
                "code": "ahh",
                "param": "model"
              }
            }
            """;

        var errorJson = invalidResponseJson(responseJson);

        assertThat(errorJson, is("""
            {"error":{"code":"ahh","message":"Received a server error status code for request from inference entity id [abc] status [500]. \
            Error message: [a message]","param":"model","type":"not_found_error"}}"""));
    }

    public void testFailValidationWithoutOptionalFields() throws IOException {
        var responseJson = """
            {
              "error": {
                "type": "not_found_error",
                "message": "a message"
              }
            }
            """;

        var errorJson = invalidResponseJson(responseJson);

        assertThat(errorJson, is("""
            {"error":{"message":"Received a server error status code for request from inference entity id [abc] status [500]. \
            Error message: [a message]","type":"not_found_error"}}"""));
    }

    public void testFailValidationWithInvalidJson() throws IOException {
        var responseJson = """
            what? this isn't a json
            """;

        var errorJson = invalidResponseJson(responseJson);

        assertThat(errorJson, is("""
            {"error":{"code":"bad_request","message":"Received a server error status code for request from inference entity id [abc] status\
             [500]","type":"ErrorResponse"}}"""));
    }

    private String invalidResponseJson(String responseJson) throws IOException {
        var exception = invalidResponse(responseJson);
        assertThat(exception, isA(RetryException.class));
        assertThat(unwrapCause(exception), isA(UnifiedChatCompletionException.class));
        return toJson((UnifiedChatCompletionException) unwrapCause(exception));
    }

    private Exception invalidResponse(String responseJson) {
        return expectThrows(
            RetryException.class,
            () -> responseHandler.validateResponse(
                mock(),
                mock(),
                mockRequest(),
                new HttpResult(mock500Response(), responseJson.getBytes(StandardCharsets.UTF_8)),
                true
            )
        );
    }

    private static Request mockRequest() {
        var request = mock(Request.class);
        when(request.getInferenceEntityId()).thenReturn("abc");
        when(request.isStreaming()).thenReturn(true);
        return request;
    }

    private static HttpResponse mock500Response() {
        int statusCode = 500;
        var statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(statusCode);

        var response = mock(HttpResponse.class);
        when(response.getStatusLine()).thenReturn(statusLine);

        return response;
    }

    private String toJson(UnifiedChatCompletionException e) throws IOException {
        try (var builder = XContentFactory.jsonBuilder()) {
            e.toXContentChunked(EMPTY_PARAMS).forEachRemaining(xContent -> {
                try {
                    xContent.toXContent(builder, EMPTY_PARAMS);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            });
            return XContentHelper.convertToJson(BytesReference.bytes(builder), false, builder.contentType());
        }
    }
}
