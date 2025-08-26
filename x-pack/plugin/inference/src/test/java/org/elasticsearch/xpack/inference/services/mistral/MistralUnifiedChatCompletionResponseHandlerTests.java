/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral;

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
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

import static org.elasticsearch.ExceptionsHelper.unwrapCause;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MistralUnifiedChatCompletionResponseHandlerTests extends ESTestCase {
    private final MistralUnifiedChatCompletionResponseHandler responseHandler = new MistralUnifiedChatCompletionResponseHandler(
        "chat completions",
        (a, b) -> mock()
    );

    public void testFailNotFound() throws IOException {
        var responseJson = XContentHelper.stripWhitespace("""
            {
                "detail": "Not Found"
            }
            """);

        var errorJson = invalidResponseJson(responseJson, 404);

        assertThat(errorJson, is(XContentHelper.stripWhitespace("""
            {
              "error" : {
                "code" : "not_found",
                "message" : "Resource not found at [https://api.mistral.ai/v1/chat/completions] for request from inference entity id [id] \
            status [404]. Error message: [{\\"detail\\":\\"Not Found\\"}]",
                "type" : "mistral_error"
              }
            }""")));
    }

    public void testFailUnauthorized() throws IOException {
        var responseJson = XContentHelper.stripWhitespace("""
            {
                "message": "Unauthorized",
                "request_id": "a580d263fb1521778782b22104efb415"
            }
            """);

        var errorJson = invalidResponseJson(responseJson, 401);

        assertThat(errorJson, is(XContentHelper.stripWhitespace("""
            {
              "error" : {
                "code" : "unauthorized",
                "message" : "Received an authentication error status code for request from inference entity id [id] status [401]. Error \
            message: [{\\"message\\":\\"Unauthorized\\",\\"request_id\\":\\"a580d263fb1521778782b22104efb415\\"}]",
                "type" : "mistral_error"
              }
            }""")));
    }

    public void testFailBadRequest() throws IOException {
        var responseJson = XContentHelper.stripWhitespace("""
            {
                "object": "error",
                "message": "Invalid model: mistral-small-l2atest",
                "type": "invalid_model",
                "param": null,
                "code": "1500"
            }
            """);

        var errorJson = invalidResponseJson(responseJson, 400);

        assertThat(errorJson, is(XContentHelper.stripWhitespace("""
            {
              "error" : {
                "code" : "bad_request",
                "message" : "Received a bad request status code for request from inference entity id [id] status [400]. Error message: \
            [{\\"object\\":\\"error\\",\\"message\\":\\"Invalid model: mistral-small-l2atest\\",\\"type\\":\\"invalid_model\\",\\"par\
            am\\":null,\\"code\\":\\"1500\\"}]",
                "type" : "mistral_error"
              }
            }""")));
    }

    private String invalidResponseJson(String responseJson, int statusCode) throws IOException {
        var exception = invalidResponse(responseJson, statusCode);
        assertThat(exception, isA(RetryException.class));
        assertThat(unwrapCause(exception), isA(UnifiedChatCompletionException.class));
        return toJson((UnifiedChatCompletionException) unwrapCause(exception));
    }

    private Exception invalidResponse(String responseJson, int statusCode) {
        return expectThrows(
            RetryException.class,
            () -> responseHandler.validateResponse(
                mock(),
                mock(),
                mockRequest(),
                new HttpResult(mockErrorResponse(statusCode), responseJson.getBytes(StandardCharsets.UTF_8)),
                true
            )
        );
    }

    private static Request mockRequest() throws URISyntaxException {
        var request = mock(Request.class);
        when(request.getInferenceEntityId()).thenReturn("id");
        when(request.isStreaming()).thenReturn(true);
        when(request.getURI()).thenReturn(new URI("https://api.mistral.ai/v1/chat/completions"));
        return request;
    }

    private static HttpResponse mockErrorResponse(int statusCode) {
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
