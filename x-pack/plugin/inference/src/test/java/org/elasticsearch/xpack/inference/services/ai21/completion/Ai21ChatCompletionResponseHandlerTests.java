/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ai21.completion;

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

public class Ai21ChatCompletionResponseHandlerTests extends ESTestCase {
    private final Ai21ChatCompletionResponseHandler responseHandler = new Ai21ChatCompletionResponseHandler(
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
                "message" : "Resource not found at [https://api.ai21.com/studio/v1/chat/completions] for request from inference entity id \
            [id] status [404]. Error message: [{\\"detail\\":\\"Not Found\\"}]",
                "type" : "ai21_error"
              }
            }""")));
    }

    public void testFailUnauthorized() throws IOException {
        var responseJson = XContentHelper.stripWhitespace("""
            {
                "detail": "Forbidden: Bad or missing Apikey/JWT."
            }
            """);

        var errorJson = invalidResponseJson(responseJson, 401);

        assertThat(errorJson, is(XContentHelper.stripWhitespace("""
            {
              "error" : {
                "code" : "unauthorized",
                "message" : "Received an authentication error status code for request from inference entity id [id] status [401]. Error \
            message: [{\\"detail\\":\\"Forbidden: Bad or missing Apikey/JWT.\\"}]",
                "type" : "ai21_error"
              }
            }""")));
    }

    public void testFailUnprocessableEntity() throws IOException {
        var responseJson = XContentHelper.stripWhitespace("""
            {
                "detail": "The provided model is not supported. See https://docs.ai21.com/docs/jamba-foundation-models#api-versioning \
            for a list of supported models"
            }
            """);

        var errorJson = invalidResponseJson(responseJson, 422);

        assertThat(errorJson, is(XContentHelper.stripWhitespace("""
            {
              "error" : {
                "code" : "unprocessable_entity",
                "message" : "Received an input validation error response for request from inference entity id [id] status [422]. \
            Error message: [{\\"detail\\":\\"The provided model is not supported. \
            See https://docs.ai21.com/docs/jamba-foundation-models#api-versioning for a list of supported models\\"}]",
                "type" : "ai21_error"
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
                new HttpResult(mockErrorResponse(statusCode), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );
    }

    private static Request mockRequest() throws URISyntaxException {
        var request = mock(Request.class);
        when(request.getInferenceEntityId()).thenReturn("id");
        when(request.isStreaming()).thenReturn(true);
        when(request.getURI()).thenReturn(new URI("https://api.ai21.com/studio/v1/chat/completions"));
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
