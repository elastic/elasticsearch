/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Strings;
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

public class GoogleVertexAiUnifiedChatCompletionResponseHandlerTests extends ESTestCase {
    private static final String INFERENCE_ID = "vertexAiInference";

    private final GoogleVertexAiUnifiedChatCompletionResponseHandler responseHandler =
        new GoogleVertexAiUnifiedChatCompletionResponseHandler(
            "chat_completion",
            (parser, xContentRegistry) -> mock() // Dummy parse function, not used in these error tests
        );

    public void testFailValidationWithAllErrorFields() throws IOException {
        var responseJson = """
            {
              "error": {
                "code": 400,
                "message": "Invalid JSON payload received.",
                "status": "INVALID_ARGUMENT"
              }
            }
            """;

        var errorJson = invalidResponseJson(responseJson);

        assertThat(errorJson, is(Strings.format("""
            {"error":{"code":"400","message":"Received a server error status code for request from inference entity id [%s] \
            status [500]. Error message: [Invalid JSON payload received.]","type":"INVALID_ARGUMENT"}}\
            """, INFERENCE_ID)));
    }

    public void testFailValidationWithAllErrorFieldsAndDetails() throws IOException {
        var responseJson = """
            {
              "error": {
                "code": 400,
                "message": "Invalid JSON payload received.",
                "status": "INVALID_ARGUMENT",
                "details": [
                    { "some":"value" }
                ]
              }
            }
            """;

        var errorJson = invalidResponseJson(responseJson);

        assertThat(errorJson, is(Strings.format("""
            {"error":{"code":"400","message":"Received a server error status code for request from inference entity id [%s] \
            status [500]. Error message: [Invalid JSON payload received.]","type":"INVALID_ARGUMENT"}}\
            """, INFERENCE_ID)));
    }

    private static Request mockRequest() {
        var request = mock(Request.class);
        when(request.getInferenceEntityId()).thenReturn(INFERENCE_ID);
        when(request.isStreaming()).thenReturn(true);
        return request;
    }

    private static HttpResponse mockHttpResponse(int statusCode) {
        var statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(statusCode);

        var response = mock(HttpResponse.class);
        when(response.getStatusLine()).thenReturn(statusLine);

        return response;
    }

    private String invalidResponseJson(String responseJson) throws IOException {
        var exception = invalidResponse(responseJson);
        assertThat(exception, isA(RetryException.class));
        assertThat(unwrapCause(exception), isA(UnifiedChatCompletionException.class));
        return toJson((UnifiedChatCompletionException) unwrapCause(exception));
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

    private Exception invalidResponse(String responseJson) {
        return expectThrows(
            RetryException.class,
            () -> responseHandler.validateResponse(
                mock(),
                mock(),
                mockRequest(),
                new HttpResult(mockHttpResponse(500), responseJson.getBytes(StandardCharsets.UTF_8)),
                true
            )
        );
    }

}
