/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic;

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

public class AnthropicChatCompletionResponseHandlerTests extends ESTestCase {
    private static final String INFERENCE_ID = "anthropic_inference_id";

    private final AnthropicChatCompletionResponseHandler responseHandler = new AnthropicChatCompletionResponseHandler("chat_completion");

    public void testFailValidation() throws IOException {
        var responseJson = """
            {
              "type": "error",
              "error": {
                "type": "not_found_error",
                "message": "The requested resource could not be found."
              },
              "request_id": "req_011CSHoEeqs5C35K2UUqR7Fy"
            }
            """;

        var errorJson = invalidResponseJson(responseJson);

        assertThat(errorJson, is(Strings.format("""
            {"error":{"code":"not_found","message":"Received an unsuccessful status code for request from inference entity id [anthropic_i\
            nference_id] status [404]. Error message: [{\\n  \\"type\\": \\"error\\",\\n  \\"error\\": {\\n    \\"type\\": \\"not_found_er\
            ror\\",\\n    \\"message\\": \\"The requested resource could not be found.\\"\\n  },\\n  \\"request_id\\": \\"req_011CSHoEeqs5\
            C35K2UUqR7Fy\\"\\n}\\n]","type":"anthropic_error"}}\
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
                new HttpResult(mockHttpResponse(404), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );
    }

}
