/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.apache.http.StatusLine;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicHttpResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiUnifiedChatCompletionResponseEntity;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.elasticsearch.ExceptionsHelper.unwrapCause;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceResponseHandler.RETRY_AFTER_HEADER;
import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceUnifiedChatCompletionResponseHandler.CHAT_COMPLETIONS_REQUEST_DESCRIPTION;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The {@code chat_completion} task type reports failures as {@link UnifiedChatCompletionException} regardless of whether the caller
 * asked for a stream, so that the error shape is stable across the streaming and non-streaming variants of the unified API. The
 * non-streaming case is the one worth pinning: this handler used to fall back to a plain
 * {@link org.elasticsearch.ElasticsearchStatusException} when {@code isStreaming()} was false.
 */
public class ElasticInferenceServiceUnifiedChatCompletionResponseHandlerTests extends ESTestCase {

    private static final String INFERENCE_ID = "id";

    private final ElasticInferenceServiceUnifiedChatCompletionResponseHandler responseHandler =
        new ElasticInferenceServiceUnifiedChatCompletionResponseHandler(
            CHAT_COMPLETIONS_REQUEST_DESCRIPTION,
            OpenAiUnifiedChatCompletionResponseEntity::fromResponse
        );

    private static final String ERROR_MESSAGE = "some error";

    private static final String ERROR_RESPONSE_JSON = Strings.format("""
        {
          "error": "%s"
        }
        """, ERROR_MESSAGE);

    public void testFailValidation_Streaming() throws IOException {
        var errorJson = invalidResponseJson(ERROR_RESPONSE_JSON, 404, true);

        assertThat(errorJson, is(Strings.format(XContentHelper.stripWhitespace("""
            {
              "error": {
                "code": "not_found",
                "message": "Received an unsuccessful status code for request from inference entity id [id] status [404]. \
            Error message: [%s]",
                "type": "error"
              }
            }"""), ERROR_MESSAGE)));
    }

    /**
     * Previously this fell through to {@code super.buildError(...)} and produced an {@code ElasticsearchStatusException}.
     */
    public void testFailValidation_NonStreaming_StillReturnsUnifiedException() throws IOException {
        var errorJson = invalidResponseJson(ERROR_RESPONSE_JSON, 404, false);

        assertThat(errorJson, is(Strings.format(XContentHelper.stripWhitespace("""
            {
              "error": {
                "code": "not_found",
                "message": "Received an unsuccessful status code for request from inference entity id [id] status [404]. \
            Error message: [%s]",
                "type": "error"
              }
            }"""), ERROR_MESSAGE)));
    }

    /**
     * A 500 maps to {@link RestStatus#BAD_REQUEST} via {@code toRestStatus}, which is what drives the code field.
     */
    public void testFailValidation_ServerError() throws IOException {
        var errorJson = invalidResponseJson(ERROR_RESPONSE_JSON, 500, randomBoolean());

        assertThat(errorJson, is(Strings.format(XContentHelper.stripWhitespace("""
            {
              "error": {
                "code": "bad_request",
                "message": "Received a server error status code for request from inference entity id [id] status [500]. \
            Error message: [%s]",
                "type": "error"
              }
            }"""), ERROR_MESSAGE)));
    }

    public void testFailValidation_UnparseableBody_OmitsErrorMessage() throws IOException {
        var errorJson = invalidResponseJson("what? this isn't a json", 400, randomBoolean());

        assertThat(errorJson, is(XContentHelper.stripWhitespace("""
            {
              "error": {
                "code": "bad_request",
                "message": "Received a bad request status code for request from inference entity id [id] status [400]",
                "type": "error"
              }
            }""")));
    }

    public void testFailValidation_AppliesRetryAfterHeaderToUnifiedException() {
        var retryAfter = String.valueOf(randomIntBetween(1, 1000));
        var httpResult = httpResult(429, ERROR_RESPONSE_JSON, new BasicHeader(RETRY_AFTER_HEADER, retryAfter));

        var exception = expectThrows(
            RetryException.class,
            () -> responseHandler.validateResponse(mock(), mock(), mockRequest(randomBoolean()), httpResult)
        );
        var cause = unwrapCause(exception);

        assertThat(cause, isA(UnifiedChatCompletionException.class));
        assertThat(((UnifiedChatCompletionException) cause).getHttpHeader(RETRY_AFTER_HEADER), contains(retryAfter));
    }

    private String invalidResponseJson(String responseJson, int statusCode, boolean isStreaming) throws IOException {
        var exception = expectThrows(
            RetryException.class,
            () -> responseHandler.validateResponse(mock(), mock(), mockRequest(isStreaming), httpResult(statusCode, responseJson))
        );

        var cause = unwrapCause(exception);
        assertThat(cause, isA(UnifiedChatCompletionException.class));
        return toJson((UnifiedChatCompletionException) cause);
    }

    private static OutboundRequest mockRequest(boolean isStreaming) {
        var outboundRequest = mock(OutboundRequest.class);
        when(outboundRequest.getInferenceEntityId()).thenReturn(INFERENCE_ID);
        when(outboundRequest.isStreaming()).thenReturn(isStreaming);
        return outboundRequest;
    }

    private static HttpResult httpResult(int statusCode, String body, BasicHeader... headers) {
        var statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(statusCode);

        var httpResponse = new BasicHttpResponse(statusLine);
        for (var header : headers) {
            httpResponse.addHeader(header);
        }

        return new HttpResult(httpResponse, body.getBytes(StandardCharsets.UTF_8));
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
