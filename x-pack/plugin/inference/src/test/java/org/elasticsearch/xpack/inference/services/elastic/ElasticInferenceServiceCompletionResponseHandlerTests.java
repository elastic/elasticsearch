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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.CompletionResults;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.elasticsearch.ExceptionsHelper.unwrapCause;
import static org.elasticsearch.xpack.core.inference.results.CompletionResultsTests.buildExpectationCompletion;
import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceResponseHandler.RETRY_AFTER_HEADER;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The {@code completion} task type must keep the classic {@code _inference} contract: {@link CompletionResults} on success and a plain
 * {@link ElasticsearchStatusException} on failure. Before this handler existed, {@code completion} models shared the
 * {@code chat_completion} strategy and so returned the unified chat-completion shapes instead. These tests guard against regressing to
 * that.
 */
public class ElasticInferenceServiceCompletionResponseHandlerTests extends ESTestCase {

    private static final String INFERENCE_ID = "id";

    private final ElasticInferenceServiceCompletionResponseHandler responseHandler = new ElasticInferenceServiceCompletionResponseHandler();

    public void testRequestType_IsTheSharedCompletionDescription() {
        assertThat(responseHandler.getRequestType(), is("Elastic Inference Service completion"));
    }

    public void testCanHandleStreamingResponses() {
        assertTrue(responseHandler.canHandleStreamingResponses());
    }

    public void testParseResult_ReturnsCompletionResults() {
        var contents = "Hello there, how may I assist you today?";

        var responseJson = Strings.format("""
            {
              "id": "chatcmpl-123",
              "object": "chat.completion",
              "created": 1677652288,
              "model": "my-model-id",
              "choices": [
                {
                  "index": 0,
                  "message": {
                    "role": "assistant",
                    "content": "%s"
                  },
                  "finish_reason": "stop"
                }
              ],
              "usage": {
                "prompt_tokens": 9,
                "completion_tokens": 12,
                "total_tokens": 21
              }
            }
            """, contents);

        var results = responseHandler.parseResult(mockRequest(), httpResult(200, responseJson));

        assertThat(results, isA(CompletionResults.class));
        assertThat(results.asMap(), is(buildExpectationCompletion(List.of(contents))));
    }

    /**
     * The regression guard: {@code completion} failures must not surface as the unified chat-completion error shape.
     */
    public void testBuildFailureStatusCodeException_ReturnsElasticsearchStatusException_NotUnified() {
        var errorMessage = "some error";

        var responseJson = Strings.format("""
            {
              "error": "%s"
            }
            """, errorMessage);

        var exception = responseHandler.buildFailureStatusCodeException(mockRequest(), httpResult(400, responseJson));
        var cause = unwrapCause(exception);

        assertFalse(exception.shouldRetry());
        assertThat(cause, isA(ElasticsearchStatusException.class));
        assertThat(cause, is(not(isA(UnifiedChatCompletionException.class))));
        assertThat(((ElasticsearchStatusException) cause).status(), is(RestStatus.BAD_REQUEST));
        assertThat(
            cause.getMessage(),
            is(
                Strings.format(
                    "Received a bad request status code for request from inference entity id [id] status [400]. Error message: [%s]",
                    errorMessage
                )
            )
        );
    }

    public void testBuildFailureStatusCodeException_AppliesRetryAfterHeader() {
        var retryAfter = String.valueOf(randomIntBetween(1, 1000));
        var httpResult = httpResult(429, """
            {
              "error": "slow down"
            }
            """, new BasicHeader(RETRY_AFTER_HEADER, retryAfter));

        var exception = responseHandler.buildFailureStatusCodeException(mockRequest(), httpResult);
        var cause = unwrapCause(exception);

        assertTrue(exception.shouldRetry());
        assertThat(cause, isA(ElasticsearchException.class));
        assertThat(((ElasticsearchException) cause).getHttpHeader(RETRY_AFTER_HEADER), contains(retryAfter));
    }

    private static OutboundRequest mockRequest() {
        var outboundRequest = mock(OutboundRequest.class);
        when(outboundRequest.getInferenceEntityId()).thenReturn(INFERENCE_ID);
        return outboundRequest;
    }

    private static HttpResult httpResult(int statusCode, String body, BasicHeader... headers) {
        var statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(statusCode);

        // A real BasicHttpResponse rather than a mock so that getFirstHeader(...) behaves for the Retry-After assertions.
        var httpResponse = new BasicHttpResponse(statusLine);
        for (var header : headers) {
            httpResponse.addHeader(header);
        }

        return new HttpResult(httpResponse, body.getBytes(StandardCharsets.UTF_8));
    }
}
