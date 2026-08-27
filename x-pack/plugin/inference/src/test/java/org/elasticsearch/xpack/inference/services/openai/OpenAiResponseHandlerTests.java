/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.message.BasicHeader;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ContentTooLargeException;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.external.request.RequestTests;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OpenAiResponseHandlerTests extends ESTestCase {

    public void testBuildFailureStatusCodeException_ReturnsFor503_WithShouldRetryTrue() {
        var retryException = callHandleFailureStatusCode(503);
        assertTrue(retryException.shouldRetry());
        assertThat(
            retryException.getCause().getMessage(),
            containsString("Received a server busy error status code for request from inference entity id [id] status [503]")
        );
        assertThat(((ElasticsearchStatusException) retryException.getCause()).status(), is(RestStatus.BAD_REQUEST));
    }

    public void testBuildFailureStatusCodeException_ReturnsFor501() {
        var retryException = callHandleFailureStatusCode(501);
        assertFalse(retryException.shouldRetry());
        assertThat(
            retryException.getCause().getMessage(),
            containsString("Received a server error status code for request from inference entity id [id] status [501]")
        );
        assertThat(((ElasticsearchStatusException) retryException.getCause()).status(), is(RestStatus.BAD_REQUEST));
    }

    public void testBuildFailureStatusCodeException_ReturnsFor500_WithShouldRetryTrue() {
        var retryException = callHandleFailureStatusCode(500);
        assertTrue(retryException.shouldRetry());
        assertThat(
            retryException.getCause().getMessage(),
            containsString("Received a server error status code for request from inference entity id [id] status [500]")
        );
        assertThat(((ElasticsearchStatusException) retryException.getCause()).status(), is(RestStatus.BAD_REQUEST));
    }

    public void testBuildFailureStatusCodeException_ReturnsFor429_WithShouldRetryTrue() {
        var retryException = callHandleFailureStatusCode(429);
        assertTrue(retryException.shouldRetry());
        assertThat(retryException.getCause().getMessage(), containsString("Received a rate limit status code. Token limit"));
        assertThat(((ElasticsearchStatusException) retryException.getCause()).status(), is(RestStatus.TOO_MANY_REQUESTS));
    }

    public void testBuildFailureStatusCodeException_ReturnsFor413_ContentTooLarge() {
        var retryException = callHandleFailureStatusCode(413);
        assertThat(retryException, instanceOf(ContentTooLargeException.class));
        assertTrue(retryException.shouldRetry());
        assertThat(retryException.getCause().getMessage(), containsString("Received a content too large status code"));
        assertThat(((ElasticsearchStatusException) retryException.getCause()).status(), is(RestStatus.REQUEST_ENTITY_TOO_LARGE));
    }

    public void testBuildFailureStatusCodeException_ReturnsFor400_ContentTooLarge() {
        var retryException = callHandleFailureStatusCode(createContentTooLargeResult(400));
        assertThat(retryException, instanceOf(ContentTooLargeException.class));
        assertTrue(retryException.shouldRetry());
        assertThat(retryException.getCause().getMessage(), containsString("Received a content too large status code"));
        assertThat(((ElasticsearchStatusException) retryException.getCause()).status(), is(RestStatus.BAD_REQUEST));
    }

    public void testBuildFailureStatusCodeException_ReturnsFor400_GenericBadRequest() {
        var retryException = callHandleFailureStatusCode(400);
        assertFalse(retryException.shouldRetry());
        assertThat(
            retryException.getCause().getMessage(),
            containsString("Received a bad request status code for request from inference entity id [id] status [400]")
        );
        assertThat(((ElasticsearchStatusException) retryException.getCause()).status(), is(RestStatus.BAD_REQUEST));
    }

    public void testBuildFailureStatusCodeException_ReturnsFor400_NotContentTooLargeWithDifferentErrorMessage() {
        var retryException = callHandleFailureStatusCode(createResult(400, "blah"));
        assertFalse(retryException.shouldRetry());
        assertThat(
            retryException.getCause().getMessage(),
            containsString("Received a bad request status code for request from inference entity id [id] status [400]")
        );
        assertThat(((ElasticsearchStatusException) retryException.getCause()).status(), is(RestStatus.BAD_REQUEST));
    }

    public void testBuildFailureStatusCodeException_ReturnsFor422() {
        var retryException = callHandleFailureStatusCode(422);
        assertFalse(retryException.shouldRetry());
        assertThat(
            retryException.getCause().getMessage(),
            containsString("Received an input validation error response for request from inference entity id [id] status [422]")
        );
        assertThat(((ElasticsearchStatusException) retryException.getCause()).status(), is(RestStatus.UNPROCESSABLE_ENTITY));
    }

    public void testBuildFailureStatusCodeException_ReturnsFor401() {
        var retryException = callHandleFailureStatusCode(401);
        assertFalse(retryException.shouldRetry());
        assertThat(
            retryException.getCause().getMessage(),
            containsString("Received an authentication error status code for request from inference entity id [id] status [401]")
        );
        assertThat(((ElasticsearchStatusException) retryException.getCause()).status(), is(RestStatus.UNAUTHORIZED));
    }

    public void testBuildFailureStatusCodeException_ReturnsFor401_CallsOnAuthenticationFailure() {
        var failure = invokeHandlerExpectingFailure(401);
        assertFalse(failure.exception().shouldRetry());
        verify(failure.request()).onAuthenticationFailure();
    }

    public void testBuildFailureStatusCodeException_Non401_DoesNotCallOnAuthenticationFailure() {
        var failure = invokeHandlerExpectingFailure(500);
        verify(failure.request(), never()).onAuthenticationFailure();
    }

    public void testBuildFailureStatusCodeException_ReturnsFor300_Redirection() {
        var retryException = callHandleFailureStatusCode(300);
        assertFalse(retryException.shouldRetry());
        assertThat(
            retryException.getCause().getMessage(),
            containsString("Unhandled redirection for request from inference entity id [id] status [300]")
        );
        assertThat(((ElasticsearchStatusException) retryException.getCause()).status(), is(RestStatus.MULTIPLE_CHOICES));
    }

    public void testBuildFailureStatusCodeException_ReturnsFor402() {
        var retryException = callHandleFailureStatusCode(402);
        assertFalse(retryException.shouldRetry());
        assertThat(
            retryException.getCause().getMessage(),
            containsString("Received an unsuccessful status code for request from inference entity id [id] status [402]")
        );
        assertThat(((ElasticsearchStatusException) retryException.getCause()).status(), is(RestStatus.PAYMENT_REQUIRED));
    }

    public void testBuildRateLimitErrorMessage() {
        int statusCode = 429;
        var response = mock(HttpResponse.class);
        when(response.getCode()).thenReturn(statusCode);
        var httpResult = new HttpResult(response, new byte[] {});

        {
            when(response.getFirstHeader(OpenAiResponseHandler.REQUESTS_LIMIT)).thenReturn(
                new BasicHeader(OpenAiResponseHandler.REQUESTS_LIMIT, "3000")
            );
            when(response.getFirstHeader(OpenAiResponseHandler.REMAINING_REQUESTS)).thenReturn(
                new BasicHeader(OpenAiResponseHandler.REMAINING_REQUESTS, "2999")
            );
            when(response.getFirstHeader(OpenAiResponseHandler.TOKENS_LIMIT)).thenReturn(
                new BasicHeader(OpenAiResponseHandler.TOKENS_LIMIT, "10000")
            );
            when(response.getFirstHeader(OpenAiResponseHandler.REMAINING_TOKENS)).thenReturn(
                new BasicHeader(OpenAiResponseHandler.REMAINING_TOKENS, "99800")
            );

            var error = OpenAiResponseHandler.buildRateLimitErrorMessage(httpResult);
            assertThat(
                error,
                containsString("Token limit [10000], remaining tokens [99800]. Request limit [3000], remaining requests [2999]")
            );
        }

        {
            when(response.getFirstHeader(OpenAiResponseHandler.TOKENS_LIMIT)).thenReturn(null);
            when(response.getFirstHeader(OpenAiResponseHandler.REMAINING_TOKENS)).thenReturn(null);
            var error = OpenAiResponseHandler.buildRateLimitErrorMessage(httpResult);
            assertThat(
                error,
                containsString("Token limit [unknown], remaining tokens [unknown]. Request limit [3000], remaining requests [2999]")
            );
        }

        {
            when(response.getFirstHeader(OpenAiResponseHandler.REQUESTS_LIMIT)).thenReturn(null);
            when(response.getFirstHeader(OpenAiResponseHandler.REMAINING_REQUESTS)).thenReturn(
                new BasicHeader(OpenAiResponseHandler.REMAINING_REQUESTS, "2999")
            );
            when(response.getFirstHeader(OpenAiResponseHandler.TOKENS_LIMIT)).thenReturn(null);
            when(response.getFirstHeader(OpenAiResponseHandler.REMAINING_TOKENS)).thenReturn(null);
            var error = OpenAiResponseHandler.buildRateLimitErrorMessage(httpResult);
            assertThat(
                error,
                containsString("Token limit [unknown], remaining tokens [unknown]. Request limit [unknown], remaining requests [2999]")
            );
        }

        {
            when(response.getFirstHeader(OpenAiResponseHandler.REQUESTS_LIMIT)).thenReturn(null);
            when(response.getFirstHeader(OpenAiResponseHandler.REMAINING_REQUESTS)).thenReturn(
                new BasicHeader(OpenAiResponseHandler.REMAINING_REQUESTS, "2999")
            );
            when(response.getFirstHeader(OpenAiResponseHandler.TOKENS_LIMIT)).thenReturn(
                new BasicHeader(OpenAiResponseHandler.TOKENS_LIMIT, "10000")
            );
            when(response.getFirstHeader(OpenAiResponseHandler.REMAINING_TOKENS)).thenReturn(null);
            var error = OpenAiResponseHandler.buildRateLimitErrorMessage(httpResult);
            assertThat(
                error,
                containsString("Token limit [10000], remaining tokens [unknown]. Request limit [unknown], remaining requests [2999]")
            );
        }
    }

    private record FailureResult(OutboundRequest request, RetryException exception) {}

    private static RetryException callHandleFailureStatusCode(int statusCode) {
        var statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(statusCode);
        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);
        var header = mock(Header.class);
        when(header.getElements()).thenReturn(new HeaderElement[] {});
        when(httpResponse.getFirstHeader(anyString())).thenReturn(header);
        var mockRequest = RequestTests.mockRequest("id");
        var httpResult = new HttpResult(httpResponse, new byte[] {});
        var handler = new OpenAiResponseHandler("", (request, result) -> null, false);
        return handler.buildFailureStatusCodeException(mockRequest, httpResult);
    }

    private static RetryException callHandleFailureStatusCode(HttpResult httpResult) {
        var mockRequest = RequestTests.mockRequest("id");
        var handler = new OpenAiResponseHandler("", (request, result) -> null, false);
        return handler.buildFailureStatusCodeException(mockRequest, httpResult);
    }

    private static FailureResult invokeHandlerExpectingFailure(int statusCode) {
        var statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(statusCode);
        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);
        var header = mock(Header.class);
        when(header.getElements()).thenReturn(new HeaderElement[] {});
        when(httpResponse.getFirstHeader(anyString())).thenReturn(header);

        var mockRequest = RequestTests.mockRequest("id");
        var httpResult = new HttpResult(httpResponse, new byte[] {});
        var handler = new OpenAiResponseHandler("", (request, result) -> null, false);

        var exception = handler.buildFailureStatusCodeException(mockRequest, httpResult);
        return new FailureResult(mockRequest, exception);
    }

    private static HttpResult createContentTooLargeResult(int statusCode) {
        return createResult(
            statusCode,
            "This model's maximum context length is 8192 tokens, however you requested 13531 tokens (13531 in your prompt;"
                + "0 for the completion). Please reduce your prompt; or completion length."
        );
    }

    private static HttpResult createResult(int statusCode, String message) {
        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getCode()).thenReturn(statusCode);

        String responseJson = Strings.format("""
                {
                    "error": {
                        "message": "%s",
                        "type": "content_too_large",
                        "param": null,
                        "code": null
                    }
                }
            """, message);

        return new HttpResult(httpResponse, responseJson.getBytes(StandardCharsets.UTF_8));
    }
}
