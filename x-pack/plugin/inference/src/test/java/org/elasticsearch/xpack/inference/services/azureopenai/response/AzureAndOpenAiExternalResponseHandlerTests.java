/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.response;

import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ContentTooLargeException;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.RequestTests;
import org.elasticsearch.xpack.inference.external.response.ErrorMessageResponseEntity;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AzureAndOpenAiExternalResponseHandlerTests extends ESTestCase {

    public void testCheckForFailureStatusCode() {
        var statusLine = mock(StatusLine.class);

        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);
        var header = mock(Header.class);
        when(header.getElements()).thenReturn(new HeaderElement[] {});
        when(httpResponse.getFirstHeader(anyString())).thenReturn(header);

        var mockRequest = RequestTests.mockRequest("id");
        var httpResult = new HttpResult(httpResponse, new byte[] {});
        var handler = new AzureMistralOpenAiExternalResponseHandler(
            "",
            (request, result) -> null,
            ErrorMessageResponseEntity::fromResponse,
            false
        );

        // 200 ok
        when(statusLine.getStatusCode()).thenReturn(200);
        handler.checkForFailureStatusCode(mockRequest, httpResult);
        // 503
        when(statusLine.getStatusCode()).thenReturn(503);
        var retryException = expectThrows(RetryException.class, () -> handler.checkForFailureStatusCode(mockRequest, httpResult));
        assertTrue(retryException.shouldRetry());
        assertThat(
            retryException.getCause().getMessage(),
            containsString("Received a server busy error status code for request from inference entity id [id] status [503]")
        );
        assertThat(((ElasticsearchStatusException) retryException.getCause()).status(), is(RestStatus.BAD_REQUEST));
        // 501
        when(statusLine.getStatusCode()).thenReturn(501);
        retryException = expectThrows(RetryException.class, () -> handler.checkForFailureStatusCode(mockRequest, httpResult));
        assertFalse(retryException.shouldRetry());
        assertThat(
            retryException.getCause().getMessage(),
            containsString("Received a server error status code for request from inference entity id [id] status [501]")
        );
        assertThat(((ElasticsearchStatusException) retryException.getCause()).status(), is(RestStatus.BAD_REQUEST));
        // 500
        when(statusLine.getStatusCode()).thenReturn(500);
        retryException = expectThrows(RetryException.class, () -> handler.checkForFailureStatusCode(mockRequest, httpResult));
        assertTrue(retryException.shouldRetry());
        assertThat(
            retryException.getCause().getMessage(),
            containsString("Received a server error status code for request from inference entity id [id] status [500]")
        );
        assertThat(((ElasticsearchStatusException) retryException.getCause()).status(), is(RestStatus.BAD_REQUEST));
        // 429
        when(statusLine.getStatusCode()).thenReturn(429);
        retryException = expectThrows(RetryException.class, () -> handler.checkForFailureStatusCode(mockRequest, httpResult));
        assertTrue(retryException.shouldRetry());
        assertThat(retryException.getCause().getMessage(), containsString("Received a rate limit status code."));
        assertThat(((ElasticsearchStatusException) retryException.getCause()).status(), is(RestStatus.TOO_MANY_REQUESTS));
        // 413
        when(statusLine.getStatusCode()).thenReturn(413);
        retryException = expectThrows(ContentTooLargeException.class, () -> handler.checkForFailureStatusCode(mockRequest, httpResult));
        assertTrue(retryException.shouldRetry());
        assertThat(retryException.getCause().getMessage(), containsString("Received a content too large status code"));
        assertThat(((ElasticsearchStatusException) retryException.getCause()).status(), is(RestStatus.REQUEST_ENTITY_TOO_LARGE));
        // 400 content too large
        retryException = expectThrows(
            ContentTooLargeException.class,
            () -> handler.checkForFailureStatusCode(mockRequest, createContentTooLargeResult(400))
        );
        assertTrue(retryException.shouldRetry());
        assertThat(retryException.getCause().getMessage(), containsString("Received a content too large status code"));
        assertThat(((ElasticsearchStatusException) retryException.getCause()).status(), is(RestStatus.BAD_REQUEST));
        // 400 generic bad request should not be marked as a content too large
        when(statusLine.getStatusCode()).thenReturn(400);
        retryException = expectThrows(RetryException.class, () -> handler.checkForFailureStatusCode(mockRequest, httpResult));
        assertFalse(retryException.shouldRetry());
        assertThat(
            retryException.getCause().getMessage(),
            containsString("Received an unsuccessful status code for request from inference entity id [id] status [400]")
        );
        assertThat(((ElasticsearchStatusException) retryException.getCause()).status(), is(RestStatus.BAD_REQUEST));
        // 400 is not flagged as a content too large when the error message is different
        when(statusLine.getStatusCode()).thenReturn(400);
        retryException = expectThrows(
            RetryException.class,
            () -> handler.checkForFailureStatusCode(mockRequest, createResult(400, "blah"))
        );
        assertFalse(retryException.shouldRetry());
        assertThat(
            retryException.getCause().getMessage(),
            containsString("Received an unsuccessful status code for request from inference entity id [id] status [400]")
        );
        assertThat(((ElasticsearchStatusException) retryException.getCause()).status(), is(RestStatus.BAD_REQUEST));
        // 401
        when(statusLine.getStatusCode()).thenReturn(401);
        retryException = expectThrows(RetryException.class, () -> handler.checkForFailureStatusCode(mockRequest, httpResult));
        assertFalse(retryException.shouldRetry());
        assertThat(
            retryException.getCause().getMessage(),
            containsString("Received an authentication error status code for request from inference entity id [id] status [401]")
        );
        assertThat(((ElasticsearchStatusException) retryException.getCause()).status(), is(RestStatus.UNAUTHORIZED));
        // 300
        when(statusLine.getStatusCode()).thenReturn(300);
        retryException = expectThrows(RetryException.class, () -> handler.checkForFailureStatusCode(mockRequest, httpResult));
        assertFalse(retryException.shouldRetry());
        assertThat(
            retryException.getCause().getMessage(),
            containsString("Unhandled redirection for request from inference entity id [id] status [300]")
        );
        assertThat(((ElasticsearchStatusException) retryException.getCause()).status(), is(RestStatus.MULTIPLE_CHOICES));
        // 402
        when(statusLine.getStatusCode()).thenReturn(402);
        retryException = expectThrows(RetryException.class, () -> handler.checkForFailureStatusCode(mockRequest, httpResult));
        assertFalse(retryException.shouldRetry());
        assertThat(
            retryException.getCause().getMessage(),
            containsString("Received an unsuccessful status code for request from inference entity id [id] status [402]")
        );
        assertThat(((ElasticsearchStatusException) retryException.getCause()).status(), is(RestStatus.PAYMENT_REQUIRED));
    }

    public void testBuildRateLimitErrorMessage() {
        int statusCode = 429;
        var statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(statusCode);
        var response = mock(HttpResponse.class);
        when(response.getStatusLine()).thenReturn(statusLine);
        var httpResult = new HttpResult(response, new byte[] {});

        {
            when(response.getFirstHeader(AzureMistralOpenAiExternalResponseHandler.REQUESTS_LIMIT)).thenReturn(
                new BasicHeader(AzureMistralOpenAiExternalResponseHandler.REQUESTS_LIMIT, "3000")
            );
            when(response.getFirstHeader(AzureMistralOpenAiExternalResponseHandler.REMAINING_REQUESTS)).thenReturn(
                new BasicHeader(AzureMistralOpenAiExternalResponseHandler.REMAINING_REQUESTS, "2999")
            );
            when(response.getFirstHeader(AzureMistralOpenAiExternalResponseHandler.TOKENS_LIMIT)).thenReturn(
                new BasicHeader(AzureMistralOpenAiExternalResponseHandler.TOKENS_LIMIT, "10000")
            );
            when(response.getFirstHeader(AzureMistralOpenAiExternalResponseHandler.REMAINING_TOKENS)).thenReturn(
                new BasicHeader(AzureMistralOpenAiExternalResponseHandler.REMAINING_TOKENS, "99800")
            );

            var error = AzureMistralOpenAiExternalResponseHandler.buildRateLimitErrorMessage(httpResult);
            assertThat(
                error,
                containsString("Token limit [10000], remaining tokens [99800]. Request limit [3000], remaining requests [2999]")
            );
        }

        {
            when(response.getFirstHeader(AzureMistralOpenAiExternalResponseHandler.TOKENS_LIMIT)).thenReturn(null);
            when(response.getFirstHeader(AzureMistralOpenAiExternalResponseHandler.REMAINING_TOKENS)).thenReturn(null);
            var error = AzureMistralOpenAiExternalResponseHandler.buildRateLimitErrorMessage(httpResult);
            assertThat(
                error,
                containsString("Token limit [unknown], remaining tokens [unknown]. Request limit [3000], remaining requests [2999]")
            );
        }

        {
            when(response.getFirstHeader(AzureMistralOpenAiExternalResponseHandler.REQUESTS_LIMIT)).thenReturn(null);
            when(response.getFirstHeader(AzureMistralOpenAiExternalResponseHandler.REMAINING_REQUESTS)).thenReturn(
                new BasicHeader(AzureMistralOpenAiExternalResponseHandler.REMAINING_REQUESTS, "2999")
            );
            when(response.getFirstHeader(AzureMistralOpenAiExternalResponseHandler.TOKENS_LIMIT)).thenReturn(null);
            when(response.getFirstHeader(AzureMistralOpenAiExternalResponseHandler.REMAINING_TOKENS)).thenReturn(null);
            var error = AzureMistralOpenAiExternalResponseHandler.buildRateLimitErrorMessage(httpResult);
            assertThat(error, containsString("Remaining tokens [unknown]. Remaining requests [2999]"));
        }

        {
            when(response.getFirstHeader(AzureMistralOpenAiExternalResponseHandler.REQUESTS_LIMIT)).thenReturn(null);
            when(response.getFirstHeader(AzureMistralOpenAiExternalResponseHandler.REMAINING_REQUESTS)).thenReturn(
                new BasicHeader(AzureMistralOpenAiExternalResponseHandler.REMAINING_REQUESTS, "2999")
            );
            when(response.getFirstHeader(AzureMistralOpenAiExternalResponseHandler.TOKENS_LIMIT)).thenReturn(
                new BasicHeader(AzureMistralOpenAiExternalResponseHandler.TOKENS_LIMIT, "10000")
            );
            when(response.getFirstHeader(AzureMistralOpenAiExternalResponseHandler.REMAINING_TOKENS)).thenReturn(null);
            var error = AzureMistralOpenAiExternalResponseHandler.buildRateLimitErrorMessage(httpResult);
            assertThat(
                error,
                containsString("Token limit [10000], remaining tokens [unknown]. Request limit [unknown], remaining requests [2999]")
            );
        }
    }

    private static HttpResult createContentTooLargeResult(int statusCode) {
        return createResult(
            statusCode,
            "This model's maximum context length is 8192 tokens, however you requested 13531 tokens (13531 in your prompt;"
                + "0 for the completion). Please reduce your prompt; or completion length."
        );
    }

    private static HttpResult createResult(int statusCode, String message) {
        var statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(statusCode);
        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);

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
