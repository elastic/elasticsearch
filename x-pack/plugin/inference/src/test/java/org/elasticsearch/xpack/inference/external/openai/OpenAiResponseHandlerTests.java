/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.openai;

import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpResponse;
import org.apache.http.RequestLine;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OpenAiResponseHandlerTests extends ESTestCase {

    public void testCheckForFailureStatusCode() {
        var statusLine = mock(StatusLine.class);

        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);
        var header = mock(Header.class);
        when(header.getElements()).thenReturn(new HeaderElement[] {});
        when(httpResponse.getFirstHeader(anyString())).thenReturn(header);

        var httpRequest = mock(HttpRequestBase.class);
        var httpResult = new HttpResult(httpResponse, new byte[] {});
        var handler = new OpenAiResponseHandler("", result -> null);

        // 200 ok
        when(statusLine.getStatusCode()).thenReturn(200);
        handler.checkForFailureStatusCode(httpRequest, httpResult);
        // 503
        when(statusLine.getStatusCode()).thenReturn(503);
        var retryException = expectThrows(RetryException.class, () -> handler.checkForFailureStatusCode(httpRequest, httpResult));
        assertFalse(retryException.shouldRetry());
        assertThat(
            retryException.getCause().getMessage(),
            containsString("Received a server error status code for request [null] status [503]")
        );
        assertThat(((ElasticsearchStatusException) retryException.getCause()).status(), is(RestStatus.BAD_REQUEST));
        // 429
        when(statusLine.getStatusCode()).thenReturn(429);
        retryException = expectThrows(RetryException.class, () -> handler.checkForFailureStatusCode(httpRequest, httpResult));
        assertTrue(retryException.shouldRetry());
        assertThat(retryException.getCause().getMessage(), containsString("Received a rate limit status code. Token limit"));
        assertThat(((ElasticsearchStatusException) retryException.getCause()).status(), is(RestStatus.TOO_MANY_REQUESTS));
        // 401
        when(statusLine.getStatusCode()).thenReturn(401);
        retryException = expectThrows(RetryException.class, () -> handler.checkForFailureStatusCode(httpRequest, httpResult));
        assertFalse(retryException.shouldRetry());
        assertThat(
            retryException.getCause().getMessage(),
            containsString("Received an authentication error status code for request [null] status [401]")
        );
        assertThat(((ElasticsearchStatusException) retryException.getCause()).status(), is(RestStatus.UNAUTHORIZED));
        // 300
        when(statusLine.getStatusCode()).thenReturn(300);
        retryException = expectThrows(RetryException.class, () -> handler.checkForFailureStatusCode(httpRequest, httpResult));
        assertFalse(retryException.shouldRetry());
        assertThat(retryException.getCause().getMessage(), containsString("Unhandled redirection for request [null] status [300]"));
        assertThat(((ElasticsearchStatusException) retryException.getCause()).status(), is(RestStatus.MULTIPLE_CHOICES));
        // 402
        when(statusLine.getStatusCode()).thenReturn(402);
        retryException = expectThrows(RetryException.class, () -> handler.checkForFailureStatusCode(httpRequest, httpResult));
        assertFalse(retryException.shouldRetry());
        assertThat(
            retryException.getCause().getMessage(),
            containsString("Received an unsuccessful status code for request [null] status [402]")
        );
        assertThat(((ElasticsearchStatusException) retryException.getCause()).status(), is(RestStatus.PAYMENT_REQUIRED));
    }

    public void testBuildRateLimitErrorMessage() {
        int statusCode = 429;
        var statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(statusCode);
        var requestLine = mock(RequestLine.class);
        var response = mock(HttpResponse.class);
        when(response.getStatusLine()).thenReturn(statusLine);
        var request = mock(HttpRequestBase.class);
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

            var error = OpenAiResponseHandler.buildRateLimitErrorMessage(request, httpResult);
            assertThat(
                error,
                containsString("Token limit [10000], remaining tokens [99800]. Request limit [3000], remaining requests [2999]")
            );
        }

        {
            when(response.getFirstHeader(OpenAiResponseHandler.TOKENS_LIMIT)).thenReturn(null);
            when(response.getFirstHeader(OpenAiResponseHandler.REMAINING_TOKENS)).thenReturn(null);
            var error = OpenAiResponseHandler.buildRateLimitErrorMessage(request, httpResult);
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
            var error = OpenAiResponseHandler.buildRateLimitErrorMessage(request, httpResult);
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
            var error = OpenAiResponseHandler.buildRateLimitErrorMessage(request, httpResult);
            assertThat(
                error,
                containsString("Token limit [10000], remaining tokens [unknown]. Request limit [unknown], remaining requests [2999]")
            );
        }
    }
}
