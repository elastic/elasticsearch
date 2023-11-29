/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.huggingface;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpRequestBase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HuggingFaceResponseHandlerTests extends ESTestCase {

    public void testCheckForFailureStatusCode() {
        var statusLine = mock(StatusLine.class);

        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);

        var httpRequest = mock(HttpRequestBase.class);

        var httpResult = new HttpResult(httpResponse, new byte[] {});

        var handler = new HuggingFaceResponseHandler("", result -> null);

        // 200 ok
        when(statusLine.getStatusCode()).thenReturn(200);
        handler.checkForFailureStatusCode(httpRequest, httpResult);
        // 503
        when(statusLine.getStatusCode()).thenReturn(503);
        var retryException = expectThrows(RetryException.class, () -> handler.checkForFailureStatusCode(httpRequest, httpResult));
        assertTrue(retryException.shouldRetry());
        assertThat(
            retryException.getCause().getMessage(),
            containsString("Received a rate limit status code for request [null] status [503]")
        );
        // 502
        when(statusLine.getStatusCode()).thenReturn(502);
        retryException = expectThrows(RetryException.class, () -> handler.checkForFailureStatusCode(httpRequest, httpResult));
        assertTrue(retryException.shouldRetry());
        assertThat(
            retryException.getCause().getMessage(),
            containsString("Received a rate limit status code for request [null] status [502]")
        );
        // 429
        when(statusLine.getStatusCode()).thenReturn(429);
        retryException = expectThrows(RetryException.class, () -> handler.checkForFailureStatusCode(httpRequest, httpResult));
        assertTrue(retryException.shouldRetry());
        assertThat(
            retryException.getCause().getMessage(),
            containsString("Received a rate limit status code for request [null] status [429]")
        );
        // 401
        when(statusLine.getStatusCode()).thenReturn(401);
        retryException = expectThrows(RetryException.class, () -> handler.checkForFailureStatusCode(httpRequest, httpResult));
        assertFalse(retryException.shouldRetry());
        assertThat(
            retryException.getCause().getMessage(),
            containsString("Received an authentication error status code for request [null] status [401]")
        );
        // 300
        when(statusLine.getStatusCode()).thenReturn(300);
        retryException = expectThrows(RetryException.class, () -> handler.checkForFailureStatusCode(httpRequest, httpResult));
        assertFalse(retryException.shouldRetry());
        assertThat(retryException.getCause().getMessage(), containsString("Unhandled redirection for request [null] status [300]"));
        // 402
        when(statusLine.getStatusCode()).thenReturn(402);
        retryException = expectThrows(RetryException.class, () -> handler.checkForFailureStatusCode(httpRequest, httpResult));
        assertFalse(retryException.shouldRetry());
        assertThat(
            retryException.getCause().getMessage(),
            containsString("Received an unsuccessful status code for request [null] status [402]")
        );
    }
}
