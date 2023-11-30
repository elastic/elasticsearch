/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpRequestBase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OpenAiResponseHandlerTests extends ESTestCase {

    public void testCheckForFailureStatusCode() {
        var statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(200).thenReturn(503).thenReturn(429).thenReturn(401).thenReturn(300).thenReturn(402);

        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);

        var httpRequest = mock(HttpRequestBase.class);

        var httpResult = new HttpResult(httpResponse, new byte[] {});

        // 200 ok
        OpenAiResponseHandler.checkForFailureStatusCode(httpRequest, httpResult);
        // 503
        var retryException = expectThrows(
            RetryException.class,
            () -> OpenAiResponseHandler.checkForFailureStatusCode(httpRequest, httpResult)
        );
        assertFalse(retryException.shouldRetry());
        assertThat(retryException.getMessage(), containsString("Received a server error status code for request [null] status [503]"));
        // 429
        retryException = expectThrows(RetryException.class, () -> OpenAiResponseHandler.checkForFailureStatusCode(httpRequest, httpResult));
        assertFalse(retryException.shouldRetry());
        assertThat(retryException.getMessage(), containsString("Received a rate limit status code for request [null] status [429]"));
        // 401
        retryException = expectThrows(RetryException.class, () -> OpenAiResponseHandler.checkForFailureStatusCode(httpRequest, httpResult));
        assertFalse(retryException.shouldRetry());
        assertThat(
            retryException.getMessage(),
            containsString("Received a authentication error status code for request [null] status [401]")
        );
        // 300
        retryException = expectThrows(RetryException.class, () -> OpenAiResponseHandler.checkForFailureStatusCode(httpRequest, httpResult));
        assertFalse(retryException.shouldRetry());
        assertThat(retryException.getMessage(), containsString("Unhandled redirection for request [null] status [300]"));
        // 402
        retryException = expectThrows(RetryException.class, () -> OpenAiResponseHandler.checkForFailureStatusCode(httpRequest, httpResult));
        assertFalse(retryException.shouldRetry());
        assertThat(retryException.getMessage(), containsString("Received an unsuccessful status code for request [null] status [402]"));
    }
}
