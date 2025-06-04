/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai;

import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.hamcrest.MatcherAssert;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JinaAIResponseHandlerTests extends ESTestCase {
    public void testCheckForFailureStatusCode_DoesNotThrowForStatusCodesBetween200And299() {
        callCheckForFailureStatusCode(randomIntBetween(200, 299), "id");
    }

    public void testCheckForFailureStatusCode_ThrowsFor503() {
        var exception = expectThrows(RetryException.class, () -> callCheckForFailureStatusCode(503, "id"));
        assertFalse(exception.shouldRetry());
        MatcherAssert.assertThat(
            exception.getCause().getMessage(),
            containsString("Received a server error status code for request from inference entity id [id] status [503]")
        );
        MatcherAssert.assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.BAD_REQUEST));
    }

    public void testCheckForFailureStatusCode_ThrowsFor500_WithShouldRetryTrue() {
        var exception = expectThrows(RetryException.class, () -> callCheckForFailureStatusCode(500, "id"));
        assertTrue(exception.shouldRetry());
        MatcherAssert.assertThat(
            exception.getCause().getMessage(),
            containsString("Received a server error status code for request from inference entity id [id] status [500]")
        );
        MatcherAssert.assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.BAD_REQUEST));
    }

    public void testCheckForFailureStatusCode_ThrowsFor429_WithShouldRetryTrue() {
        var exception = expectThrows(RetryException.class, () -> callCheckForFailureStatusCode(429, "id"));
        assertTrue(exception.shouldRetry());
        MatcherAssert.assertThat(
            exception.getCause().getMessage(),
            containsString("Received a rate limit status code for request from inference entity id [id] status [429]")
        );
        MatcherAssert.assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.TOO_MANY_REQUESTS));
    }

    public void testCheckForFailureStatusCode_ThrowsFor400() {
        var exception = expectThrows(RetryException.class, () -> callCheckForFailureStatusCode(400, "id"));
        assertFalse(exception.shouldRetry());
        MatcherAssert.assertThat(
            exception.getCause().getMessage(),
            containsString("Received an input validation error response for request from inference entity id [id] status [400]")
        );
        MatcherAssert.assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.BAD_REQUEST));
    }

    public void testCheckForFailureStatusCode_ThrowsFor400_InputsTooLarge() {
        var exception = expectThrows(
            RetryException.class,
            () -> callCheckForFailureStatusCode(400, "\"input\" length 2049 is larger than the largest allowed size 2048", "id")
        );
        assertFalse(exception.shouldRetry());
        MatcherAssert.assertThat(
            exception.getCause().getMessage(),
            containsString("Received an input validation error response for request from inference entity id [id] status [400]")
        );
        MatcherAssert.assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.BAD_REQUEST));
    }

    public void testCheckForFailureStatusCode_ThrowsFor401() {
        var exception = expectThrows(RetryException.class, () -> callCheckForFailureStatusCode(401, "inferenceEntityId"));
        assertFalse(exception.shouldRetry());
        MatcherAssert.assertThat(
            exception.getCause().getMessage(),
            containsString(
                "Received an authentication error status code for request from inference entity id [inferenceEntityId] status [401]"
            )
        );
        MatcherAssert.assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.UNAUTHORIZED));
    }

    public void testCheckForFailureStatusCode_ThrowsFor402() {
        var exception = expectThrows(RetryException.class, () -> callCheckForFailureStatusCode(402, "inferenceEntityId"));
        assertFalse(exception.shouldRetry());
        MatcherAssert.assertThat(exception.getCause().getMessage(), containsString("Payment required"));
        MatcherAssert.assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.PAYMENT_REQUIRED));
    }

    private static void callCheckForFailureStatusCode(int statusCode, String modelId) {
        callCheckForFailureStatusCode(statusCode, null, modelId);
    }

    private static void callCheckForFailureStatusCode(int statusCode, @Nullable String errorMessage, String modelId) {
        var statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(statusCode);

        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);
        var header = mock(Header.class);
        when(header.getElements()).thenReturn(new HeaderElement[] {});
        when(httpResponse.getFirstHeader(anyString())).thenReturn(header);

        String escapedErrorMessage = errorMessage != null ? errorMessage.replace("\\", "\\\\").replace("\"", "\\\"") : errorMessage;

        String responseJson = Strings.format("""
                {
                    "detail": "%s"
                }
            """, escapedErrorMessage);

        var mockRequest = mock(Request.class);
        when(mockRequest.getInferenceEntityId()).thenReturn(modelId);
        var httpResult = new HttpResult(httpResponse, errorMessage == null ? new byte[] {} : responseJson.getBytes(StandardCharsets.UTF_8));
        var handler = new JinaAIResponseHandler("", (request, result) -> null);

        handler.checkForFailureStatusCode(mockRequest, httpResult);
    }
}
