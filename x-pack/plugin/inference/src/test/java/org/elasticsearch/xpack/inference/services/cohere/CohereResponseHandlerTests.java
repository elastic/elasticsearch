/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere;

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

public class CohereResponseHandlerTests extends ESTestCase {
    public void testCheckForFailureStatusCode_DoesNotThrowFor200() {
        callCheckForFailureStatusCode(200, "id");
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

    public void testCheckForFailureStatusCode_ThrowsFor429() {
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
            containsString("Received an unsuccessful status code for request from inference entity id [id] status [400]")
        );
        MatcherAssert.assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.BAD_REQUEST));
    }

    public void testCheckForFailureStatusCode_ThrowsFor400_TextsTooLarge() {
        var exception = expectThrows(
            RetryException.class,
            () -> callCheckForFailureStatusCode(400, "invalid request: total number of texts must be at most 96 - received 100", "id")
        );
        assertFalse(exception.shouldRetry());
        MatcherAssert.assertThat(
            exception.getCause().getMessage(),
            containsString("Received a texts array too large response for request from inference entity id [id] status [400]")
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

    public void testCheckForFailureStatusCode_ThrowsFor300() {
        var exception = expectThrows(RetryException.class, () -> callCheckForFailureStatusCode(300, "id"));
        assertFalse(exception.shouldRetry());
        MatcherAssert.assertThat(
            exception.getCause().getMessage(),
            containsString("Unhandled redirection for request from inference entity id [id] status [300]")
        );
        MatcherAssert.assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.MULTIPLE_CHOICES));
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

        String responseJson = Strings.format("""
                {
                    "message": "%s"
                }
            """, errorMessage);

        var mockRequest = mock(Request.class);
        when(mockRequest.getInferenceEntityId()).thenReturn(modelId);
        var httpResult = new HttpResult(httpResponse, errorMessage == null ? new byte[] {} : responseJson.getBytes(StandardCharsets.UTF_8));
        var handler = new CohereResponseHandler("", (request, result) -> null, false);

        handler.checkForFailureStatusCode(mockRequest, httpResult);
    }
}
