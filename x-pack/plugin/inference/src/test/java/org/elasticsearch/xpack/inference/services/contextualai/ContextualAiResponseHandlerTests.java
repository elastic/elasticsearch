/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai;

import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.nio.charset.StandardCharsets;

import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_INFERENCE_ENTITY_ID;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ContextualAiResponseHandlerTests extends ESTestCase {

    public void testCheckForFailureStatusCode_StatusCode200_DoesNotThrow() {
        callCheckForFailureStatusCode(200, null);
    }

    public void testCheckForFailureStatusCode_StatusCode500_ThrowsRetryableServerError() {
        var errorBody = "Internal server error: service unavailable";
        var exception = expectThrows(RetryException.class, () -> callCheckForFailureStatusCode(500, errorBody));
        assertTrue(exception.shouldRetry());
        assertThat(
            exception.getCause().getMessage(),
            containsString(
                Strings.format(
                    "Received a server error status code for request from inference entity id [%s] status [500]",
                    TEST_INFERENCE_ENTITY_ID
                )
            )
        );
        assertThat(exception.getCause().getMessage(), containsString(errorBody));
        assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.BAD_REQUEST));
    }

    public void testCheckForFailureStatusCode_StatusCode503_ThrowsRetryableServerError() {
        var errorBody = "Service temporarily unavailable";
        var exception = expectThrows(RetryException.class, () -> callCheckForFailureStatusCode(503, errorBody));
        assertTrue(exception.shouldRetry());
        assertThat(
            exception.getCause().getMessage(),
            containsString(
                Strings.format(
                    "Received a server error status code for request from inference entity id [%s] status [503]",
                    TEST_INFERENCE_ENTITY_ID
                )
            )
        );
        assertThat(exception.getCause().getMessage(), containsString(errorBody));
        assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.BAD_REQUEST));
    }

    public void testCheckForFailureStatusCode_StatusCode429_ThrowsRetryableRateLimitError() {
        var errorBody = "Rate limit exceeded, please retry later";
        var exception = expectThrows(RetryException.class, () -> callCheckForFailureStatusCode(429, errorBody));
        assertTrue(exception.shouldRetry());
        assertThat(
            exception.getCause().getMessage(),
            containsString(
                Strings.format(
                    "Received a rate limit status code for request from inference entity id [%s] status [429]",
                    TEST_INFERENCE_ENTITY_ID
                )
            )
        );
        assertThat(exception.getCause().getMessage(), containsString(errorBody));
        assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.TOO_MANY_REQUESTS));
    }

    public void testCheckForFailureStatusCode_StatusCode401_ThrowsAuthenticationError() {
        var errorBody = "Invalid API key provided";
        var exception = expectThrows(RetryException.class, () -> callCheckForFailureStatusCode(401, errorBody));
        assertFalse(exception.shouldRetry());
        assertThat(
            exception.getCause().getMessage(),
            containsString(
                Strings.format(
                    "Received an authentication error status code for request from inference entity id [%s] status [401]",
                    TEST_INFERENCE_ENTITY_ID
                )
            )
        );
        assertThat(exception.getCause().getMessage(), containsString(errorBody));
        assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.UNAUTHORIZED));
    }

    public void testCheckForFailureStatusCode_StatusCode400_ThrowsUnsuccessfulError() {
        var errorBody = "Invalid request: missing required field";
        var exception = expectThrows(RetryException.class, () -> callCheckForFailureStatusCode(400, errorBody));
        assertFalse(exception.shouldRetry());
        assertThat(
            exception.getCause().getMessage(),
            containsString(
                Strings.format(
                    "Received an unsuccessful status code for request from inference entity id [%s] status [400]",
                    TEST_INFERENCE_ENTITY_ID
                )
            )
        );
        assertThat(exception.getCause().getMessage(), containsString(errorBody));
        assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.BAD_REQUEST));
    }

    public void testCheckForFailureStatusCode_StatusCode300_ThrowsRedirectionError() {
        var errorBody = "Resource has been moved";
        var exception = expectThrows(RetryException.class, () -> callCheckForFailureStatusCode(300, errorBody));
        assertFalse(exception.shouldRetry());
        assertThat(
            exception.getCause().getMessage(),
            containsString(
                Strings.format("Unhandled redirection for request from inference entity id [%s] status [300]", TEST_INFERENCE_ENTITY_ID)
            )
        );
        assertThat(exception.getCause().getMessage(), containsString(errorBody));
        assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.MULTIPLE_CHOICES));
    }

    private static void callCheckForFailureStatusCode(int statusCode, @Nullable String errorMessage) {
        var statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(statusCode);

        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);
        var header = mock(Header.class);
        when(header.getElements()).thenReturn(new HeaderElement[] {});
        when(httpResponse.getFirstHeader(anyString())).thenReturn(header);

        var mockRequest = mock(Request.class);
        when(mockRequest.getInferenceEntityId()).thenReturn(TEST_INFERENCE_ENTITY_ID);
        var httpResult = new HttpResult(httpResponse, errorMessage == null ? new byte[] {} : errorMessage.getBytes(StandardCharsets.UTF_8));
        var handler = new ContextualAiResponseHandler("", (request, result) -> null, false);

        handler.checkForFailureStatusCode(mockRequest, httpResult);
    }
}
