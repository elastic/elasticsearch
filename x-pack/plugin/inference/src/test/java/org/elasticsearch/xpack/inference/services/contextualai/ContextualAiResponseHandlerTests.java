/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai;

import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpResponse;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;

import java.nio.charset.StandardCharsets;

import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_INFERENCE_ENTITY_ID;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ContextualAiResponseHandlerTests extends ESTestCase {

    public void testBuildFailureStatusCodeException_StatusCode500_ReturnsRetryableServerError() {
        var errorBody = "Internal server error: service unavailable";
        var exception = callHandleFailureStatusCode(500, errorBody);
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

    public void testBuildFailureStatusCodeException_StatusCode503_ReturnsRetryableServerError() {
        var errorBody = "Service temporarily unavailable";
        var exception = callHandleFailureStatusCode(503, errorBody);
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

    public void testBuildFailureStatusCodeException_StatusCode429_ReturnsRetryableRateLimitError() {
        var errorBody = "Rate limit exceeded, please retry later";
        var exception = callHandleFailureStatusCode(429, errorBody);
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

    public void testBuildFailureStatusCodeException_StatusCode401_ReturnsAuthenticationError() {
        var errorBody = "Invalid API key provided";
        var exception = callHandleFailureStatusCode(401, errorBody);
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

    public void testBuildFailureStatusCodeException_StatusCode400_ReturnsUnsuccessfulError() {
        var errorBody = "Invalid request: missing required field";
        var exception = callHandleFailureStatusCode(400, errorBody);
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

    public void testBuildFailureStatusCodeException_StatusCode300_ReturnsRedirectionError() {
        var errorBody = "Resource has been moved";
        var exception = callHandleFailureStatusCode(300, errorBody);
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

    private static RetryException callHandleFailureStatusCode(int statusCode, @Nullable String errorMessage) {
        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getCode()).thenReturn(statusCode);
        var header = mock(Header.class);
        when(httpResponse.getFirstHeader(anyString())).thenReturn(header);

        var mockRequest = mock(OutboundRequest.class);
        when(mockRequest.getInferenceEntityId()).thenReturn(TEST_INFERENCE_ENTITY_ID);
        var httpResult = new HttpResult(httpResponse, errorMessage == null ? new byte[] {} : errorMessage.getBytes(StandardCharsets.UTF_8));
        var handler = new ContextualAiResponseHandler("", (request, result) -> null, false);

        return handler.buildFailureStatusCodeException(mockRequest, httpResult);
    }
}
