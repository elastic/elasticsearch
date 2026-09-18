/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;

import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OciGenAiResponseHandlerTests extends ESTestCase {

    private static final String ERROR_BODY = """
        { "code": "TooManyRequests", "message": "Too many requests for the given tenancy" }
        """;

    public void testRateLimitIsRetryable() {
        var exception = buildFailure(429);

        assertTrue(exception.shouldRetry());
        assertThat(
            exception.getCause().getMessage(),
            is(
                "Received a rate limit status code for request from inference entity id [id] status [429]. "
                    + "Error message: [Too many requests for the given tenancy]"
            )
        );
        assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.TOO_MANY_REQUESTS));
    }

    public void testServerErrorsAreRetryable() {
        assertTrue(buildFailure(500).shouldRetry());
        assertTrue(buildFailure(503).shouldRetry());
        assertThat(buildFailure(503).getCause().getMessage(), is(serverErrorMessage(503)));
    }

    public void testNotFound() {
        var exception = buildFailure(404);

        assertFalse(exception.shouldRetry());
        assertThat(
            exception.getCause().getMessage(),
            is(
                "Resource not found at [https://inference.example.com/20231130/actions/embedText] for request from inference entity id "
                    + "[id] status [404]. Error message: [Too many requests for the given tenancy]"
            )
        );
    }

    public void testAuthenticationAndPermissionErrors() {
        assertThat(buildFailure(401).getCause().getMessage(), is(message("Received an authentication error status code", 401)));
        assertThat(buildFailure(403).getCause().getMessage(), is(message("Received a permission denied error status code", 403)));
        assertThat(buildFailure(400).getCause().getMessage(), is(message("Received a bad request status code", 400)));
        assertThat(buildFailure(418).getCause().getMessage(), is(message("Received an unsuccessful status code", 418)));
        assertFalse(buildFailure(401).shouldRetry());
    }

    private static String serverErrorMessage(int status) {
        return message("Received a server error status code", status);
    }

    private static String message(String prefix, int status) {
        return prefix
            + " for request from inference entity id [id] status ["
            + status
            + "]. Error message: [Too many requests for the given tenancy]";
    }

    private static RetryException buildFailure(int statusCode) {
        var handler = new OciGenAiResponseHandler("test", (request, result) -> null);
        var exception = handler.buildFailureStatusCodeException(mockRequest(), httpResult(statusCode));
        assertThat(exception.getCause(), instanceOf(ElasticsearchStatusException.class));
        return exception;
    }

    private static OutboundRequest mockRequest() {
        var request = mock(OutboundRequest.class);
        when(request.getInferenceEntityId()).thenReturn("id");
        when(request.getURI()).thenReturn(URI.create("https://inference.example.com/20231130/actions/embedText"));
        return request;
    }

    private static HttpResult httpResult(int statusCode) {
        var statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(statusCode);
        var response = mock(HttpResponse.class);
        when(response.getStatusLine()).thenReturn(statusLine);
        return new HttpResult(response, ERROR_BODY.getBytes(StandardCharsets.UTF_8));
    }
}
