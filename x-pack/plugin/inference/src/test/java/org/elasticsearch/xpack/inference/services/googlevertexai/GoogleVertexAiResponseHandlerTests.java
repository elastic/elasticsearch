/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai;

import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpResponse;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GoogleVertexAiResponseHandlerTests extends ESTestCase {

    public void testBuildFailureStatusCodeException_ReturnsFor500_ShouldRetry() {
        var exception = callHandleFailureStatusCode(500, "id");
        assertTrue(exception.shouldRetry());
        assertThat(
            exception.getCause().getMessage(),
            containsString("Received a server error status code for request from inference entity id [id] status [500]")
        );
        assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.BAD_REQUEST));
    }

    public void testBuildFailureStatusCodeException_ReturnsFor503_ShouldRetry() {
        var exception = callHandleFailureStatusCode(503, "id");
        assertTrue(exception.shouldRetry());
        assertThat(
            exception.getCause().getMessage(),
            containsString(
                "The Google Vertex AI service may be temporarily overloaded or down for request from inference entity id [id] status [503]"
            )
        );
        assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.BAD_REQUEST));
    }

    public void testBuildFailureStatusCodeException_ReturnsFor505_ShouldNotRetry() {
        var exception = callHandleFailureStatusCode(505, "id");
        assertFalse(exception.shouldRetry());
        assertThat(
            exception.getCause().getMessage(),
            containsString("Received a server error status code for request from inference entity id [id] status [505]")
        );
        assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.BAD_REQUEST));
    }

    public void testBuildFailureStatusCodeException_ReturnsFor429_ShouldRetry() {
        var exception = callHandleFailureStatusCode(429, "id");
        assertTrue(exception.shouldRetry());
        assertThat(
            exception.getCause().getMessage(),
            containsString("Received a rate limit status code for request from inference entity id [id] status [429]")
        );
        assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.TOO_MANY_REQUESTS));
    }

    public void testBuildFailureStatusCodeException_ReturnsFor404_ShouldNotRetry() {
        var exception = callHandleFailureStatusCode(404, "id");
        assertFalse(exception.shouldRetry());
        assertThat(
            exception.getCause().getMessage(),
            containsString("Resource not found at [null] for request from inference entity id [id] status [404]")
        );
        assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.NOT_FOUND));
    }

    public void testBuildFailureStatusCodeException_ReturnsFor403_ShouldNotRetry() {
        var exception = callHandleFailureStatusCode(403, "id");
        assertFalse(exception.shouldRetry());
        assertThat(
            exception.getCause().getMessage(),
            containsString("Received a permission denied error status code for request from inference entity id [id] status [403]")
        );
        assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.FORBIDDEN));
    }

    public void testBuildFailureStatusCodeException_ReturnsFor300_ShouldNotRetry() {
        var exception = callHandleFailureStatusCode(300, "id");
        assertFalse(exception.shouldRetry());
        assertThat(
            exception.getCause().getMessage(),
            containsString("Unhandled redirection for request from inference entity id [id] status [300]")
        );
        assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.MULTIPLE_CHOICES));
    }

    public void testBuildFailureStatusCodeException_ReturnsFor425_ShouldNotRetry() {
        var exception = callHandleFailureStatusCode(425, "id");
        assertFalse(exception.shouldRetry());
        assertThat(
            exception.getCause().getMessage(),
            containsString("Received an unsuccessful status code for request from inference entity id [id] status [425]")
        );
        assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.BAD_REQUEST));
    }

    private static RetryException callHandleFailureStatusCode(int statusCode, String modelId) {
        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getCode()).thenReturn(statusCode);
        var header = mock(Header.class);
        when(httpResponse.getFirstHeader(anyString())).thenReturn(header);

        var mockRequest = mock(OutboundRequest.class);
        when(mockRequest.getInferenceEntityId()).thenReturn(modelId);
        var httpResult = new HttpResult(httpResponse, new byte[] {});
        var handler = new GoogleVertexAiResponseHandler("", (request, result) -> null);

        return handler.buildFailureStatusCodeException(mockRequest, httpResult);
    }
}
