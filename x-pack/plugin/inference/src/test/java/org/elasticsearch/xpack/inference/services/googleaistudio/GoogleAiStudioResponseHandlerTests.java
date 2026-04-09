/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googleaistudio;

import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.Request;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GoogleAiStudioResponseHandlerTests extends ESTestCase {

    public void testCheckForFailureStatusCode_DoesNotThrowFor200() {
        callCheckForFailureStatusCode(200, "id");
    }

    public void testCheckForFailureStatusCode_ThrowsFor500_ShouldRetry() {
        var exception = expectThrows(RetryException.class, () -> callCheckForFailureStatusCode(500, "id"));
        assertTrue(exception.shouldRetry());
        assertThat(
            exception.getCause().getMessage(),
            containsString("Received a server error status code for request from inference entity id [id] status [500]")
        );
        assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.BAD_REQUEST));
    }

    public void testCheckForFailureStatusCode_ThrowsFor503_ShouldRetry() {
        var exception = expectThrows(RetryException.class, () -> callCheckForFailureStatusCode(503, "id"));
        assertTrue(exception.shouldRetry());
        assertThat(
            exception.getCause().getMessage(),
            containsString(
                "The Google AI Studio service may be temporarily overloaded or down for request from inference entity id [id] status [503]"
            )
        );
        assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.BAD_REQUEST));
    }

    public void testCheckForFailureStatusCode_ThrowsFor505_ShouldNotRetry() {
        var exception = expectThrows(RetryException.class, () -> callCheckForFailureStatusCode(505, "id"));
        assertFalse(exception.shouldRetry());
        assertThat(
            exception.getCause().getMessage(),
            containsString("Received a server error status code for request from inference entity id [id] status [505]")
        );
        assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.BAD_REQUEST));
    }

    public void testCheckForFailureStatusCode_ThrowsFor429_ShouldRetry() {
        var exception = expectThrows(RetryException.class, () -> callCheckForFailureStatusCode(429, "id"));
        assertTrue(exception.shouldRetry());
        assertThat(
            exception.getCause().getMessage(),
            containsString("Received a rate limit status code for request from inference entity id [id] status [429]")
        );
        assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.TOO_MANY_REQUESTS));
    }

    public void testCheckForFailureStatusCode_ThrowsFor404_ShouldNotRetry() {
        var exception = expectThrows(RetryException.class, () -> callCheckForFailureStatusCode(404, "id"));
        assertFalse(exception.shouldRetry());
        assertThat(
            exception.getCause().getMessage(),
            containsString("Resource not found at [null] for request from inference entity id [id] status [404]")
        );
        assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.NOT_FOUND));
    }

    public void testCheckForFailureStatusCode_ThrowsFor403_ShouldNotRetry() {
        var exception = expectThrows(RetryException.class, () -> callCheckForFailureStatusCode(403, "id"));
        assertFalse(exception.shouldRetry());
        assertThat(
            exception.getCause().getMessage(),
            containsString("Received a permission denied error status code for request from inference entity id [id] status [403]")
        );
        assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.FORBIDDEN));
    }

    public void testCheckForFailureStatusCode_ThrowsFor300_ShouldNotRetry() {
        var exception = expectThrows(RetryException.class, () -> callCheckForFailureStatusCode(300, "id"));
        assertFalse(exception.shouldRetry());
        assertThat(
            exception.getCause().getMessage(),
            containsString("Unhandled redirection for request from inference entity id [id] status [300]")
        );
        assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.MULTIPLE_CHOICES));
    }

    public void testCheckForFailureStatusCode_ThrowsFor425_ShouldNotRetry() {
        var exception = expectThrows(RetryException.class, () -> callCheckForFailureStatusCode(425, "id"));
        assertFalse(exception.shouldRetry());
        assertThat(
            exception.getCause().getMessage(),
            containsString("Received an unsuccessful status code for request from inference entity id [id] status [425]")
        );
        assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.BAD_REQUEST));
    }

    private static void callCheckForFailureStatusCode(int statusCode, String modelId) {
        var statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(statusCode);

        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);
        var header = mock(Header.class);
        when(header.getElements()).thenReturn(new HeaderElement[] {});
        when(httpResponse.getFirstHeader(anyString())).thenReturn(header);

        var mockRequest = mock(Request.class);
        when(mockRequest.getInferenceEntityId()).thenReturn(modelId);
        var httpResult = new HttpResult(httpResponse, new byte[] {});
        var handler = new GoogleAiStudioResponseHandler("", (request, result) -> null);

        handler.checkForFailureStatusCode(mockRequest, httpResult);
    }

}
