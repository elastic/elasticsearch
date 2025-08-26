/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic;

import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.message.BasicHeader;
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

public class AnthropicResponseHandlerTests extends ESTestCase {

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

    public void testCheckForFailureStatusCode_ThrowsFor529_ShouldRetry() {
        var exception = expectThrows(RetryException.class, () -> callCheckForFailureStatusCode(529, "id"));
        assertTrue(exception.shouldRetry());
        assertThat(
            exception.getCause().getMessage(),
            containsString(
                "Received an Anthropic server is temporarily overloaded status code for request from inference entity id [id] status [529]"
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
            containsString(
                "Received a rate limit status code. Token limit [unknown], remaining tokens [unknown], tokens reset [unknown]. "
                    + "Request limit [unknown], remaining requests [unknown], request reset [unknown]. "
                    + "Retry after [unknown] for request from inference entity id [id] status [429]"
            )
        );
        assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.TOO_MANY_REQUESTS));
    }

    public void testCheckForFailureStatusCode_ThrowsFor429_ShouldRetry_RetrievesFieldsFromHeaders() {
        int statusCode = 429;
        var statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(statusCode);
        var response = mock(HttpResponse.class);
        when(response.getStatusLine()).thenReturn(statusLine);
        var httpResult = new HttpResult(response, new byte[] {});

        when(response.getFirstHeader(AnthropicResponseHandler.REQUESTS_LIMIT)).thenReturn(
            new BasicHeader(AnthropicResponseHandler.REQUESTS_LIMIT, "3000")
        );
        when(response.getFirstHeader(AnthropicResponseHandler.REMAINING_REQUESTS)).thenReturn(
            new BasicHeader(AnthropicResponseHandler.REMAINING_REQUESTS, "2999")
        );
        when(response.getFirstHeader(AnthropicResponseHandler.TOKENS_LIMIT)).thenReturn(
            new BasicHeader(AnthropicResponseHandler.TOKENS_LIMIT, "10000")
        );
        when(response.getFirstHeader(AnthropicResponseHandler.REMAINING_TOKENS)).thenReturn(
            new BasicHeader(AnthropicResponseHandler.REMAINING_TOKENS, "99800")
        );
        when(response.getFirstHeader(AnthropicResponseHandler.REQUEST_RESET)).thenReturn(
            new BasicHeader(AnthropicResponseHandler.REQUEST_RESET, "123")
        );
        when(response.getFirstHeader(AnthropicResponseHandler.TOKENS_RESET)).thenReturn(
            new BasicHeader(AnthropicResponseHandler.TOKENS_RESET, "456")
        );
        when(response.getFirstHeader(AnthropicResponseHandler.RETRY_AFTER)).thenReturn(
            new BasicHeader(AnthropicResponseHandler.RETRY_AFTER, "2")
        );

        var error = AnthropicResponseHandler.buildRateLimitErrorMessage(httpResult);
        assertThat(
            error,
            containsString(
                "Received a rate limit status code. Token limit [10000], remaining tokens [99800], tokens reset [456]. "
                    + "Request limit [3000], remaining requests [2999], request reset [123]. Retry after [2]"
            )
        );
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

    private static void callCheckForFailureStatusCode(int statusCode, String inferenceEntityId) {
        var statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(statusCode);

        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);
        var header = mock(Header.class);
        when(header.getElements()).thenReturn(new HeaderElement[] {});
        when(httpResponse.getFirstHeader(anyString())).thenReturn(header);

        var mockRequest = mock(Request.class);
        when(mockRequest.getInferenceEntityId()).thenReturn(inferenceEntityId);
        var httpResult = new HttpResult(httpResponse, new byte[] {});
        var handler = new AnthropicResponseHandler("", (request, result) -> null, false);

        handler.checkForFailureStatusCode(mockRequest, httpResult);
    }

}
