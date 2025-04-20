/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

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
import org.elasticsearch.xpack.inference.external.http.retry.ContentTooLargeException;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.hamcrest.MatcherAssert;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ElasticInferenceServiceResponseHandlerTests extends ESTestCase {

    public void testCheckForFailureStatusCode_DoesNotThrowFor200() {
        callCheckForFailureStatusCode(200, "id");
    }

    public void testCheckForFailureStatusCode_ThrowsFor400() {
        var exception = expectThrows(RetryException.class, () -> callCheckForFailureStatusCode(400, "id"));
        assertFalse(exception.shouldRetry());
        MatcherAssert.assertThat(
            exception.getCause().getMessage(),
            containsString("Received a bad request status code for request from inference entity id [id] status [400]")
        );
        MatcherAssert.assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.BAD_REQUEST));
    }

    public void testCheckForFailureStatusCode_ThrowsFor405() {
        var exception = expectThrows(RetryException.class, () -> callCheckForFailureStatusCode(405, "id"));
        assertFalse(exception.shouldRetry());
        MatcherAssert.assertThat(
            exception.getCause().getMessage(),
            containsString("Received a method not allowed status code for request from inference entity id [id] status [405]")
        );
        MatcherAssert.assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.METHOD_NOT_ALLOWED));
    }

    public void testCheckForFailureStatusCode_ThrowsFor413() {
        var exception = expectThrows(ContentTooLargeException.class, () -> callCheckForFailureStatusCode(413, "id"));
        assertTrue(exception.shouldRetry());
        MatcherAssert.assertThat(
            exception.getCause().getMessage(),
            containsString("Received a content too large status code for request from inference entity id [id] status [413]")
        );
        MatcherAssert.assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.REQUEST_ENTITY_TOO_LARGE));
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

    public void testCheckForFailureStatusCode_ThrowsFor402() {
        var exception = expectThrows(RetryException.class, () -> callCheckForFailureStatusCode(402, "id"));
        assertFalse(exception.shouldRetry());
        MatcherAssert.assertThat(
            exception.getCause().getMessage(),
            containsString("Received an unsuccessful status code for request from inference entity id [id] status [402]")
        );
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

        String responseJson = Strings.format("""
                {
                    "message": "%s"
                }
            """, errorMessage);

        var mockRequest = mock(Request.class);
        when(mockRequest.getInferenceEntityId()).thenReturn(modelId);
        var httpResult = new HttpResult(httpResponse, errorMessage == null ? new byte[] {} : responseJson.getBytes(StandardCharsets.UTF_8));
        var handler = new ElasticInferenceServiceResponseHandler("", (request, result) -> null);

        handler.checkForFailureStatusCode(mockRequest, httpResult);
    }
}
