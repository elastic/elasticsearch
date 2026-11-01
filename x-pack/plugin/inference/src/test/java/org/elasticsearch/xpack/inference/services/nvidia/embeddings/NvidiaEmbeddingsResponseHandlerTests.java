/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.embeddings;

import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpResponse;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ContentTooLargeException;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.RequestTests;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NvidiaEmbeddingsResponseHandlerTests extends ESTestCase {
    private static final String INFERENCE_ID = "id";
    private final NvidiaEmbeddingsResponseHandler responseHandler = new NvidiaEmbeddingsResponseHandler("embeddings", (a, b) -> null);

    public void testCheckForFailureStatusCode_413ContentTooLarge() {
        var httpResponse = mockHttpResponse();

        var mockRequest = RequestTests.mockRequest(INFERENCE_ID);
        var httpResult = new HttpResult(httpResponse, new byte[] {});

        // 413
        when(httpResponse.getCode()).thenReturn(413);
        RetryException retryException = responseHandler.buildFailureStatusCodeException(mockRequest, httpResult);
        assertThat(retryException, instanceOf(ContentTooLargeException.class));
        assertThat(retryException.shouldRetry(), is(true));
        assertThat(retryException.getCause().getMessage(), containsString("Received a content too large status code"));
        assertThat(((ElasticsearchStatusException) retryException.getCause()).status(), is(RestStatus.REQUEST_ENTITY_TOO_LARGE));
    }

    public void testCheckForFailureStatusCode_400ContentTooLarge() {
        var mockRequest = RequestTests.mockRequest(INFERENCE_ID);
        // 400 content too large
        var retryException = responseHandler.buildFailureStatusCodeException(mockRequest, createContentTooLargeResult400());
        assertThat(retryException, instanceOf(ContentTooLargeException.class));
        assertThat(retryException.shouldRetry(), is(true));
        assertThat(retryException.getCause().getMessage(), containsString("Received a content too large status code"));
        assertThat(((ElasticsearchStatusException) retryException.getCause()).status(), is(RestStatus.BAD_REQUEST));
    }

    public void testCheckForFailureStatusCode_400GenericBadRequest() {
        var httpResponse = mockHttpResponse();

        var mockRequest = RequestTests.mockRequest(INFERENCE_ID);
        var httpResult = new HttpResult(httpResponse, new byte[] {});
        // 400 generic bad request should not be marked as a content too large
        when(httpResponse.getCode()).thenReturn(400);
        var retryException = responseHandler.buildFailureStatusCodeException(mockRequest, httpResult);
        assertThat(retryException.shouldRetry(), is(false));
        assertThat(
            retryException.getCause().getMessage(),
            containsString("Received a bad request status code for request from inference entity id [id] status [400]")
        );
        assertThat(((ElasticsearchStatusException) retryException.getCause()).status(), is(RestStatus.BAD_REQUEST));
    }

    private static HttpResponse mockHttpResponse() {
        var httpResponse = mock(HttpResponse.class);
        var header = mock(Header.class);
        when(httpResponse.getFirstHeader(anyString())).thenReturn(header);
        return httpResponse;
    }

    private static HttpResult createContentTooLargeResult400() {
        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getCode()).thenReturn(400);

        String responseJson = Strings.format("""
                {
                    "error": "%s"
                }
            """, "Input length 18432 exceeds maximum allowed token size 8192");

        return new HttpResult(httpResponse, responseJson.getBytes(StandardCharsets.UTF_8));
    }
}
