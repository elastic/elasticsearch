/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicHttpResponse;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceResponseHandler.RETRY_AFTER_HEADER;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ElasticInferenceServiceResponseHandlerTests extends ESTestCase {

    public record FailureTestCase(
        int inputStatusCode,
        RestStatus expectedStatus,
        String errorMessage,
        boolean shouldRetry,
        Map<String, String> headers
    ) {}

    private final FailureTestCase failureTestCase;

    public ElasticInferenceServiceResponseHandlerTests(FailureTestCase failureTestCase) {
        this.failureTestCase = failureTestCase;
    }

    @ParametersFactory
    public static Iterable<FailureTestCase[]> parameters() throws Exception {
        return java.util.Arrays.asList(
            new FailureTestCase[][] {
                {
                    new FailureTestCase(
                        400,
                        RestStatus.BAD_REQUEST,
                        "Received a bad request status code for request from inference entity id [id] status [400]",
                        false,
                        Map.of()
                    ) },
                {
                    new FailureTestCase(
                        402,
                        RestStatus.PAYMENT_REQUIRED,
                        "Received an unsuccessful status code for request from inference entity id [id] status [402]",
                        false,
                        Map.of()
                    ) },
                {
                    new FailureTestCase(
                        405,
                        RestStatus.METHOD_NOT_ALLOWED,
                        "Received a method not allowed status code for request from inference entity id [id] status [405]",
                        false,
                        Map.of()
                    ) },
                {
                    new FailureTestCase(
                        413,
                        RestStatus.REQUEST_ENTITY_TOO_LARGE,
                        "Received a content too large status code for request from inference entity id [id] status [413]",
                        true,
                        Map.of()
                    ) },
                {
                    new FailureTestCase(
                        429,
                        RestStatus.TOO_MANY_REQUESTS,
                        "Received a rate limit status code for request from inference entity id [id] status [429]",
                        true,
                        Map.of()
                    ) },
                {
                    new FailureTestCase(
                        429,
                        RestStatus.TOO_MANY_REQUESTS,
                        "Received a rate limit status code for request from inference entity id [id] status [429]",
                        true,
                        Map.of(RETRY_AFTER_HEADER, "123")
                    ) },
                {
                    new FailureTestCase(
                        500,
                        RestStatus.BAD_REQUEST,
                        "Received a server error status code for request from inference entity id [id] status [500]",
                        true,
                        Map.of()
                    ) },
                {
                    new FailureTestCase(
                        500,
                        RestStatus.BAD_REQUEST,
                        "Received a server error status code for request from inference entity id [id] status [500]",
                        true,
                        Map.of(RETRY_AFTER_HEADER, "42")
                    ) },
                {
                    new FailureTestCase(
                        503,
                        RestStatus.BAD_REQUEST,
                        "Received a server error status code for request from inference entity id [id] status [503]",
                        true,
                        Map.of()
                    ) },

            }
        );
    }

    public void testCheckForFailureStatusCode_Throws_WithErrorMessage() {
        var exception = expectThrows(
            RetryException.class,
            () -> callCheckForFailureStatusCode(
                failureTestCase.inputStatusCode,
                failureTestCase.errorMessage,
                "id",
                failureTestCase.headers
            )
        );
        assertThat(exception.shouldRetry(), is(failureTestCase.shouldRetry));
        assertThat(exception.getCause().getMessage(), containsString(failureTestCase.errorMessage));
        assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(failureTestCase.expectedStatus));
        if (failureTestCase.headers.containsKey(RETRY_AFTER_HEADER)) {
            assertCauseHasRetryHeader(exception, failureTestCase.headers.get(RETRY_AFTER_HEADER));
        }
    }

    public void testCheckForFailureStatusCode_DoesNotThrowFor200() {
        callCheckForFailureStatusCode(200, null, "id", Map.of());
    }

    public void testCheckForFailureStatusCode_AlwaysAppliesRetryAfterHeaderWhenPresent() {
        final String retryAfter = String.valueOf(randomIntBetween(1, 1000));
        var exception = expectThrows(
            RetryException.class,
            () -> callCheckForFailureStatusCode(
                randomIntBetween(300, 599),
                randomAlphaOfLength(10),
                "id",
                Map.of(RETRY_AFTER_HEADER, retryAfter)
            )
        );
        assertCauseHasRetryHeader(exception, retryAfter);
    }

    private static void callCheckForFailureStatusCode(
        int statusCode,
        @Nullable String errorMessage,
        String modelId,
        Map<String, String> headers
    ) {
        var statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(statusCode);

        var httpResponse = new BasicHttpResponse(statusLine);
        givenResponseHasHeaders(httpResponse, headers);

        String responseJson = Strings.format("""
                {
                    "message": "%s"
                }
            """, errorMessage);

        var mockRequest = mock(OutboundRequest.class);
        when(mockRequest.getInferenceEntityId()).thenReturn(modelId);
        var httpResult = new HttpResult(httpResponse, errorMessage == null ? new byte[] {} : responseJson.getBytes(StandardCharsets.UTF_8));
        var handler = new ElasticInferenceServiceResponseHandler("", (request, result) -> null);

        handler.checkForFailureStatusCode(mockRequest, httpResult);
    }

    private static void givenResponseHasHeaders(HttpResponse httpResponse, Map<String, String> headersMap) {
        headersMap.entrySet().stream().forEach(e -> httpResponse.addHeader(new BasicHeader(e.getKey(), e.getValue())));
    }

    private void assertCauseHasRetryHeader(RetryException e, String retryAfterValue) {
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        assertThat(cause, instanceOf(ElasticsearchException.class));
        if (cause instanceof ElasticsearchException causeAsEsException) {
            assertThat(causeAsEsException.getHttpHeader(RETRY_AFTER_HEADER), is(notNullValue()));
            assertThat(causeAsEsException.getHttpHeader(RETRY_AFTER_HEADER), contains(retryAfterValue));
        }
    }
}
