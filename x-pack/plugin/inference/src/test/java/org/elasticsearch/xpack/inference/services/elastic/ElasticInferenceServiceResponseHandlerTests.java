/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

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
import org.hamcrest.MatcherAssert;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ElasticInferenceServiceResponseHandlerTests extends ESTestCase {

    public record FailureTestCase(int inputStatusCode, RestStatus expectedStatus, String errorMessage, boolean shouldRetry) {}

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
                        false
                    ) },
                {
                    new FailureTestCase(
                        402,
                        RestStatus.PAYMENT_REQUIRED,
                        "Received an unsuccessful status code for request from inference entity id [id] status [402]",
                        false
                    ) },
                {
                    new FailureTestCase(
                        405,
                        RestStatus.METHOD_NOT_ALLOWED,
                        "Received a method not allowed status code for request from inference entity id [id] status [405]",
                        false
                    ) },
                {
                    new FailureTestCase(
                        413,
                        RestStatus.REQUEST_ENTITY_TOO_LARGE,
                        "Received a content too large status code for request from inference entity id [id] status [413]",
                        true
                    ) },
                {
                    new FailureTestCase(
                        500,
                        RestStatus.BAD_REQUEST,
                        "Received a server error status code for request from inference entity id [id] status [500]",
                        true
                    ) },
                {
                    new FailureTestCase(
                        503,
                        RestStatus.BAD_REQUEST,
                        "Received a server error status code for request from inference entity id [id] status [503]",
                        true
                    ) },

            }
        );
    }

    public void testCheckForFailureStatusCode_Throws_WithErrorMessage() {
        var exception = expectThrows(
            RetryException.class,
            () -> callCheckForFailureStatusCode(failureTestCase.inputStatusCode, failureTestCase.errorMessage, "id")
        );
        assertThat(exception.shouldRetry(), is(failureTestCase.shouldRetry));
        MatcherAssert.assertThat(exception.getCause().getMessage(), containsString(failureTestCase.errorMessage));
        MatcherAssert.assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(failureTestCase.expectedStatus));
    }

    public void testCheckForFailureStatusCode_DoesNotThrowFor200() {
        callCheckForFailureStatusCode(200, null, "id");
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
