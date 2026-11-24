/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud;

import org.apache.hc.core5.http.message.BasicHttpResponse;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TencentCloudMultimodalResponseHandlerTests extends ESTestCase {

    public void test500_InvalidParameterValue_IsNotRetried() {
        var exception = callCheckForFailureStatusCode(500, errorBody("InvalidParameterValue", "Texts cannot be nil or exceed"));
        assertFalse(exception.shouldRetry());
        assertThat(exception.getCause().getMessage(), containsString("Texts cannot be nil or exceed"));
    }

    public void test500_RequestLimitExceeded_IsNotRetried() {
        assertFalse(callCheckForFailureStatusCode(500, errorBody("RequestLimitExceeded", "Response too large")).shouldRetry());
    }

    public void test500_OtherError_IsRetried() {
        assertTrue(callCheckForFailureStatusCode(500, errorBody("InternalError", "boom")).shouldRetry());
        assertTrue(callCheckForFailureStatusCode(500, "not json").shouldRetry());
    }

    public void test502_IsRetried() {
        assertTrue(callCheckForFailureStatusCode(502, errorBody("FailedOperation", "upstream failed")).shouldRetry());
    }

    public void test429_IsRetried() {
        assertTrue(callCheckForFailureStatusCode(429, errorBody("RequestLimitExceeded", "rate limited")).shouldRetry());
    }

    public void test4xx_IsNotRetried() {
        var statusCode = randomFrom(400, 401, 403, 404, 413);
        assertFalse(callCheckForFailureStatusCode(statusCode, errorBody("InvalidParameter", "bad request")).shouldRetry());
    }

    public void testErrorCode() {
        assertThat(TencentCloudMultimodalResponseHandler.errorCode(result(500, errorBody("FailedOperation", "x"))), is("FailedOperation"));
        assertThat(TencentCloudMultimodalResponseHandler.errorCode(result(500, "{\"error\":{\"message\":\"x\"}}")), is(nullValue()));
        assertThat(TencentCloudMultimodalResponseHandler.errorCode(result(500, "")), is(nullValue()));
    }

    private static String errorBody(String code, String message) {
        return Strings.format("""
            {"error":{"message":"%s","type":"api_error","code":"%s"}}""", message, code);
    }

    private static RetryException callCheckForFailureStatusCode(int statusCode, String body) {
        var request = mock(OutboundRequest.class);
        when(request.getInferenceEntityId()).thenReturn("id");
        var handler = new TencentCloudMultimodalResponseHandler("test", (r, result) -> null);
        return handler.buildFailureStatusCodeException(request, result(statusCode, body));
    }

    private static HttpResult result(int statusCode, String body) {
        var httpResponse = new BasicHttpResponse(statusCode);
        return new HttpResult(httpResponse, body.getBytes(StandardCharsets.UTF_8));
    }
}
