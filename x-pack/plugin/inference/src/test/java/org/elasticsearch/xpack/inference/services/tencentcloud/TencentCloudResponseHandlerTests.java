/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud;

import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TencentCloudResponseHandlerTests extends ESTestCase {

    private static final String ENTITY_ID = "id";
    private static final String AUTH_ENTITY_ID = "inferenceEntityId";

    public void testBuildFailureStatusCodeException_500_ReturnsRetryTrue() {
        var exception = callHandleFailureStatusCode(500, ENTITY_ID);
        assertTrue(exception.shouldRetry());
        assertThat(exception.getCause().getMessage(), containsString(serverErrorMessage(ENTITY_ID, 500)));
        assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.BAD_REQUEST));
    }

    public void testBuildFailureStatusCodeException_GreaterThan500_ReturnsRetryFalse() {
        var exception = callHandleFailureStatusCode(503, ENTITY_ID);
        assertFalse(exception.shouldRetry());
        assertThat(exception.getCause().getMessage(), containsString(serverErrorMessage(ENTITY_ID, 503)));
    }

    public void testBuildFailureStatusCodeException_429_ReturnsRetryTrue() {
        var exception = callHandleFailureStatusCode(429, ENTITY_ID);
        assertTrue(exception.shouldRetry());
        assertThat(exception.getCause().getMessage(), containsString(rateLimitMessage(ENTITY_ID, 429)));
        assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.TOO_MANY_REQUESTS));
    }

    public void testBuildFailureStatusCodeException_400_ReturnsValidationError() {
        var exception = callHandleFailureStatusCode(400, ENTITY_ID);
        assertFalse(exception.shouldRetry());
        assertThat(exception.getCause().getMessage(), containsString(TencentCloudResponseHandler.VALIDATION_ERROR_MESSAGE));
    }

    public void testBuildFailureStatusCodeException_422_ReturnsValidationError() {
        var exception = callHandleFailureStatusCode(422, ENTITY_ID);
        assertFalse(exception.shouldRetry());
        assertThat(exception.getCause().getMessage(), containsString(TencentCloudResponseHandler.VALIDATION_ERROR_MESSAGE));
    }

    public void testBuildFailureStatusCodeException_401_ReturnsAuthError() {
        var exception = callHandleFailureStatusCode(401, AUTH_ENTITY_ID);
        assertFalse(exception.shouldRetry());
        assertThat(exception.getCause().getMessage(), containsString(authErrorMessage(AUTH_ENTITY_ID, 401)));
        assertThat(((ElasticsearchStatusException) exception.getCause()).status(), is(RestStatus.UNAUTHORIZED));
    }

    public void testBuildFailureStatusCodeException_403_ReturnsPermissionError() {
        var exception = callHandleFailureStatusCode(403, ENTITY_ID);
        assertFalse(exception.shouldRetry());
        assertThat(exception.getCause().getMessage(), containsString(TencentCloudResponseHandler.PERMISSION_ERROR_MESSAGE));
    }

    public void testBuildFailureStatusCodeException_3xx_ReturnsRedirectionError() {
        var exception = callHandleFailureStatusCode(301, ENTITY_ID);
        assertFalse(exception.shouldRetry());
        assertThat(exception.getCause().getMessage(), containsString(redirectionMessage(ENTITY_ID, 301)));
    }

    public void testBuildFailureStatusCodeException_OtherCode_ReturnsUnsuccessful() {
        var exception = callHandleFailureStatusCode(409, ENTITY_ID);
        assertFalse(exception.shouldRetry());
        assertThat(exception.getCause().getMessage(), containsString(unsuccessfulMessage(ENTITY_ID, 409)));
    }

    private static String serverErrorMessage(String entityId, int statusCode) {
        return Strings.format("Received a server error status code for request from inference entity id [%s] status [%d]", entityId, statusCode);
    }

    private static String rateLimitMessage(String entityId, int statusCode) {
        return Strings.format("Received a rate limit status code for request from inference entity id [%s] status [%d]", entityId, statusCode);
    }

    private static String authErrorMessage(String entityId, int statusCode) {
        return Strings.format(
            "Received an authentication error status code for request from inference entity id [%s] status [%d]",
            entityId,
            statusCode
        );
    }

    private static String redirectionMessage(String entityId, int statusCode) {
        return Strings.format("Unhandled redirection for request from inference entity id [%s] status [%d]", entityId, statusCode);
    }

    private static String unsuccessfulMessage(String entityId, int statusCode) {
        return Strings.format(
            "Received an unsuccessful status code for request from inference entity id [%s] status [%d]",
            entityId,
            statusCode
        );
    }

    private static RetryException callHandleFailureStatusCode(int statusCode, String modelId) {
        var statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(statusCode);
        when(statusLine.toString()).thenReturn("HTTP/1.1 " + statusCode + " Error");

        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);
        var header = mock(Header.class);
        when(header.getElements()).thenReturn(new HeaderElement[] {});
        when(httpResponse.getFirstHeader(anyString())).thenReturn(header);

        var mockRequest = mock(OutboundRequest.class);
        when(mockRequest.getInferenceEntityId()).thenReturn(modelId);

        var httpResult = new HttpResult(httpResponse, new byte[] {});
        var handler = new TencentCloudResponseHandler("test", (request, result) -> null);

        return handler.buildFailureStatusCodeException(mockRequest, httpResult);
    }
}
