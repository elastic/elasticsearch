/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.response.ErrorMessageResponseEntity;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;

import java.nio.charset.StandardCharsets;

import static org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler.toRestStatus;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BaseResponseHandlerTests extends ESTestCase {
    public void testToRestStatus_ReturnsBadRequest_WhenStatusIs500() {
        assertThat(toRestStatus(500), is(RestStatus.BAD_REQUEST));
    }

    public void testToRestStatus_ReturnsBadRequest_WhenStatusIs501() {
        assertThat(toRestStatus(501), is(RestStatus.BAD_REQUEST));
    }

    public void testToRestStatus_ReturnsStatusCodeValue_WhenStatusIs200() {
        assertThat(toRestStatus(200), is(RestStatus.OK));
    }

    public void testToRestStatus_ReturnsBadRequest_WhenStatusIsUnknown() {
        assertThat(toRestStatus(1000), is(RestStatus.BAD_REQUEST));
    }

    public void testValidateResponse_DoesNotThrowAnExceptionWhenStatus200_AndNoErrorObject() {
        var handler = getBaseResponseHandler();

        String responseJson = """
            {
              "field": "hello"
            }
            """;

        var response = mock200Response();

        var request = mock(Request.class);
        when(request.getInferenceEntityId()).thenReturn("abc");

        handler.validateResponse(
            mock(ThrottlerManager.class),
            mock(Logger.class),
            request,
            new HttpResult(response, responseJson.getBytes(StandardCharsets.UTF_8)),
            true
        );
    }

    public void testValidateResponse_ThrowsErrorWhenMalformedErrorObjectExists() {
        var handler = getBaseResponseHandler();

        String responseJson = """
            {
              "error": {
                "type": "not_found_error"
              }
            }
            """;

        var response = mock200Response();

        var request = mock(Request.class);
        when(request.getInferenceEntityId()).thenReturn("abc");

        var exception = expectThrows(
            RetryException.class,
            () -> handler.validateResponse(
                mock(ThrottlerManager.class),
                mock(Logger.class),
                request,
                new HttpResult(response, responseJson.getBytes(StandardCharsets.UTF_8)),
                true
            )
        );

        assertFalse(exception.shouldRetry());
        assertThat(
            exception.getCause().getMessage(),
            is("Received an error response for request from inference entity id [abc] status [200]")
        );
    }

    public void testValidateResponse_ThrowsErrorWhenWellFormedErrorObjectExists() {
        var handler = getBaseResponseHandler();

        String responseJson = """
            {
              "error": {
                "type": "not_found_error",
                "message": "a message"
              }
            }
            """;

        var response = mock200Response();

        var request = mock(Request.class);
        when(request.getInferenceEntityId()).thenReturn("abc");

        var exception = expectThrows(
            RetryException.class,
            () -> handler.validateResponse(
                mock(ThrottlerManager.class),
                mock(Logger.class),
                request,
                new HttpResult(response, responseJson.getBytes(StandardCharsets.UTF_8)),
                true
            )
        );

        assertFalse(exception.shouldRetry());
        assertThat(
            exception.getCause().getMessage(),
            is("Received an error response for request from inference entity id [abc] status [200]. Error message: [a message]")
        );
    }

    public void testValidateResponse_DoesNot_ThrowErrorWhenWellFormedErrorObjectExists_WhenCheckForErrorIsFalse() {
        var handler = getBaseResponseHandler();

        String responseJson = """
            {
              "error": {
                "type": "not_found_error",
                "message": "a message"
              }
            }
            """;

        var response = mock200Response();

        var request = mock(Request.class);
        when(request.getInferenceEntityId()).thenReturn("abc");

        handler.validateResponse(
            mock(ThrottlerManager.class),
            mock(Logger.class),
            request,
            new HttpResult(response, responseJson.getBytes(StandardCharsets.UTF_8)),
            false
        );
    }

    private static HttpResponse mock200Response() {
        int statusCode = 200;
        var statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(statusCode);

        var response = mock(HttpResponse.class);
        when(response.getStatusLine()).thenReturn(statusLine);

        return response;
    }

    private static BaseResponseHandler getBaseResponseHandler() {
        return new BaseResponseHandler("abc", (Request request, HttpResult result) -> null, ErrorMessageResponseEntity::fromResponse) {
            @Override
            protected void checkForFailureStatusCode(Request request, HttpResult result) {}
        };
    }
}
