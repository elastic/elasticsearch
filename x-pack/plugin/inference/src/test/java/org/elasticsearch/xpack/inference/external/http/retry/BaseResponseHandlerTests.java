/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.external.response.ErrorMessageResponseEntity;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler.toRestStatus;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BaseResponseHandlerTests extends ESTestCase {

    private static final String INFERENCE_ID = "id";

    public void testToRestStatus_ReturnsBadRequest_WhenStatusIs500() {
        assertThat(toRestStatus(500), is(RestStatus.BAD_REQUEST));
    }

    public void testConstructNonStreamingException_AppendsErrorMessage_WhenErrorStructureFound() {
        var exception = BaseResponseHandler.constructNonStreamingException(
            BaseResponseHandler.BAD_REQUEST,
            mockRequest(),
            httpResult(400),
            new ErrorResponse("some error")
        );

        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(
            exception.getMessage(),
            is("Received a bad request status code for request from inference entity id [id] status [400]. Error message: [some error]")
        );
    }

    public void testConstructNonStreamingException_OmitsErrorMessage_WhenErrorStructureNotFound() {
        var exception = BaseResponseHandler.constructNonStreamingException(
            BaseResponseHandler.BAD_REQUEST,
            mockRequest(),
            httpResult(400),
            ErrorResponse.UNDEFINED_ERROR
        );

        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(exception.getMessage(), is("Received a bad request status code for request from inference entity id [id] status [400]"));
    }

    public void testConstructNonStreamingException_OmitsErrorMessage_WhenErrorResponseIsNull() {
        var exception = BaseResponseHandler.constructNonStreamingException(
            BaseResponseHandler.SERVER_ERROR,
            mockRequest(),
            httpResult(500),
            null
        );

        // toRestStatus maps anything >= 500 onto BAD_REQUEST
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(
            exception.getMessage(),
            is("Received a server error status code for request from inference entity id [id] status [500]")
        );
    }

    private static OutboundRequest mockRequest() {
        var outboundRequest = mock(OutboundRequest.class);
        when(outboundRequest.getInferenceEntityId()).thenReturn(INFERENCE_ID);
        return outboundRequest;
    }

    private static HttpResult httpResult(int statusCode) {
        var statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(statusCode);

        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);

        return new HttpResult(httpResponse, new byte[0]);
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

    public void testValidateResponse_SkipsBuildFailureStatusCodeException_WhenResponseIsSuccessful() {
        var handler = new BaseResponseHandler(
            "test",
            (OutboundRequest outboundRequest, HttpResult result) -> null,
            ErrorMessageResponseEntity::fromResponse
        ) {
            @Override
            public RetryException buildFailureStatusCodeException(OutboundRequest outboundRequest, HttpResult result) {
                return new RetryException(false, new RuntimeException("should not be called"));
            }
        };

        var response = mock200Response();
        var request = mock(OutboundRequest.class);
        when(request.getInferenceEntityId()).thenReturn("test-id");

        // 200 → buildFailureStatusCodeException must not be called
        handler.validateResponse(
            mock(ThrottlerManager.class),
            mock(Logger.class),
            request,
            new HttpResult(response, "{}".getBytes(StandardCharsets.UTF_8))
        );
    }

    public void testValidateResponse_CallsBuildFailureStatusCodeException_WhenResponseIsNotSuccessful() {
        var handlerCalled = new AtomicBoolean(false);
        var handler = new BaseResponseHandler(
            "test",
            (OutboundRequest outboundRequest, HttpResult result) -> null,
            ErrorMessageResponseEntity::fromResponse
        ) {
            @Override
            public RetryException buildFailureStatusCodeException(OutboundRequest outboundRequest, HttpResult result) {
                handlerCalled.set(true);
                return new RetryException(false, new RuntimeException("failure"));
            }
        };

        var statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(500);
        var response = mock(HttpResponse.class);
        when(response.getStatusLine()).thenReturn(statusLine);
        var request = mock(OutboundRequest.class);
        when(request.getInferenceEntityId()).thenReturn("test-id");

        expectThrows(
            RetryException.class,
            () -> handler.validateResponse(
                mock(ThrottlerManager.class),
                mock(Logger.class),
                request,
                new HttpResult(response, "{}".getBytes(StandardCharsets.UTF_8))
            )
        );
        assertTrue(handlerCalled.get());
    }

    public void testValidateResponse_DoesNotThrowAnExceptionWhenStatus200_AndNoErrorObject() {
        var handler = getBaseResponseHandler();

        String responseJson = """
            {
              "field": "hello"
            }
            """;

        var response = mock200Response();

        var request = mock(OutboundRequest.class);
        when(request.getInferenceEntityId()).thenReturn("abc");

        handler.validateResponse(
            mock(ThrottlerManager.class),
            mock(Logger.class),
            request,
            new HttpResult(response, responseJson.getBytes(StandardCharsets.UTF_8))
        );
    }

    public void testValidateResponse_DoesNotThrowError_WhenStatus200_AndMalformedErrorObject() {
        var handler = getBaseResponseHandler();

        String responseJson = """
            {
              "error": {
                "type": "not_found_error"
              }
            }
            """;

        var response = mock200Response();

        var request = mock(OutboundRequest.class);
        when(request.getInferenceEntityId()).thenReturn("abc");

        handler.validateResponse(
            mock(ThrottlerManager.class),
            mock(Logger.class),
            request,
            new HttpResult(response, responseJson.getBytes(StandardCharsets.UTF_8))
        );
    }

    public void testValidateResponse_DoesNotThrow_WhenStatus200_AndWellFormedErrorObjectExists() {
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

        var request = mock(OutboundRequest.class);
        when(request.getInferenceEntityId()).thenReturn("abc");

        handler.validateResponse(
            mock(ThrottlerManager.class),
            mock(Logger.class),
            request,
            new HttpResult(response, responseJson.getBytes(StandardCharsets.UTF_8))
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

        var request = mock(OutboundRequest.class);
        when(request.getInferenceEntityId()).thenReturn("abc");

        handler.validateResponse(
            mock(ThrottlerManager.class),
            mock(Logger.class),
            request,
            new HttpResult(response, responseJson.getBytes(StandardCharsets.UTF_8))
        );
    }

    public void testValidateResponse_UsesBuildFailureStatusCodeException_WhenResponseIsNotSuccessful_AndBodyIsEmpty() {
        var handler = new BaseResponseHandler(
            "test",
            (OutboundRequest outboundRequest, HttpResult result) -> null,
            ErrorMessageResponseEntity::fromResponse
        ) {
            @Override
            public RetryException buildFailureStatusCodeException(OutboundRequest outboundRequest, HttpResult result) {
                return new RetryException(false, new RuntimeException("failure exception"));
            }
        };

        var statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(500);
        var response = mock(HttpResponse.class);
        when(response.getStatusLine()).thenReturn(statusLine);
        var request = mock(OutboundRequest.class);
        when(request.getInferenceEntityId()).thenReturn("test-id");

        // An empty body must not suppress the failure-status exception; buildFailureStatusCodeException wins.
        var thrownException = expectThrows(
            RetryException.class,
            () -> handler.validateResponse(mock(ThrottlerManager.class), mock(Logger.class), request, new HttpResult(response, new byte[0]))
        );
        assertThat(thrownException.getCause().getMessage(), is("failure exception"));
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
        return new BaseResponseHandler(
            "abc",
            (OutboundRequest outboundRequest, HttpResult result) -> null,
            ErrorMessageResponseEntity::fromResponse
        ) {
            @Override
            public RetryException buildFailureStatusCodeException(OutboundRequest outboundRequest, HttpResult result) {
                return new RetryException(false, new RuntimeException("failure"));
            }
        };
    }
}
