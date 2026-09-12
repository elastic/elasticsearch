/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.message.BasicHttpResponse;
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

        var response = new BasicHttpResponse(500);
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

        var response = new BasicHttpResponse(500);
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
        return new BasicHttpResponse(200);
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
