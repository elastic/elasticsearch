/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.RequestTests;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OpenAiChatCompletionResponseHandlerTests extends ESTestCase {

    public void testHandle429InputAndOutputTokensTooLarge_ThrowWithoutRetrying() {
        String responseBody = """
            {
                "error": {
                    "message": "The input or output tokens must be reduced in order to run successfully",
                    "type": "content_too_large",
                    "param": null,
                    "code": null
                }
            }
            """;
        ByteArrayInputStream responseBodyStream = new ByteArrayInputStream(responseBody.getBytes(StandardCharsets.UTF_8));

        var header = mock(Header.class);
        when(header.getElements()).thenReturn(new HeaderElement[] {});

        var statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(429);

        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getFirstHeader(anyString())).thenReturn(header);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);

        var mockRequest = RequestTests.mockRequest("id");
        var httpResult = new HttpResult(httpResponse, responseBodyStream.readAllBytes());
        var handler = new OpenAiChatCompletionResponseHandler("", (request, result) -> null);

        var retryException = expectThrows(RetryException.class, () -> handler.checkForFailureStatusCode(mockRequest, httpResult));

        assertFalse(retryException.shouldRetry());
        assertThat(
            retryException.getCause().getMessage(),
            is(
                "Received a rate limit status code for request from inference entity id [id] status [429]. "
                    + "Error message: [The input or output tokens must be reduced in order to run successfully]"
            )
        );
    }

}
