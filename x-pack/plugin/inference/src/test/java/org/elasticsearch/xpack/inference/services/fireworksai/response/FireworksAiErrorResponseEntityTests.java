/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai.response;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FireworksAiErrorResponseEntityTests extends ESTestCase {

    // Common status line strings
    private static final String BAD_REQUEST_STATUS = "HTTP/1.1 400 Bad Request";
    private static final String UNAUTHORIZED_STATUS = "HTTP/1.1 401 Unauthorized";
    private static final String RATE_LIMIT_STATUS = "HTTP/1.1 429 Too Many Requests";
    private static final String SERVER_ERROR_STATUS = "HTTP/1.1 500 Internal Server Error";

    public void testFromResponse_ReturnsStatusLineAsErrorMessage() {
        // Test that various HTTP error status lines are correctly returned as error messages
        assertErrorMessage(BAD_REQUEST_STATUS);
        assertErrorMessage(UNAUTHORIZED_STATUS);
        assertErrorMessage(RATE_LIMIT_STATUS);
        assertErrorMessage(SERVER_ERROR_STATUS);
    }

    private void assertErrorMessage(String expectedStatusLine) {
        var statusLine = mock(StatusLine.class);
        when(statusLine.toString()).thenReturn(expectedStatusLine);

        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);

        var httpResult = new HttpResult(httpResponse, new byte[0]);

        ErrorResponse errorResponse = FireworksAiErrorResponseEntity.fromResponse(httpResult);

        assertThat(errorResponse.getErrorMessage(), is(expectedStatusLine));
    }
}
