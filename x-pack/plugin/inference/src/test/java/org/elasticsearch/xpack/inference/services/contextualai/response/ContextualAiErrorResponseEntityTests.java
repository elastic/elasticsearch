/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai.response;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ContextualAiErrorResponseEntityTests extends ESTestCase {

    public void testFromResponse() {
        var statusLine = mock(StatusLine.class);
        when(statusLine.toString()).thenReturn("HTTP/1.1 400 Bad Request");

        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);

        var httpResult = new HttpResult(httpResponse, new byte[0]);

        ErrorResponse errorResponse = ContextualAiErrorResponseEntity.fromResponse(httpResult);

        assertThat(errorResponse.getErrorMessage(), is("HTTP/1.1 400 Bad Request"));
    }

    public void testFromResponse_ServerError() {
        var statusLine = mock(StatusLine.class);
        when(statusLine.toString()).thenReturn("HTTP/1.1 500 Internal Server Error");

        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);

        var httpResult = new HttpResult(httpResponse, new byte[0]);

        ErrorResponse errorResponse = ContextualAiErrorResponseEntity.fromResponse(httpResult);

        assertThat(errorResponse.getErrorMessage(), is("HTTP/1.1 500 Internal Server Error"));
    }
}
