/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.request.Request;

import static org.elasticsearch.xpack.inference.external.http.HttpUtils.checkForEmptyBody;
import static org.elasticsearch.xpack.inference.external.http.HttpUtils.checkForFailureStatusCode;
import static org.elasticsearch.xpack.inference.external.request.RequestTests.mockRequest;
import static org.elasticsearch.xpack.inference.logging.ThrottlerManagerTests.mockThrottlerManager;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HttpUtilsTests extends ESTestCase {
    public void testCheckForFailureStatusCode_ThrowsWhenStatusCodeIs300() {
        var statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(300);

        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);

        var result = new HttpResult(httpResponse, new byte[0]);

        var thrownException = expectThrows(
            IllegalStateException.class,
            () -> checkForFailureStatusCode(mockThrottlerManager(), mock(Logger.class), mockRequest("id"), result)
        );

        assertThat(thrownException.getMessage(), is("Unhandled redirection for request from inference entity id [id] status [300]"));
    }

    public void testCheckForFailureStatusCode_DoesNotThrowWhenStatusCodeIs200() {
        var statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(200);

        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);

        var result = new HttpResult(httpResponse, new byte[0]);

        checkForFailureStatusCode(mockThrottlerManager(), mock(Logger.class), mock(Request.class), result);
    }

    public void testCheckForEmptyBody_DoesNotThrowWhenTheBodyIsNotEmpty() {
        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getStatusLine()).thenReturn(mock(StatusLine.class));

        var result = new HttpResult(httpResponse, new byte[] { 'a' });

        checkForEmptyBody(mockThrottlerManager(), mock(Logger.class), mock(Request.class), result);
    }

    public void testCheckForEmptyBody_ThrowsWhenTheBodyIsEmpty() {
        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getStatusLine()).thenReturn(mock(StatusLine.class));

        var result = new HttpResult(httpResponse, new byte[0]);

        var thrownException = expectThrows(
            IllegalStateException.class,
            () -> checkForEmptyBody(mockThrottlerManager(), mock(Logger.class), mockRequest("id"), result)
        );

        assertThat(thrownException.getMessage(), is("Response body was empty for request from inference entity id [id]"));
    }
}
