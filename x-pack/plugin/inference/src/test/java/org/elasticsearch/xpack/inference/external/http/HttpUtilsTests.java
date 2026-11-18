/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.apache.hc.core5.http.message.BasicHttpResponse;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;

import static org.elasticsearch.xpack.inference.external.http.HttpUtils.checkForEmptyBody;
import static org.elasticsearch.xpack.inference.external.http.HttpUtils.checkForFailureStatusCode;
import static org.elasticsearch.xpack.inference.external.request.RequestTests.mockRequest;
import static org.elasticsearch.xpack.inference.logging.ThrottlerManagerTests.mockThrottlerManager;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class HttpUtilsTests extends ESTestCase {
    public void testCheckForFailureStatusCode_ThrowsWhenStatusCodeIs300() {
        var result = new HttpResult(new BasicHttpResponse(300), new byte[0]);

        var thrownException = expectThrows(
            IllegalStateException.class,
            () -> checkForFailureStatusCode(mockThrottlerManager(), mock(Logger.class), mockRequest("id"), result)
        );

        assertThat(thrownException.getMessage(), is("Unhandled redirection for request from inference entity id [id] status [300]"));
    }

    public void testCheckForFailureStatusCode_DoesNotThrowWhenStatusCodeIs200() {
        var result = new HttpResult(new BasicHttpResponse(200), new byte[0]);

        checkForFailureStatusCode(mockThrottlerManager(), mock(Logger.class), mock(OutboundRequest.class), result);
    }

    public void testCheckForEmptyBody_DoesNotThrowWhenTheBodyIsNotEmpty() {
        var result = new HttpResult(new BasicHttpResponse(200), new byte[] { 'a' });

        checkForEmptyBody(mockThrottlerManager(), mock(Logger.class), mock(OutboundRequest.class), result);
    }

    public void testCheckForEmptyBody_ThrowsWhenTheBodyIsEmpty() {
        var result = new HttpResult(new BasicHttpResponse(200), new byte[0]);

        var thrownException = expectThrows(
            IllegalStateException.class,
            () -> checkForEmptyBody(mockThrottlerManager(), mock(Logger.class), mockRequest("id"), result)
        );

        assertThat(thrownException.getMessage(), is("Response body was empty for request from inference entity id [id]"));
    }
}
