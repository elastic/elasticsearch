/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.test.ESTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RequestTests {

    private RequestTests() {}

    public static Request mockRequest(String modelId) {
        var request = mock(Request.class);
        when(request.getInferenceEntityId()).thenReturn(modelId);

        return request;
    }

    /**
     * Synchronously obtain the {@link HttpRequest} from {@code request} by calling
     * {@link Request#createHttpRequest} and blocking on the result. For use in tests that
     * assert on the built HTTP request without restructuring to async.
     *
     * @param request the request to build
     * @return the built HTTP request
     */
    public static HttpRequest getHttpRequestSync(Request request) {
        var future = new PlainActionFuture<HttpRequest>();
        request.createHttpRequest(future);
        return future.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
    }
}
