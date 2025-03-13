/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class InferenceLicenseBaseRestTest extends ESRestTestCase {
    protected void sendRestrictedRequest(String method, String endpoint, String body) throws IOException {
        var request = new Request(method, endpoint);
        request.setJsonEntity(body);

        var exception = assertThrows(ResponseException.class, () -> client().performRequest(request));
        assertEquals(403, exception.getResponse().getStatusLine().getStatusCode());
        assertThat(exception.getMessage(), containsString("current license is non-compliant for [inference]"));
    }

    protected void sendNonRestrictedRequest(String method, String endpoint, String body, int expectedStatusCode, boolean exceptionExpected)
        throws IOException {
        var request = new Request(method, endpoint);
        request.setJsonEntity(body);

        int actualStatusCode;
        if (exceptionExpected) {
            var exception = assertThrows(ResponseException.class, () -> client().performRequest(request));
            actualStatusCode = exception.getResponse().getStatusLine().getStatusCode();
        } else {
            var response = client().performRequest(request);
            actualStatusCode = response.getStatusLine().getStatusCode();
        }
        assertEquals(expectedStatusCode, actualStatusCode);
    }
}
