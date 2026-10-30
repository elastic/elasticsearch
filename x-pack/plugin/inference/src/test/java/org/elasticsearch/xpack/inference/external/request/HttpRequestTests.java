/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request;

import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;

public class HttpRequestTests {
    /**
     * Creates a placeholder {@link HttpRequest} for tests that never execute the request. A real
     * {@link org.apache.hc.client5.http.async.methods.SimpleHttpRequest} is used because the class is final and cannot be mocked.
     */
    public static HttpRequest createMock(String modelId) {
        return new HttpRequest(SimpleRequestBuilder.post("http://localhost:12345").build(), modelId);
    }
}
