/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request;

import org.apache.http.client.methods.HttpRequestBase;

import static org.mockito.Mockito.mock;

public class HttpRequestTests {
    public static HttpRequest createMock(String modelId) {
        return new HttpRequest(mock(HttpRequestBase.class), modelId);
    }
}
