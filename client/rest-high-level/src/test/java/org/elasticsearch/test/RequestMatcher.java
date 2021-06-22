/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.elasticsearch.client.Request;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

/**
 * A Harmcrest matcher for request method and endpoint
 */
public class RequestMatcher extends BaseMatcher<Request> {

    private final String method;
    private final String endpoint;

    public RequestMatcher(String method, String endpoint) {
        this.method = method;
        this.endpoint = endpoint;
    }

    @Override
    public boolean matches(Object actual) {
        if (actual instanceof Request) {
            Request req = (Request) actual;
            return method.equals(req.getMethod()) && endpoint.equals(req.getEndpoint());
        }
        return false;
    }

    @Override
    public void describeTo(Description description) {
        description
            .appendText("request to ")
            .appendText(method)
            .appendText(" ")
            .appendText(endpoint);
    }
}
