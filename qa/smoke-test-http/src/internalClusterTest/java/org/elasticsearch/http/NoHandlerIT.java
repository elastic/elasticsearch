/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class NoHandlerIT extends HttpSmokeTestCase {

    public void testNoHandlerRespectsAcceptHeader() throws IOException {
        runTestNoHandlerRespectsAcceptHeader(
            "application/json",
            "application/json",
            "\"error\":\"no handler found for uri [/foo/bar/baz/qux/quux] and method [GET]\""
        );
        runTestNoHandlerRespectsAcceptHeader(
            "application/yaml",
            "application/yaml",
            "error: \"no handler found for uri [/foo/bar/baz/qux/quux] and method [GET]\""
        );
    }

    private void runTestNoHandlerRespectsAcceptHeader(final String accept, final String contentType, final String expect)
        throws IOException {
        Request request = new Request("GET", "/foo/bar/baz/qux/quux");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("Accept", accept);
        request.setOptions(options);
        final ResponseException e = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));

        final Response response = e.getResponse();
        assertThat(response.getHeader("Content-Type"), equalTo(contentType));
        assertThat(EntityUtils.toString(e.getResponse().getEntity()), containsString(expect));
        assertThat(response.getStatusLine().getStatusCode(), is(400));
    }

}
