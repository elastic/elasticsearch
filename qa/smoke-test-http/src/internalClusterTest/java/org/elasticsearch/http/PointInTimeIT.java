/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.http;

import org.apache.http.HttpStatus;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class PointInTimeIT extends HttpSmokeTestCase {

    public void testMissingRequiredParameters() {
        // Without index and keep_alive
        {
            Request request = new Request("POST", "/_pit");
            ResponseException error = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
            assertThat(
                error.getMessage(),
                containsString("Validation Failed: 1: [index] is not specified;2: [keep_alive] is not specified;")
            );
            assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
        }
        // Without keep_alive
        {
            Request request = new Request("POST", "logs-*/_pit");
            ResponseException error = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
            assertThat(error.getMessage(), containsString("Validation Failed: 1: [keep_alive] is not specified;"));
            assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
        }
        // Without index
        {
            Request request = new Request("POST", "_pit");
            request.addParameter("keep_alive", "1m");
            ResponseException error = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
            assertThat(error.getMessage(), containsString("Validation Failed: 1: [index] is not specified;"));
            assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
        }
    }
}
