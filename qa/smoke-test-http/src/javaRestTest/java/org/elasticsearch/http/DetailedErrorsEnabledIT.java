/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

/**
 * Tests that by default the error_trace parameter can be used to show stacktraces
 */
public class DetailedErrorsEnabledIT extends HttpSmokeTestCase {

    public void testThatErrorTraceWorksByDefault() throws IOException {
        try {
            Request request = new Request("DELETE", "/");
            request.addParameter("error_trace", "true");
            getRestClient().performRequest(request);
            fail("request should have failed");
        } catch (ResponseException e) {
            Response response = e.getResponse();
            assertThat(response.getHeader("Content-Type"), containsString("application/json"));
            assertThat(
                EntityUtils.toString(response.getEntity()),
                containsString(
                    "\"stack_trace\":\"[Validation Failed: 1: index / indices is missing;]; "
                        + "nested: ActionRequestValidationException[Validation Failed: 1:"
                )
            );
        }

        try {
            getRestClient().performRequest(new Request("DELETE", "/"));
            fail("request should have failed");
        } catch (ResponseException e) {
            Response response = e.getResponse();
            assertThat(response.getHeader("Content-Type"), containsString("application/json; charset=UTF-8"));
            assertThat(
                EntityUtils.toString(response.getEntity()),
                not(
                    containsString(
                        "\"stack_trace\":\"[Validation Failed: 1: index / indices is missing;]; "
                            + "nested: ActionRequestValidationException[Validation Failed: 1:"
                    )
                )
            );
        }
    }
}
