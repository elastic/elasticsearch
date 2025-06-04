/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.XContentTestUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.collection.IsMapContaining.hasKey;

/**
 * Tests that by default the error_trace parameter can be used to show stacktraces
 */
public class DetailedErrorsEnabledIT extends HttpSmokeTestCase {

    public void testThatErrorTraceCanBeEnabled() throws IOException {

        try {
            Request request = new Request("DELETE", "/");
            request.addParameter("error_trace", "true");
            getRestClient().performRequest(request);
            fail("request should have failed");
        } catch (ResponseException e) {
            Response response = e.getResponse();
            assertThat(response.getHeader("Content-Type"), containsString("application/json"));

            var jsonNode = XContentTestUtils.createJsonMapView(response.getEntity().getContent());

            assertThat(
                jsonNode.get("error.stack_trace"),
                startsWith("org.elasticsearch.action.ActionRequestValidationException: Validation Failed: 1: index / indices is missing")
            );

            // An ActionRequestValidationException isn't an ElasticsearchException, so when the code tries
            // to work out the root cause, all it actually achieves is wrapping the actual exception in
            // an ElasticsearchException. At least this proves that the root cause logic is executing.
            @SuppressWarnings("unchecked")
            Map<String, Object> cause = (Map<String, Object>) jsonNode.<List<Object>>get("error.root_cause").get(0);
            assertThat(
                cause.get("stack_trace").toString(),
                startsWith("org.elasticsearch.exception.ElasticsearchException$1: Validation Failed: 1: index / indices is missing")
            );
        }
    }

    public void testThatErrorTraceDefaultsToDisabled() throws IOException {

        try {
            getRestClient().performRequest(new Request("DELETE", "/"));
            fail("request should have failed");
        } catch (ResponseException e) {
            Response response = e.getResponse();
            assertThat(response.getHeader("Content-Type"), containsString("application/json"));

            var jsonNode = XContentTestUtils.createJsonMapView(response.getEntity().getContent());

            assertThat("Unexpected .stack_trace in JSON response", jsonNode.get("error.stack_track"), nullValue());
            @SuppressWarnings("unchecked")
            Map<String, Object> cause = (Map<String, Object>) jsonNode.<List<Object>>get("error.root_cause").get(0);
            assertThat("Unexpected .error.root_cause[0].stack_trace in JSON response", cause, not(hasKey("stack_trace")));
        }
    }
}
