/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;

/**
 * Tests that by default the error_trace parameter can be used to show stacktraces
 */
public class DetailedErrorsEnabledIT extends HttpSmokeTestCase {

    public void testThatErrorTraceCanBeEnabled() throws IOException {
        ObjectMapper mapper = new ObjectMapper();

        try {
            Request request = new Request("DELETE", "/");
            request.addParameter("error_trace", "true");
            getRestClient().performRequest(request);
            fail("request should have failed");
        } catch (ResponseException e) {
            Response response = e.getResponse();
            assertThat(response.getHeader("Content-Type"), containsString("application/json"));

            JsonNode jsonNode = mapper.readTree(response.getEntity().getContent());

            assertThat(
                jsonNode.get("error").get("stack_trace").asText(),
                startsWith("org.elasticsearch.action.ActionRequestValidationException: Validation Failed: 1: index / indices is missing"));

            // An ActionRequestValidationException isn't an ElasticsearchException, so when the code tries
            // to work out the root cause, all it actually achieves is wrapping the actual exception in
            // an ElasticsearchException. At least this proves that the root cause logic is executing.
            assertThat(
                jsonNode.get("error").get("root_cause").get(0).get("stack_trace").asText(),
                startsWith("org.elasticsearch.ElasticsearchException$1: Validation Failed: 1: index / indices is missing"));
        }
    }

    public void testThatErrorTraceDefaultsToDisabled() throws IOException {

        try {
            getRestClient().performRequest(new Request("DELETE", "/"));
            fail("request should have failed");
        } catch (ResponseException e) {
            Response response = e.getResponse();
            assertThat(response.getHeader("Content-Type"), containsString("application/json"));

            ObjectMapper mapper = new ObjectMapper();
            JsonNode jsonNode = mapper.readTree(response.getEntity().getContent());

            assertFalse("Unexpected .stack_trace in JSON response", jsonNode.get("error").has("stack_trace"));
            assertFalse(
                "Unexpected .error.root_cause[0].stack_trace in JSON response",
                jsonNode.get("error").get("root_cause").get(0).has("stack_trace"));
        }
    }
}
