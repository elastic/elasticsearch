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

public class FieldCapabilitiesIT extends HttpSmokeTestCase {

    public void testProjectRoutingInNonCrossProjectEnv() {
        // With project_routing as query param in non-CPS env
        {
            Request request = new Request("POST", "_field_caps?fields=*");
            request.setJsonEntity("""
                { "project_routing": "_alias:_origin" }
                """);
            ResponseException error = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
            assertThat(error.getMessage(), containsString("request [_field_caps] contains unrecognized parameter: [project_routing]"));
            assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
        }
        // With project_routing in body in non-CPS env
        {
            Request request = new Request("POST", "_field_caps?fields=*");
            request.setJsonEntity("""
                {
                    "project_routing": "_alias:_origin"
                }
                """);
            ResponseException error = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
            assertThat(error.getMessage(), containsString("Unknown key for a VALUE_STRING in [project_routing]"));
            assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
        }
    }
}
