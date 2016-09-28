/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.http;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

/**
 * Tests that by default the error_trace parameter can be used to show stacktraces
 */
public class DetailedErrorsEnabledIT extends HttpSmokeTestCase {

    public void testThatErrorTraceWorksByDefault() throws IOException {
        try {
            getRestClient().performRequest("DELETE", "/", Collections.singletonMap("error_trace", "true"));
            fail("request should have failed");
        } catch(ResponseException e) {
            Response response = e.getResponse();
            assertThat(response.getHeader("Content-Type"), containsString("application/json"));
            assertThat(EntityUtils.toString(response.getEntity()),
                    containsString("\"stack_trace\":\"[Validation Failed: 1: index / indices is missing;]; " +
                    "nested: ActionRequestValidationException[Validation Failed: 1:"));
        }

        try {
            getRestClient().performRequest("DELETE", "/");
            fail("request should have failed");
        } catch(ResponseException e) {
            Response response = e.getResponse();
            assertThat(response.getHeader("Content-Type"), containsString("application/json"));
            assertThat(EntityUtils.toString(response.getEntity()),
                    not(containsString("\"stack_trace\":\"[Validation Failed: 1: index / indices is missing;]; "
                    + "nested: ActionRequestValidationException[Validation Failed: 1:")));
        }
    }
}
