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
                "application/json; charset=UTF-8",
                "\"error\":\"no handler found for uri [/foo/bar/baz/qux/quux] and method [GET]\"");
        runTestNoHandlerRespectsAcceptHeader(
                "application/yaml",
                "application/yaml",
                "error: \"no handler found for uri [/foo/bar/baz/qux/quux] and method [GET]\"");
    }

    private void runTestNoHandlerRespectsAcceptHeader(
            final String accept, final String contentType, final String expect) throws IOException {
        Request request = new Request("GET", "/foo/bar/baz/qux/quux");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("Accept", accept);
        request.setOptions(options);
        final ResponseException e = expectThrows(ResponseException.class,
                        () -> getRestClient().performRequest(request));

        final Response response = e.getResponse();
        assertThat(response.getHeader("Content-Type"), equalTo(contentType));
        assertThat(EntityUtils.toString(e.getResponse().getEntity()), containsString(expect));
        assertThat(response.getStatusLine().getStatusCode(), is(400));
    }

}
