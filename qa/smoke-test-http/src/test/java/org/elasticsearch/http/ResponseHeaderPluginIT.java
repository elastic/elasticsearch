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

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.Matchers.equalTo;

/**
 * Test a rest action that sets special response headers
 */
@ClusterScope(scope = Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 1)
public class ResponseHeaderPluginIT extends HttpSmokeTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestResponseHeaderPlugin.class);
        return plugins;
    }

    public void testThatSettingHeadersWorks() throws IOException {
        ensureGreen();
        try {
            getRestClient().performRequest(new Request("GET", "/_protected"));
            fail("request should have failed");
        } catch(ResponseException e) {
            Response response = e.getResponse();
            assertThat(response.getStatusLine().getStatusCode(), equalTo(401));
            assertThat(response.getHeader("Secret"), equalTo("required"));
        }

        Request request = new Request("GET", "/_protected");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("Secret", "password");
        request.setOptions(options);
        Response authResponse = getRestClient().performRequest(request);
        assertThat(authResponse.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(authResponse.getHeader("Secret"), equalTo("granted"));
    }
}
