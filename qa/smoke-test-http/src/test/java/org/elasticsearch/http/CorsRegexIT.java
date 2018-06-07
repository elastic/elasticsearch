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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.io.IOException;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_CREDENTIALS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_METHODS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ENABLED;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Test CORS where the allow origin value is a regular expression.
 */
@ClusterScope(scope = Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 1)
public class CorsRegexIT extends HttpSmokeTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), "/https?:\\/\\/localhost(:[0-9]+)?/")
                .put(SETTING_CORS_ALLOW_CREDENTIALS.getKey(), true)
                .put(SETTING_CORS_ALLOW_METHODS.getKey(), "get, options, post")
                .put(SETTING_CORS_ENABLED.getKey(), true)
                .build();
    }

    public void testThatRegularExpressionWorksOnMatch() throws IOException {
        {
            String corsValue = "http://localhost:9200";
            Request request = new Request("GET", "/");
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeader("User-Agent", "Mozilla Bar");
            options.addHeader("Origin", corsValue);
            request.setOptions(options);
            Response response = getRestClient().performRequest(request);
            assertResponseWithOriginHeader(response, corsValue);
        }
        {
            String corsValue = "https://localhost:9201";
            Request request = new Request("GET", "/");
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeader("User-Agent", "Mozilla Bar");
            options.addHeader("Origin", corsValue);
            request.setOptions(options);
            Response response = getRestClient().performRequest(request);
            assertResponseWithOriginHeader(response, corsValue);
            assertThat(response.getHeader("Access-Control-Allow-Credentials"), is("true"));
        }
    }

    public void testThatRegularExpressionReturnsForbiddenOnNonMatch() throws IOException {
        Request request = new Request("GET", "/");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("User-Agent", "Mozilla Bar");
        options.addHeader("Origin", "http://evil-host:9200");
        request.setOptions(options);
        try {
            getRestClient().performRequest(request);
            fail("request should have failed");
        } catch(ResponseException e) {
            Response response = e.getResponse();
            // a rejected origin gets a FORBIDDEN - 403
            assertThat(response.getStatusLine().getStatusCode(), is(403));
            assertThat(response.getHeader("Access-Control-Allow-Origin"), nullValue());
        }
    }

    public void testThatSendingNoOriginHeaderReturnsNoAccessControlHeader() throws IOException {
        Request request = new Request("GET", "/");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("User-Agent", "Mozilla Bar");
        request.setOptions(options);
        Response response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        assertThat(response.getHeader("Access-Control-Allow-Origin"), nullValue());
    }

    public void testThatRegularExpressionIsNotAppliedWithoutCorrectBrowserOnMatch() throws IOException {
        Response response = getRestClient().performRequest(new Request("GET", "/"));
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        assertThat(response.getHeader("Access-Control-Allow-Origin"), nullValue());
    }

    public void testThatPreFlightRequestWorksOnMatch() throws IOException {
        String corsValue = "http://localhost:9200";
        Request request = new Request("OPTIONS", "/");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("User-Agent", "Mozilla Bar");
        options.addHeader("Origin", corsValue);
        options.addHeader("Access-Control-Request-Method", "GET");
        request.setOptions(options);
        Response response = getRestClient().performRequest(request);
        assertResponseWithOriginHeader(response, corsValue);
        assertNotNull(response.getHeader("Access-Control-Allow-Methods"));
    }

    public void testThatPreFlightRequestReturnsNullOnNonMatch() throws IOException {
        String corsValue = "http://evil-host:9200";
        Request request = new Request("OPTIONS", "/");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("User-Agent", "Mozilla Bar");
        options.addHeader("Origin", corsValue);
        options.addHeader("Access-Control-Request-Method", "GET");
        request.setOptions(options);
        try {
            getRestClient().performRequest(request);
            fail("request should have failed");
        } catch(ResponseException e) {
            Response response = e.getResponse();
            // a rejected origin gets a FORBIDDEN - 403
            assertThat(response.getStatusLine().getStatusCode(), is(403));
            assertThat(response.getHeader("Access-Control-Allow-Origin"), nullValue());
            assertThat(response.getHeader("Access-Control-Allow-Methods"), nullValue());
        }
    }

    private static void assertResponseWithOriginHeader(Response response, String expectedCorsHeader) {
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        assertThat(response.getHeader("Access-Control-Allow-Origin"), is(expectedCorsHeader));
    }
}
