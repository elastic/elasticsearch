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
package org.elasticsearch.rest;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_CREDENTIALS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_METHODS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ENABLED;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * Test CORS where the allow origin value is a regular expression.
 */
@ClusterScope(scope = Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 1)
public class CorsRegexIT extends ESIntegTestCase {

    protected static final ESLogger logger = Loggers.getLogger(CorsRegexIT.class);

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), "/https?:\\/\\/localhost(:[0-9]+)?/")
                .put(SETTING_CORS_ALLOW_CREDENTIALS.getKey(), true)
                .put(SETTING_CORS_ALLOW_METHODS.getKey(), "get, options, post")
                .put(SETTING_CORS_ENABLED.getKey(), true)
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                .build();
    }

    public void testThatRegularExpressionWorksOnMatch() throws Exception {
        String corsValue = "http://localhost:9200";
        HttpResponse response = httpClient().method("GET").path("/").addHeader("User-Agent", "Mozilla Bar").addHeader("Origin", corsValue).execute();
        assertResponseWithOriginheader(response, corsValue);

        corsValue = "https://localhost:9200";
        response = httpClient().method("GET").path("/").addHeader("User-Agent", "Mozilla Bar").addHeader("Origin", corsValue).execute();
        assertResponseWithOriginheader(response, corsValue);
        assertThat(response.getHeaders(), hasKey("Access-Control-Allow-Credentials"));
        assertThat(response.getHeaders().get("Access-Control-Allow-Credentials"), is("true"));
    }

    public void testThatRegularExpressionReturnsForbiddenOnNonMatch() throws Exception {
        HttpResponse response = httpClient().method("GET").path("/").addHeader("User-Agent", "Mozilla Bar").addHeader("Origin", "http://evil-host:9200").execute();
        // a rejected origin gets a FORBIDDEN - 403
        assertThat(response.getStatusCode(), is(403));
        assertThat(response.getHeaders(), not(hasKey("Access-Control-Allow-Origin")));
    }

    public void testThatSendingNoOriginHeaderReturnsNoAccessControlHeader() throws Exception {
        HttpResponse response = httpClient().method("GET").path("/").addHeader("User-Agent", "Mozilla Bar").execute();
        assertThat(response.getStatusCode(), is(200));
        assertThat(response.getHeaders(), not(hasKey("Access-Control-Allow-Origin")));
    }

    public void testThatRegularExpressionIsNotAppliedWithoutCorrectBrowserOnMatch() throws Exception {
        HttpResponse response = httpClient().method("GET").path("/").execute();
        assertThat(response.getStatusCode(), is(200));
        assertThat(response.getHeaders(), not(hasKey("Access-Control-Allow-Origin")));
    }

    public void testThatPreFlightRequestWorksOnMatch() throws Exception {
        String corsValue = "http://localhost:9200";
        HttpResponse response = httpClient().method("OPTIONS")
                                    .path("/")
                                    .addHeader("User-Agent", "Mozilla Bar")
                                    .addHeader("Origin", corsValue)
                                    .addHeader(HttpHeaders.Names.ACCESS_CONTROL_REQUEST_METHOD, "GET")
                                    .execute();
        assertResponseWithOriginheader(response, corsValue);
        assertThat(response.getHeaders(), hasKey("Access-Control-Allow-Methods"));
    }

    public void testThatPreFlightRequestReturnsNullOnNonMatch() throws Exception {
        HttpResponse response = httpClient().method("OPTIONS")
                                    .path("/")
                                    .addHeader("User-Agent", "Mozilla Bar")
                                    .addHeader("Origin", "http://evil-host:9200")
                                    .addHeader(HttpHeaders.Names.ACCESS_CONTROL_REQUEST_METHOD, "GET")
                                    .execute();
        // a rejected origin gets a FORBIDDEN - 403
        assertThat(response.getStatusCode(), is(403));
        assertThat(response.getHeaders(), not(hasKey("Access-Control-Allow-Origin")));
        assertThat(response.getHeaders(), not(hasKey("Access-Control-Allow-Methods")));
    }

    protected static void assertResponseWithOriginheader(HttpResponse response, String expectedCorsHeader) {
        assertThat(response.getStatusCode(), is(200));
        assertThat(response.getHeaders(), hasKey("Access-Control-Allow-Origin"));
        assertThat(response.getHeaders().get("Access-Control-Allow-Origin"), is(expectedCorsHeader));
    }

}
