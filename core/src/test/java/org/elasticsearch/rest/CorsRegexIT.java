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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.junit.Test;

import static org.elasticsearch.http.netty.NettyHttpServerTransport.SETTING_CORS_ALLOW_ORIGIN;
import static org.elasticsearch.http.netty.NettyHttpServerTransport.SETTING_CORS_ALLOW_CREDENTIALS;
import static org.elasticsearch.http.netty.NettyHttpServerTransport.SETTING_CORS_ENABLED;
import static org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import static org.elasticsearch.test.ESIntegTestCase.Scope;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.is;

/**
 *
 */
@ClusterScope(scope = Scope.SUITE, numDataNodes = 1)
public class CorsRegexIT extends ESIntegTestCase {

    protected static final ESLogger logger = Loggers.getLogger(CorsRegexIT.class);

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(SETTING_CORS_ALLOW_ORIGIN, "/https?:\\/\\/localhost(:[0-9]+)?/")
                .put(SETTING_CORS_ALLOW_CREDENTIALS, true)
                .put(SETTING_CORS_ENABLED, true)
                .put(Node.HTTP_ENABLED, true)
                .build();
    }

    @Test
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

    @Test
    public void testThatRegularExpressionReturnsNullOnNonMatch() throws Exception {
        HttpResponse response = httpClient().method("GET").path("/").addHeader("User-Agent", "Mozilla Bar").addHeader("Origin", "http://evil-host:9200").execute();
        assertResponseWithOriginheader(response, "null");
    }

    @Test
    public void testThatSendingNoOriginHeaderReturnsNoAccessControlHeader() throws Exception {
        HttpResponse response = httpClient().method("GET").path("/").addHeader("User-Agent", "Mozilla Bar").execute();
        assertThat(response.getStatusCode(), is(200));
        assertThat(response.getHeaders(), not(hasKey("Access-Control-Allow-Origin")));
    }

    @Test
    public void testThatRegularExpressionIsNotAppliedWithoutCorrectBrowserOnMatch() throws Exception {
        HttpResponse response = httpClient().method("GET").path("/").execute();
        assertThat(response.getStatusCode(), is(200));
        assertThat(response.getHeaders(), not(hasKey("Access-Control-Allow-Origin")));
    }

    @Test
    public void testThatPreFlightRequestWorksOnMatch() throws Exception {
        String corsValue = "http://localhost:9200";
        HttpResponse response = httpClient().method("OPTIONS").path("/").addHeader("User-Agent", "Mozilla Bar").addHeader("Origin", corsValue).execute();
        assertResponseWithOriginheader(response, corsValue);
    }

    @Test
    public void testThatPreFlightRequestReturnsNullOnNonMatch() throws Exception {
        HttpResponse response = httpClient().method("OPTIONS").path("/").addHeader("User-Agent", "Mozilla Bar").addHeader("Origin", "http://evil-host:9200").execute();
        assertResponseWithOriginheader(response, "null");
    }

    public static void assertResponseWithOriginheader(HttpResponse response, String expectedCorsHeader) {
        assertThat(response.getStatusCode(), is(200));
        assertThat(response.getHeaders(), hasKey("Access-Control-Allow-Origin"));
        assertThat(response.getHeaders().get("Access-Control-Allow-Origin"), is(expectedCorsHeader));
    }
}
