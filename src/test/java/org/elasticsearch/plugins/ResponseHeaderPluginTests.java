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
package org.elasticsearch.plugins;

import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.elasticsearch.plugins.responseheader.TestResponseHeaderPlugin;
import org.junit.Test;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.hamcrest.Matchers.equalTo;

/**
 * Test a rest action that sets special response headers
 */
@ClusterScope(scope = Scope.SUITE, numDataNodes = 1)
public class ResponseHeaderPluginTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("plugin.types", TestResponseHeaderPlugin.class.getName())
                .put("force.http.enabled", true)
                .build();
    }

    @Test
    public void testThatSettingHeadersWorks() throws Exception {
        ensureGreen();
        HttpResponse response = httpClient().method("GET").path("/_protected").execute();
        assertThat(response.getStatusCode(), equalTo(RestStatus.UNAUTHORIZED.getStatus()));
        assertThat(response.getHeaders().get("Secret"), equalTo("required"));

        HttpResponse authResponse = httpClient().method("GET").path("/_protected").addHeader("Secret", "password").execute();
        assertThat(authResponse.getStatusCode(), equalTo(RestStatus.OK.getStatus()));
        assertThat(authResponse.getHeaders().get("Secret"), equalTo("granted"));
    }

    private HttpRequestBuilder httpClient() {
        return new HttpRequestBuilder(HttpClients.createDefault()).httpTransport(internalCluster().getDataNodeInstance(HttpServerTransport.class));
    }
}