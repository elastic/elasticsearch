/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugin;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.plugin.httprequest.TestHttpRequestPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.helper.HttpClient;
import org.elasticsearch.rest.helper.HttpClientResponse;
import org.elasticsearch.test.AbstractIntegrationTest;
import org.elasticsearch.test.AbstractIntegrationTest.ClusterScope;
import org.elasticsearch.test.AbstractIntegrationTest.Scope;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Test a rest action that casts RestRequest to HttpRequest and call some new methods (mainly for client identification)
 */
@ClusterScope(scope=Scope.SUITE, numNodes=1)
public class HttpRequestPluginTests extends AbstractIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder().put("plugin.types", TestHttpRequestPlugin.class.getName()).put(super.nodeSettings(nodeOrdinal)).build();
    }
    
    @Test
    public void testHttpRequestMethods() throws Exception {
        ensureGreen();
        Map<String,String> headers = new HashMap<String,String>();
        headers.put("X-Opaque-Id","veryOpaque");        
        
        HttpClientResponse response = httpClient().request("GET","/_httprequestinfo",headers);
        logger.info(response.getHeaders().toString());
        assertThat(response.errorCode(), equalTo(RestStatus.OK.getStatus()));
        assertThat(response.getHeader("opaqueId"), equalTo("veryOpaque"));
        assertThat(response.getHeader("localAddr"), not(""));
        assertThat(response.getHeader("remoteAddr"), not(""));
        assertThat(response.getHeader("localPort"), not(""));
        assertThat(response.getHeader("remotePort"), not(""));
    }

    private HttpClient httpClient() {
        HttpServerTransport httpServerTransport = cluster().getInstance(HttpServerTransport.class);
        return new HttpClient(httpServerTransport.boundAddress().publishAddress());
    }
}