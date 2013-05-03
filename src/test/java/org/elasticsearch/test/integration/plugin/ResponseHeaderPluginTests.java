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

package org.elasticsearch.test.integration.plugin;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.beust.jcommander.internal.Maps;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.elasticsearch.test.integration.rest.helper.HttpClient;
import org.elasticsearch.test.integration.rest.helper.HttpClientResponse;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URL;
import java.util.Map;

/**
 * Test a rest action that sets special response headers
 */
public class ResponseHeaderPluginTests extends AbstractNodesTests {

    public static final String NODE_ID = "TEST";

    @BeforeMethod
    public void startNode() throws Exception {
        URL resource = ResponseHeaderPluginTests.class.getResource("/org/elasticsearch/test/integration/responseheader/");
        ImmutableSettings.Builder settings = settingsBuilder();
        if (resource != null) {
            settings.put("path.plugins", new File(resource.toURI()).getAbsolutePath());
        }

        startNode(NODE_ID, settings);
        client(NODE_ID).admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
    }

    @AfterMethod
    public void closeNodes() {
        closeAllNodes();
    }

    @Test
    public void testThatSettingHeadersWorks() throws Exception {
        HttpClientResponse response = httpClient().request("/_protected");
        assertThat(response.errorCode(), equalTo(RestStatus.UNAUTHORIZED.getStatus()));
        assertThat(response.getHeader("Secret"), equalTo("required"));

        Map<String, String> headers = Maps.newHashMap();
        headers.put("Secret", "password");
        HttpClientResponse authResponse = httpClient().request("GET", "_protected", headers);
        assertThat(authResponse.errorCode(), equalTo(RestStatus.OK.getStatus()));
        assertThat(authResponse.getHeader("Secret"), equalTo("granted"));
    }

    private HttpClient httpClient() {
        HttpServerTransport httpServerTransport = ((InternalNode) node(NODE_ID)).injector().getInstance(HttpServerTransport.class);
        return new HttpClient(httpServerTransport.boundAddress().publishAddress());
    }
}