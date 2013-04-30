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

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.test.integration.nodesinfo.plugin.dummyfilter.TestHttpServer.HEADER_NAME;
import static org.elasticsearch.test.integration.nodesinfo.plugin.dummyfilter.TestHttpServer.HEADER_VALUE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.elasticsearch.cluster.ClusterService;
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

import java.net.URL;

import java.io.File;

/**
 * Test a java plugin that replaces the http server and retruns a response
 * with a custom header.
 */
public class CustomHeaderPluginTests extends AbstractNodesTests {

    @BeforeMethod
    public void startNode() throws Exception {
    	String name = "test";
        URL resource = CustomHeaderPluginTests.class.getResource("/org/elasticsearch/test/integration/customheader/");
        ImmutableSettings.Builder settings = settingsBuilder();
        if (resource != null) {
            settings.put("path.plugins", new File(resource.toURI()).getAbsolutePath());
        }

        startNode(name, settings);

        // We wait for a Green status
        client(name).admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();

        String serverNodeId = ((InternalNode) node(name)).injector()
                .getInstance(ClusterService.class).state().nodes().localNodeId();
        logger.debug("--> server {} started" + serverNodeId);
    }

    @AfterMethod
    public void closeNodes() {
        closeAllNodes();
    }

    public HttpClient httpClient(String id) {
        HttpServerTransport httpServerTransport = ((InternalNode) node(id)).injector().getInstance(HttpServerTransport.class);
        return new HttpClient(httpServerTransport.boundAddress().publishAddress());
    }

    @Test
    public void testCustomHeaderPlugin() throws Exception {
        // We use an HTTP Client to test redirection
        HttpClientResponse response = httpClient("test").request("/_plugin/dummyfilter1");
        assertThat(response.errorCode(), equalTo(RestStatus.UNAUTHORIZED.getStatus()));
        assertThat(response.getHeader(HEADER_NAME), equalTo(HEADER_VALUE));
    }


}