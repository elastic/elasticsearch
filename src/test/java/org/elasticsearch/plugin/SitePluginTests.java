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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.helper.HttpClient;
import org.elasticsearch.rest.helper.HttpClientResponse;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * We want to test site plugins
 */
@ClusterScope(scope = Scope.SUITE, numNodes = 1)
public class SitePluginTests extends ElasticsearchIntegrationTest {


    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        try {
            File pluginDir = new File(SitePluginTests.class.getResource("/org/elasticsearch/plugin").toURI());
            return settingsBuilder()
                    .put(super.nodeSettings(nodeOrdinal))
                    .put("path.plugins", pluginDir.getAbsolutePath())
                    .put("force.http.enabled", true)
                    .build();
        } catch (URISyntaxException ex) {
            throw new RuntimeException(ex);
        }
    }

    public HttpClient httpClient(String id) {
        HttpServerTransport httpServerTransport = cluster().getInstance(HttpServerTransport.class);
        return new HttpClient(httpServerTransport.boundAddress().publishAddress());
    }

    @Test
    public void testRedirectSitePlugin() throws Exception {
        // We use an HTTP Client to test redirection
        HttpClientResponse response = httpClient("test").request("/_plugin/dummy");
        assertThat(response.errorCode(), equalTo(RestStatus.MOVED_PERMANENTLY.getStatus()));
        assertThat(response.response(), containsString("/_plugin/dummy/"));

        // We test the real URL
        response = httpClient("test").request("/_plugin/dummy/");
        assertThat(response.errorCode(), equalTo(RestStatus.OK.getStatus()));
        assertThat(response.response(), containsString("<title>Dummy Site Plugin</title>"));
    }
}
