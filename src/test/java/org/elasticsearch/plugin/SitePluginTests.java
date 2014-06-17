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
package org.elasticsearch.plugin;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.helper.HttpClient;
import org.elasticsearch.rest.helper.HttpClientResponse;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.*;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * We want to test site plugins
 */
@ClusterScope(scope = Scope.SUITE, numDataNodes = 1)
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

    public HttpClient httpClient() {
        HttpServerTransport httpServerTransport = internalCluster().getDataNodeInstance(HttpServerTransport.class);
        return new HttpClient(httpServerTransport.boundAddress().publishAddress());
    }

    @Test
    public void testRedirectSitePlugin() throws Exception {
        // We use an HTTP Client to test redirection
        HttpClientResponse response = httpClient().request("/_plugin/dummy");
        assertThat(response.errorCode(), equalTo(RestStatus.MOVED_PERMANENTLY.getStatus()));
        assertThat(response.response(), containsString("/_plugin/dummy/"));

        // We test the real URL
        response = httpClient().request("/_plugin/dummy/");
        assertThat(response.errorCode(), equalTo(RestStatus.OK.getStatus()));
        assertThat(response.response(), containsString("<title>Dummy Site Plugin</title>"));
    }

    /**
     * Test direct access to an existing file (index.html)
     */
    @Test
    public void testAnyPage() throws Exception {
        HttpClientResponse response = httpClient().request("/_plugin/dummy/index.html");
        assertThat(response.errorCode(), equalTo(RestStatus.OK.getStatus()));
        assertThat(response.response(), containsString("<title>Dummy Site Plugin</title>"));
    }

    /**
     * Test case for #4845: https://github.com/elasticsearch/elasticsearch/issues/4845
     * Serving _site plugins do not pick up on index.html for sub directories
     */
    @Test
    public void testWelcomePageInSubDirs() throws Exception {
        HttpClientResponse response = httpClient().request("/_plugin/subdir/dir/");
        assertThat(response.errorCode(), equalTo(RestStatus.OK.getStatus()));
        assertThat(response.response(), containsString("<title>Dummy Site Plugin (subdir)</title>"));

        response = httpClient().request("/_plugin/subdir/dir_without_index/");
        assertThat(response.errorCode(), equalTo(RestStatus.FORBIDDEN.getStatus()));

        response = httpClient().request("/_plugin/subdir/dir_without_index/page.html");
        assertThat(response.errorCode(), equalTo(RestStatus.OK.getStatus()));
        assertThat(response.response(), containsString("<title>Dummy Site Plugin (page)</title>"));
    }
}
