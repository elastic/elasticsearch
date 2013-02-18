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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.elasticsearch.test.integration.rest.helper.HttpClient;
import org.elasticsearch.test.integration.rest.helper.HttpClientResponse;
import org.testng.annotations.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * We want to test site plugins
 */
public class SitePluginTests extends AbstractNodesTests {

    @Test
    public void testRedirectSitePlugin() throws Exception {

        Settings settings = settingsBuilder()
                .put("path.plugins", "target/test-classes/org/elasticsearch/test/integration/plugin/")
                .build();

        InternalNode node = (InternalNode) buildNode("test", settings);
        node.start();

        // We use an HTTP Client to test redirection
        HttpClientResponse response = new HttpClient("http://localhost:9200").request("/_plugin/dummy");
        assertThat(response.errorCode(), equalTo(RestStatus.MOVED_PERMANENTLY.getStatus()));
        assertThat(response.response(), containsString("/_plugin/dummy/"));

        // We test the real URL
        response = new HttpClient("http://localhost:9200").request("/_plugin/dummy/");
        assertThat(response.errorCode(), equalTo(RestStatus.OK.getStatus()));
        assertThat(response.response(), containsString("<title>Dummy Site Plugin</title>"));
    }
}
