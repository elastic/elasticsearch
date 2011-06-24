/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.integration.document;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.elasticsearch.client.Requests.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

public class GetActionTests extends AbstractNodesTests {

    protected Client client;

    @BeforeClass public void startNodes() {
        startNode("node1");
        startNode("node2");
        client = client("node1");
    }

    @AfterClass public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    @Test public void simpleGetTests() {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // fine
        }
        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.refresh_interval", -1)).execute().actionGet();

        ClusterHealthResponse clusterHealth = client.admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        assertThat(clusterHealth.timedOut(), equalTo(false));
        assertThat(clusterHealth.status(), equalTo(ClusterHealthStatus.GREEN));

        GetResponse response = client.prepareGet("test", "type1", "1").execute().actionGet();
        assertThat(response.exists(), equalTo(false));

        logger.info("--> index doc 1");
        client.prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2").execute().actionGet();

        logger.info("--> realtime get 1");
        response = client.prepareGet("test", "type1", "1").execute().actionGet();
        assertThat(response.exists(), equalTo(true));
        assertThat(response.sourceAsMap().get("field1").toString(), equalTo("value1"));
        assertThat(response.sourceAsMap().get("field2").toString(), equalTo("value2"));

        logger.info("--> realtime get 1 (no type)");
        response = client.prepareGet("test", null, "1").execute().actionGet();
        assertThat(response.exists(), equalTo(true));
        assertThat(response.sourceAsMap().get("field1").toString(), equalTo("value1"));
        assertThat(response.sourceAsMap().get("field2").toString(), equalTo("value2"));

        logger.info("--> non realtime get 1");
        response = client.prepareGet("test", "type1", "1").setRealtime(false).execute().actionGet();
        assertThat(response.exists(), equalTo(false));

        logger.info("--> realtime fetch of field (requires fetching parsing source)");
        response = client.prepareGet("test", "type1", "1").setFields("field1").execute().actionGet();
        assertThat(response.exists(), equalTo(true));
        assertThat(response.source(), nullValue());
        assertThat(response.field("field1").values().get(0).toString(), equalTo("value1"));
        assertThat(response.field("field2"), nullValue());

        logger.info("--> flush the index, so we load it from it");
        client.admin().indices().prepareFlush().execute().actionGet();

        logger.info("--> realtime get 1 (loaded from index)");
        response = client.prepareGet("test", "type1", "1").execute().actionGet();
        assertThat(response.exists(), equalTo(true));
        assertThat(response.sourceAsMap().get("field1").toString(), equalTo("value1"));
        assertThat(response.sourceAsMap().get("field2").toString(), equalTo("value2"));

        logger.info("--> non realtime get 1 (loaded from index)");
        response = client.prepareGet("test", "type1", "1").setRealtime(false).execute().actionGet();
        assertThat(response.exists(), equalTo(true));
        assertThat(response.sourceAsMap().get("field1").toString(), equalTo("value1"));
        assertThat(response.sourceAsMap().get("field2").toString(), equalTo("value2"));

        logger.info("--> realtime fetch of field (loaded from index)");
        response = client.prepareGet("test", "type1", "1").setFields("field1").execute().actionGet();
        assertThat(response.exists(), equalTo(true));
        assertThat(response.source(), nullValue());
        assertThat(response.field("field1").values().get(0).toString(), equalTo("value1"));
        assertThat(response.field("field2"), nullValue());
    }
}