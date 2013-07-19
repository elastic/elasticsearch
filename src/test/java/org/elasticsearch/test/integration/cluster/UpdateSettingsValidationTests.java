/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.test.integration.cluster;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.common.Priority;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.junit.After;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class UpdateSettingsValidationTests extends AbstractNodesTests {

    @After
    public void closeNodes() {
        closeAllNodes();
    }

    @Test
    public void testUpdateSettingsValidation() throws Exception {
        startNode("master", settingsBuilder().put("node.data", false).build());
        startNode("node1", settingsBuilder().put("node.master", false).build());
        startNode("node2", settingsBuilder().put("node.master", false).build());

        client("master").admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_shards", 5).put("index.number_of_replicas", 1)).execute().actionGet();
        ClusterHealthResponse healthResponse = client("master").admin().cluster().prepareHealth("test").setWaitForEvents(Priority.LANGUID).setWaitForNodes("3").setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        assertThat(healthResponse.getIndices().get("test").getActiveShards(), equalTo(10));

        client("master").admin().indices().prepareUpdateSettings("test").setSettings(settingsBuilder().put("index.number_of_replicas", 0)).execute().actionGet();
        healthResponse = client("master").admin().cluster().prepareHealth("test").setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        assertThat(healthResponse.getIndices().get("test").getActiveShards(), equalTo(5));

        try {
            client("master").admin().indices().prepareUpdateSettings("test").setSettings(settingsBuilder().put("index.refresh_interval", "")).execute().actionGet();
            assert false;
        } catch (ElasticSearchIllegalArgumentException ex) {
            logger.info("Error message: [{}]", ex.getMessage());
        }
    }
}
