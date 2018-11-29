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

package org.elasticsearch.cluster;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope= Scope.TEST, numDataNodes =0)
public class UpdateSettingsValidationIT extends ESIntegTestCase {
    public void testUpdateSettingsValidation() throws Exception {
        internalCluster().startNodes(
                Settings.builder().put(Node.NODE_DATA_SETTING.getKey(), false).build(),
                Settings.builder().put(Node.NODE_MASTER_SETTING.getKey(), false).build(),
                Settings.builder().put(Node.NODE_MASTER_SETTING.getKey(), false).build()
        );

        createIndex("test");
        NumShards test = getNumShards("test");

        ClusterHealthResponse healthResponse = client().admin().cluster().prepareHealth("test")
            .setWaitForEvents(Priority.LANGUID).setWaitForNodes("3").setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        assertThat(healthResponse.getIndices().get("test").getActiveShards(), equalTo(test.totalNumShards));

        client().admin().indices().prepareUpdateSettings("test")
            .setSettings(Settings.builder().put("index.number_of_replicas", 0)).execute().actionGet();
        healthResponse = client().admin().cluster().prepareHealth("test")
            .setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        assertThat(healthResponse.getIndices().get("test").getActiveShards(), equalTo(test.numPrimaries));

        try {
            client().admin().indices().prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.refresh_interval", "")).execute().actionGet();
            fail();
        } catch (IllegalArgumentException ex) {
            logger.info("Error message: [{}]", ex.getMessage());
        }
    }
}
