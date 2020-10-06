/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.upgrades;

import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.XContentTestUtils.JsonMapView;

import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.SYSTEM_INDEX_ENFORCEMENT_VERSION;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class SystemIndicesUpgradeIT extends AbstractRollingTestCase {

    @SuppressWarnings("unchecked")
    public void testSystemIndicesUpgrades() throws Exception {
        final String systemIndexWarning = "this request accesses system indices: [.tasks], but in a future major version, direct " +
            "access to system indices will be prevented by default";
        if (CLUSTER_TYPE == ClusterType.OLD) {
            // create index
            Request createTestIndex = new Request("PUT", "/test_index_old");
            createTestIndex.setJsonEntity("{\"settings\": {\"index.number_of_replicas\": 0}}");
            client().performRequest(createTestIndex);

            Request bulk = new Request("POST", "/_bulk");
            bulk.addParameter("refresh", "true");
            bulk.setJsonEntity("{\"index\": {\"_index\": \"test_index_old\"}}\n" +
                "{\"f1\": \"v1\", \"f2\": \"v2\"}\n");
            client().performRequest(bulk);

            // start a async reindex job
            Request reindex = new Request("POST", "/_reindex");
            reindex.setJsonEntity(
                "{\n" +
                    "  \"source\":{\n" +
                    "    \"index\":\"test_index_old\"\n" +
                    "  },\n" +
                    "  \"dest\":{\n" +
                    "    \"index\":\"test_index_reindex\"\n" +
                    "  }\n" +
                    "}");
            reindex.addParameter("wait_for_completion", "false");
            Map<String, Object> response = entityAsMap(client().performRequest(reindex));
            String taskId = (String) response.get("task");

            // wait for task
            Request getTask = new Request("GET", "/_tasks/" + taskId);
            getTask.addParameter("wait_for_completion", "true");
            client().performRequest(getTask);

            // make sure .tasks index exists
            Request getTasksIndex = new Request("GET", "/.tasks");
            getTasksIndex.addParameter("allow_no_indices", "false");

            getTasksIndex.setOptions(expectVersionSpecificWarnings(v -> {
                v.current(systemIndexWarning);
                v.compatible(systemIndexWarning);
            }));
            assertBusy(() -> {
                try {
                    assertThat(client().performRequest(getTasksIndex).getStatusLine().getStatusCode(), is(200));
                } catch (ResponseException e) {
                    throw new AssertionError(".tasks index does not exist yet");
                }
            });

            // If we are on 7.x create an alias that includes both a system index and a non-system index so we can be sure it gets
            // upgraded properly. If we're already on 8.x, skip this part of the test.
            if (minimumNodeVersion().before(SYSTEM_INDEX_ENFORCEMENT_VERSION)) {
                // Create an alias to make sure it gets upgraded properly
                Request putAliasRequest = new Request("POST", "/_aliases");
                putAliasRequest.setJsonEntity("{\n" +
                    "  \"actions\": [\n" +
                    "    {\"add\":  {\"index\":  \".tasks\", \"alias\": \"test-system-alias\"}},\n" +
                    "    {\"add\":  {\"index\":  \"test_index_reindex\", \"alias\": \"test-system-alias\"}}\n" +
                    "  ]\n" +
                    "}");
                assertThat(client().performRequest(putAliasRequest).getStatusLine().getStatusCode(), is(200));
            }
        } else if (CLUSTER_TYPE == ClusterType.UPGRADED) {
            assertBusy(() -> {
                Request clusterStateRequest = new Request("GET", "/_cluster/state/metadata");
                Map<String, Object> indices = new JsonMapView(entityAsMap(client().performRequest(clusterStateRequest)))
                    .get("metadata.indices");

                // Make sure our non-system index is still non-system
                assertThat(new JsonMapView(indices).get("test_index_old.system"), is(false));

                // Can't get the .tasks index via JsonMapView because it splits on `.`
                assertThat(indices, hasKey(".tasks"));
                JsonMapView tasksIndex = new JsonMapView((Map<String, Object>) indices.get(".tasks"));
                assertThat(tasksIndex.get("system"), is(true));

                // If .tasks was created in a 7.x version, it should have an alias on it that we need to make sure got upgraded properly.
                final String tasksCreatedVersionString = tasksIndex.get("settings.index.version.created");
                assertThat(tasksCreatedVersionString, notNullValue());
                final Version tasksCreatedVersion = Version.fromId(Integer.parseInt(tasksCreatedVersionString));
                if (tasksCreatedVersion.before(SYSTEM_INDEX_ENFORCEMENT_VERSION)) {
                    // Verify that the alias survived the upgrade
                    Request getAliasRequest = new Request("GET", "/_alias/test-system-alias");
                    getAliasRequest.setOptions(expectVersionSpecificWarnings(v -> {
                        v.current(systemIndexWarning);
                        v.compatible(systemIndexWarning);
                    }));
                    Map<String, Object> aliasResponse = entityAsMap(client().performRequest(getAliasRequest));
                    assertThat(aliasResponse, hasKey(".tasks"));
                    assertThat(aliasResponse, hasKey("test_index_reindex"));
                }
            });
        }
    }
}
