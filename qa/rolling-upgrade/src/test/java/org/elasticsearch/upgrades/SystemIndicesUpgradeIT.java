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

import org.elasticsearch.client.Request;

import java.util.Map;

import static org.hamcrest.Matchers.is;

public class SystemIndicesUpgradeIT extends AbstractRollingTestCase {

    public void testOldDoesntHaveSystemIndexMetadata() throws Exception {
        assumeTrue("only run in old cluster", CLUSTER_TYPE == ClusterType.OLD);
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
        assertBusy(() -> {
            Request getTasksIndex = new Request("GET", "/.tasks");
            assertThat(client().performRequest(getTasksIndex).getStatusLine().getStatusCode(), is(200));
        });
    }

    public void testMixedCluster() {
        assumeTrue("nothing to do in mixed cluster", CLUSTER_TYPE == ClusterType.MIXED);
    }

    @SuppressWarnings("unchecked")
    public void testUpgradedCluster() throws Exception {
        assumeTrue("only run on upgraded cluster", CLUSTER_TYPE == ClusterType.UPGRADED);

        assertBusy(() -> {
            Request clusterStateRequest = new Request("GET", "/_cluster/state/metadata");
            Map<String, Object> response = entityAsMap(client().performRequest(clusterStateRequest));
            Map<String, Object> metadata = (Map<String, Object>) response.get("metadata");
            assertNotNull(metadata);
            Map<String, Object> indices = (Map<String, Object>) metadata.get("indices");
            assertNotNull(indices);

            Map<String, Object> tasksIndex = (Map<String, Object>) indices.get(".tasks");
            assertNotNull(tasksIndex);
            assertThat(tasksIndex.get("system"), is(true));

            Map<String, Object> testIndex = (Map<String, Object>) indices.get("test_index_old");
            assertNotNull(testIndex);
            assertThat(testIndex.get("system"), is(false));
        });
    }
}
