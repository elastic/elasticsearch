/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.XContentTestUtils.JsonMapView;

import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.SYSTEM_INDEX_ENFORCEMENT_INDEX_VERSION;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class SystemIndicesUpgradeIT extends AbstractRollingUpgradeTestCase {

    public SystemIndicesUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @SuppressWarnings("unchecked")
    public void testSystemIndicesUpgrades() throws Exception {
        final String systemIndexWarning = "this request accesses system indices: [.tasks], but in a future major version, direct "
            + "access to system indices will be prevented by default";
        if (isOldCluster()) {
            // create index
            Request createTestIndex = new Request("PUT", "/test_index_old");
            createTestIndex.setJsonEntity("{\"settings\": {\"index.number_of_replicas\": 0}}");
            client().performRequest(createTestIndex);

            Request bulk = new Request("POST", "/_bulk");
            bulk.addParameter("refresh", "true");
            bulk.setJsonEntity("""
                {"index": {"_index": "test_index_old"}}
                {"f1": "v1", "f2": "v2"}
                """);
            client().performRequest(bulk);

            // start a async reindex job
            Request reindex = new Request("POST", "/_reindex");
            reindex.setJsonEntity("""
                {
                  "source":{
                    "index":"test_index_old"
                  },
                  "dest":{
                    "index":"test_index_reindex"
                  }
                }""");
            reindex.addParameter("wait_for_completion", "false");
            Map<String, Object> response = entityAsMap(client().performRequest(reindex));
            String taskId = (String) response.get("task");

            // wait for task
            Request getTask = new Request("GET", "/_tasks/" + taskId);
            getTask.addParameter("wait_for_completion", "true");
            client().performRequest(getTask);

            // make sure .tasks index exists
            Request getTasksIndex = new Request("GET", "/.tasks");
            getTasksIndex.setOptions(expectVersionSpecificWarnings(v -> {
                v.current(systemIndexWarning);
                v.compatible(systemIndexWarning);
            }));
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
        } else if (isUpgradedCluster()) {
            assertBusy(() -> {
                Request clusterStateRequest = new Request("GET", "/_cluster/state/metadata");
                Map<String, Object> indices = new JsonMapView(entityAsMap(client().performRequest(clusterStateRequest))).get(
                    "metadata.indices"
                );

                // Make sure our non-system index is still non-system
                assertThat(new JsonMapView(indices).get("test_index_old.system"), is(false));

                // Can't get the .tasks index via JsonMapView because it splits on `.`
                assertThat(indices, hasKey(".tasks"));
                JsonMapView tasksIndex = new JsonMapView((Map<String, Object>) indices.get(".tasks"));
                assertThat(tasksIndex.get("system"), is(true));

                // If .tasks was created in a 7.x version, it should have an alias on it that we need to make sure got upgraded properly.
                final String tasksCreatedVersionString = tasksIndex.get("settings.index.version.created");
                assertThat(tasksCreatedVersionString, notNullValue());
                final IndexVersion tasksCreatedVersion = IndexVersion.fromId(Integer.parseInt(tasksCreatedVersionString));
                if (tasksCreatedVersion.before(SYSTEM_INDEX_ENFORCEMENT_INDEX_VERSION)) {
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
