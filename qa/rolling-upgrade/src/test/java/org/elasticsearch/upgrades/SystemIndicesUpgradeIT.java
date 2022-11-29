/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        final String systemIndexWarning = "this request accesses system indices: [.tasks], but in a future major version, direct "
            + "access to system indices will be prevented by default";
        if (CLUSTER_TYPE == ClusterType.OLD) {
            // create index
            var createTestIndex = new Request("PUT", "/test_index_old").setJsonEntity("{\"settings\": {\"index.number_of_replicas\": 0}}");
            client().performRequest(createTestIndex);

            var bulk = new Request("POST", "/_bulk").addParameter("refresh", "true").setJsonEntity("""
                {"index": {"_index": "test_index_old"}}
                {"f1": "v1", "f2": "v2"}
                """);
            client().performRequest(bulk);

            // start a async reindex job
            var reindex = new Request("POST", "/_reindex").setJsonEntity("""
                {
                  "source":{
                    "index":"test_index_old"
                  },
                  "dest":{
                    "index":"test_index_reindex"
                  }
                }""").addParameter("wait_for_completion", "false");
            Map<String, Object> response = entityAsMap(client().performRequest(reindex));
            String taskId = (String) response.get("task");

            // wait for task
            var getTask = new Request("GET", "/_tasks/" + taskId).addParameter("wait_for_completion", "true");
            client().performRequest(getTask);

            // make sure .tasks index exists
            var getTasksIndex = new Request("GET", "/.tasks").setOptions(expectVersionSpecificWarnings(v -> {
                v.current(systemIndexWarning);
                v.compatible(systemIndexWarning);
            })).addParameter("allow_no_indices", "false");

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
                var putAliasRequest = new Request("POST", "/_aliases").setJsonEntity("""
                    {
                      "actions": [
                        {"add":  {"index":  ".tasks", "alias": "test-system-alias"}},
                        {"add":  {"index":  "test_index_reindex", "alias": "test-system-alias"}}
                      ]
                    }""").setOptions(expectVersionSpecificWarnings(v -> {
                    v.current(systemIndexWarning);
                    v.compatible(systemIndexWarning);
                }));
                assertThat(client().performRequest(putAliasRequest).getStatusLine().getStatusCode(), is(200));
            }
        } else if (CLUSTER_TYPE == ClusterType.UPGRADED) {
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
