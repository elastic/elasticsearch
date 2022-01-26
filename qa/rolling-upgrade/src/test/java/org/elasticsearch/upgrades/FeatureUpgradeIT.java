/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.migration.TransportGetFeatureUpgradeStatusAction;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.XContentTestUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class FeatureUpgradeIT extends AbstractRollingTestCase {

    @SuppressWarnings("unchecked")
    public void testGetFeatureUpgradeStatus() throws Exception {

        final String systemIndexWarning = "this request accesses system indices: [.tasks], but in a future major version, direct "
            + "access to system indices will be prevented by default";
        if (CLUSTER_TYPE == ClusterType.OLD) {
            // setup - put something in the tasks index
            // create index
            Request createTestIndex = new Request("PUT", "/feature_test_index_old");
            createTestIndex.setJsonEntity("{\"settings\": {" + "\"index.number_of_replicas\": 0," + "\"index.number_of_shards\": 1" + "}}");
            client().performRequest(createTestIndex);

            Request bulk = new Request("POST", "/_bulk");
            bulk.addParameter("refresh", "true");
            if (UPGRADE_FROM_VERSION.before(Version.V_7_0_0)) {
                bulk.setJsonEntity(
                    "{\"index\": {\"_index\": \"feature_test_index_old\", \"_type\" : \"_doc\"}}\n" + "{\"f1\": \"v1\", \"f2\": \"v2\"}\n"
                );
            } else {
                bulk.setJsonEntity("{\"index\": {\"_index\": \"feature_test_index_old\"}\n" + "{\"f1\": \"v1\", \"f2\": \"v2\"}\n");
            }
            client().performRequest(bulk);

            // start a async reindex job
            Request reindex = new Request("POST", "/_reindex");
            reindex.setJsonEntity(
                "{\n"
                    + "  \"source\":{\n"
                    + "    \"index\":\"feature_test_index_old\"\n"
                    + "  },\n"
                    + "  \"dest\":{\n"
                    + "    \"index\":\"feature_test_index_reindex\"\n"
                    + "  }\n"
                    + "}"
            );
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
            if (UPGRADE_FROM_VERSION.before(Version.V_7_0_0)) {
                getTasksIndex.addParameter("include_type_name", "false");
            }

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

        } else if (CLUSTER_TYPE == ClusterType.UPGRADED) {
            // check results
            assertBusy(() -> {
                Request clusterStateRequest = new Request("GET", "/_migration/system_features");
                XContentTestUtils.JsonMapView view = new XContentTestUtils.JsonMapView(
                    entityAsMap(client().performRequest(clusterStateRequest))
                );

                List<Map<String, Object>> features = view.get("features");
                Map<String, Object> feature = features.stream()
                    .filter(e -> "tasks".equals(e.get("feature_name")))
                    .findFirst()
                    .orElse(Collections.emptyMap());

                assertThat(feature.size(), equalTo(4));
                assertThat(feature.get("minimum_index_version"), equalTo(UPGRADE_FROM_VERSION.toString()));
                if (UPGRADE_FROM_VERSION.before(TransportGetFeatureUpgradeStatusAction.NO_UPGRADE_REQUIRED_VERSION)) {
                    assertThat(feature.get("migration_status"), equalTo("MIGRATION_NEEDED"));
                } else {
                    assertThat(feature.get("migration_status"), equalTo("NO_MIGRATION_NEEDED"));
                }
            });
        }
    }

}
