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
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.test.XContentTestUtils;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class FeatureUpgradeIT extends AbstractRollingUpgradeTestCase {

    public FeatureUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @BeforeClass
    public static void ensureNotForwardCompatTest() {
        assumeFalse("Only supported by bwc tests", Boolean.parseBoolean(System.getProperty("tests.fwc", "false")));
    }

    public void testGetFeatureUpgradeStatus() throws Exception {

        final String systemIndexWarning = "this request accesses system indices: [.tasks], but in a future major version, direct "
            + "access to system indices will be prevented by default";
        if (isOldCluster()) {
            // setup - put something in the tasks index
            // create index
            Request createTestIndex = new Request("PUT", "/feature_test_index_old");
            createTestIndex.setJsonEntity("{\"settings\": {\"index.number_of_replicas\": 0}}");
            client().performRequest(createTestIndex);

            Request bulk = new Request("POST", "/_bulk");
            bulk.addParameter("refresh", "true");
            bulk.setJsonEntity("""
                {"index": {"_index": "feature_test_index_old"}}
                {"f1": "v1", "f2": "v2"}
                """);
            client().performRequest(bulk);

            // start a async reindex job
            Request reindex = new Request("POST", "/_reindex");
            reindex.setJsonEntity("""
                {
                  "source":{
                    "index":"feature_test_index_old"
                  },
                  "dest":{
                    "index":"feature_test_index_reindex"
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

            assertBusy(() -> {
                try {
                    assertThat(client().performRequest(getTasksIndex).getStatusLine().getStatusCode(), is(200));
                } catch (ResponseException e) {
                    throw new AssertionError(".tasks index does not exist yet");
                }
            });

        } else if (isUpgradedCluster()) {
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

                assertThat(feature, aMapWithSize(4));
                assertThat(feature.get("minimum_index_version"), equalTo(getOldClusterIndexVersion().toReleaseVersion()));

                // Feature migration happens only across major versions; also, we usually begin to require migrations once we start testing
                // for the next major version upgrade (see e.g. #93666). Trying to express this with features may be problematic, so we
                // want to keep using versions here. We also assume that for non-semantic version migrations are not required.
                boolean migrationNeeded = parseLegacyVersion(getOldClusterVersion()).map(
                    v -> v.before(SystemIndices.NO_UPGRADE_REQUIRED_VERSION)
                ).orElse(false);
                if (migrationNeeded) {
                    assertThat(feature.get("migration_status"), equalTo("MIGRATION_NEEDED"));
                } else {
                    assertThat(feature.get("migration_status"), equalTo("NO_MIGRATION_NEEDED"));
                }
            });
        }
    }
}
