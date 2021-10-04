/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

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

        final String systemIndexWarning = "this request accesses system indices: [.watches], but in a future major version, direct " +
            "access to system indices will be prevented by default";
        if (CLUSTER_TYPE == ClusterType.OLD) {
            // setup - put something in the watcher index
            Request createWatch = new Request("PUT", "/_watcher/watch/1");
            createWatch.addParameter("active", "false");
            createWatch.setJsonEntity("{\n" +
                "  \"trigger\" : {\n" +
                "    \"schedule\" : { \"interval\" : \"10s\" } \n" +
                "  },\n" +
                "  \"input\" : {\n" +
                "    \"search\" : {\n" +
                "      \"request\" : {\n" +
                "        \"indices\" : [ \"logs\" ],\n" +
                "        \"body\" : {\n" +
                "          \"query\" : {\n" +
                "            \"match\" : { \"message\": \"error\" }\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}");

            client().performRequest(createWatch);

            Request getTasksIndex = new Request("GET", "/.watches");
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
                    throw new AssertionError(".watcher index does not exist yet");
                }
            });

        } else if (CLUSTER_TYPE == ClusterType.UPGRADED) {
            // check results
            assertBusy(() -> {
                Request clusterStateRequest = new Request("GET", "/_migration/system_features");
                XContentTestUtils.JsonMapView view = new XContentTestUtils.JsonMapView(
                    entityAsMap(client().performRequest(clusterStateRequest)));

                List<Map<String, Object>> features = view.get("features");
                Map<String, Object> feature = features.stream()
                    .filter(e -> "watcher".equals(e.get("feature_name")))
                    .findFirst()
                    .orElse(Collections.emptyMap());

                assertThat(feature.size(), equalTo(4));
                assertThat(feature.get("minimum_index_version"), equalTo(UPGRADE_FROM_VERSION.toString()));
            });
        }
    }

}
