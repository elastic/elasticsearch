/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr;

import org.apache.lucene.util.Constants;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.ObjectPath.eval;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.Is.is;

public class FollowIndexIT extends ESCCRRestTestCase {

    public void testDowngradeRemoteClusterToBasic() throws Exception {
        if ("follow".equals(targetCluster) == false) {
            return;
        }

        {
            Request request = new Request("PUT", "/_ccr/auto_follow/test_pattern");
            request.setJsonEntity("{\"leader_index_patterns\": [\"logs-*\"], \"remote_cluster\": \"leader_cluster\"}");
            assertOK(client().performRequest(request));
        }

        String index1 = "logs-20190101";
        try (RestClient leaderClient = buildLeaderClient()) {
            createNewIndexAndIndexDocs(leaderClient, index1);
        }

        assertBusy(() -> {
            ensureYellow(index1);
            verifyDocuments(index1, 5, "filtered_field:true");
        });

        String index2 = "logs-20190102";
        try (RestClient leaderClient = buildLeaderClient()) {
            Request request = new Request("POST", "/_license/start_basic");
            request.addParameter("acknowledge", "true");
            Map<?, ?> response = toMap(leaderClient.performRequest(request));
            assertThat(response.get("basic_was_started"), is(true));
            assertThat(response.get("acknowledged"), is(true));

            createNewIndexAndIndexDocs(leaderClient, index2);
            index(leaderClient, index1, "5", "field", 5, "filtered_field", "true");
        }

        assertBusy(() -> {
            Request statsRequest = new Request("GET", "/_ccr/stats");
            Map<?, ?> response = toMap(client().performRequest(statsRequest));
            assertThat(eval("auto_follow_stats.number_of_successful_follow_indices", response), equalTo(1));
            assertThat(eval("auto_follow_stats.number_of_failed_remote_cluster_state_requests", response), greaterThanOrEqualTo(1));
            assertThat(eval("auto_follow_stats.recent_auto_follow_errors.0.auto_follow_exception.reason", response),
                containsString("the license mode [BASIC] on cluster [leader_cluster] does not enable [ccr]"));

            // Follow indices actively following leader indices before the downgrade to basic license remain to follow
            // the leader index after the downgrade, so document with id 5 should be replicated to follower index:
            verifyDocuments(index1, 6, "filtered_field:true");

            // Index2 was created in leader cluster after the downgrade and therefor the auto follow coordinator in
            // follow cluster should not pick that index up:
            assertThat(indexExists(index2), is(false));

            // parse the logs and ensure that the auto-coordinator skipped coordination on the leader cluster
            // (does not work on windows...)
            if (Constants.WINDOWS == false) {
                assertBusy(() -> {
                    final List<String> lines = Files.readAllLines(PathUtils.get(System.getProperty("log")));
                    final Iterator<String> it = lines.iterator();
                    boolean warn = false;
                    while (it.hasNext()) {
                        final String line = it.next();
                        if (line.matches(".*\\[WARN\\s*\\]\\[o\\.e\\.x\\.c\\.a\\.AutoFollowCoordinator\\s*\\] \\[node-0\\] " +
                            "failure occurred while fetching cluster state for auto follow pattern \\[test_pattern\\]")) {
                            warn = true;
                            break;
                        }
                    }
                    assertTrue(warn);
                    assertTrue(it.hasNext());
                    final String lineAfterWarn = it.next();
                    assertThat(
                        lineAfterWarn,
                        equalTo("org.elasticsearch.ElasticsearchStatusException: " +
                            "can not fetch remote cluster state as the remote cluster [leader_cluster] is not licensed for [ccr]; " +
                            "the license mode [BASIC] on cluster [leader_cluster] does not enable [ccr]"));
                });
            }
        });

        // Manually following index2 also does not work after the downgrade:
        Exception e = expectThrows(ResponseException.class, () -> followIndex("leader_cluster", index2));
        assertThat(e.getMessage(), containsString("the license mode [BASIC] on cluster [leader_cluster] does not enable [ccr]"));
    }

    private void createNewIndexAndIndexDocs(RestClient client, String index) throws IOException {
        Settings settings = Settings.builder()
            .put("index.soft_deletes.enabled", true)
            .build();
        Request request = new Request("PUT", "/" + index);
        request.setJsonEntity("{\"settings\": " + Strings.toString(settings) +
            ", \"mappings\": {\"_doc\": {\"properties\": {\"field\": {\"type\": \"keyword\"}}}} }");
        assertOK(client.performRequest(request));

        for (int i = 0; i < 5; i++) {
            String id = Integer.toString(i);
            index(client, index, id, "field", i, "filtered_field", "true");
        }
    }

}
