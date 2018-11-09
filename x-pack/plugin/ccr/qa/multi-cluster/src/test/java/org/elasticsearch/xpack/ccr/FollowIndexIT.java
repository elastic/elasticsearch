/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class FollowIndexIT extends ESCCRRestTestCase {

    public void testFollowIndex() throws Exception {
        final int numDocs = 128;
        final String leaderIndexName = "test_index1";
        if ("leader".equals(targetCluster)) {
            logger.info("Running against leader cluster");
            String mapping = "";
            if (randomBoolean()) { // randomly do source filtering on indexing
                mapping =
                    "\"_doc\": {" +
                    "  \"_source\": {" +
                    "    \"includes\": [\"field\"]," +
                    "    \"excludes\": [\"filtered_field\"]" +
                    "   }"+
                    "}";
            }
            Settings indexSettings = Settings.builder()
                    .put("index.soft_deletes.enabled", true)
                    .build();
            createIndex(leaderIndexName, indexSettings, mapping);
            for (int i = 0; i < numDocs; i++) {
                logger.info("Indexing doc [{}]", i);
                index(client(), leaderIndexName, Integer.toString(i), "field", i, "filtered_field", "true");
            }
            refresh(leaderIndexName);
            verifyDocuments(leaderIndexName, numDocs, "filtered_field:true");
        } else {
            logger.info("Running against follow cluster");
            final String followIndexName = "test_index2";
            followIndex(leaderIndexName, followIndexName);
            assertBusy(() -> verifyDocuments(followIndexName, numDocs, "filtered_field:true"));
            // unfollow and then follow and then index a few docs in leader index:
            pauseFollow(followIndexName);
            resumeFollow(followIndexName);
            try (RestClient leaderClient = buildLeaderClient()) {
                int id = numDocs;
                index(leaderClient, leaderIndexName, Integer.toString(id), "field", id, "filtered_field", "true");
                index(leaderClient, leaderIndexName, Integer.toString(id + 1), "field", id + 1, "filtered_field", "true");
                index(leaderClient, leaderIndexName, Integer.toString(id + 2), "field", id + 2, "filtered_field", "true");
            }
            assertBusy(() -> verifyDocuments(followIndexName, numDocs + 3, "filtered_field:true"));
            assertBusy(() -> verifyCcrMonitoring(leaderIndexName, followIndexName), 30, TimeUnit.SECONDS);

            pauseFollow(followIndexName);
            assertOK(client().performRequest(new Request("POST", "/" + followIndexName + "/_close")));
            assertOK(client().performRequest(new Request("POST", "/" + followIndexName + "/_ccr/unfollow")));
            Exception e = expectThrows(ResponseException.class, () -> resumeFollow(followIndexName));
            assertThat(e.getMessage(), containsString("follow index [" + followIndexName + "] does not have ccr metadata"));
        }
    }

    public void testFollowNonExistingLeaderIndex() throws Exception {
        assumeFalse("Test should only run when both clusters are running", "leader".equals(targetCluster));
        ResponseException e = expectThrows(ResponseException.class, () -> resumeFollow("non-existing-index"));
        assertThat(e.getMessage(), containsString("no such index [non-existing-index]"));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404));

        e = expectThrows(ResponseException.class, () -> followIndex("non-existing-index", "non-existing-index"));
        assertThat(e.getMessage(), containsString("no such index [non-existing-index]"));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404));
    }

    public void testAutoFollowPatterns() throws Exception {
        assumeFalse("Test should only run when both clusters are running", "leader".equals(targetCluster));

        Request request = new Request("PUT", "/_ccr/auto_follow/test_pattern");
        request.setJsonEntity("{\"leader_index_patterns\": [\"logs-*\"], \"remote_cluster\": \"leader_cluster\"}");
        assertOK(client().performRequest(request));

        try (RestClient leaderClient = buildLeaderClient()) {
            Settings settings = Settings.builder()
                .put("index.soft_deletes.enabled", true)
                .build();
            request = new Request("PUT", "/logs-20190101");
            request.setJsonEntity("{\"settings\": " + Strings.toString(settings) +
                ", \"mappings\": {\"_doc\": {\"properties\": {\"field\": {\"type\": \"keyword\"}}}} }");
            assertOK(leaderClient.performRequest(request));

            for (int i = 0; i < 5; i++) {
                String id = Integer.toString(i);
                index(leaderClient, "logs-20190101", id, "field", i, "filtered_field", "true");
            }
        }

        assertBusy(() -> {
            Request statsRequest = new Request("GET", "/_ccr/stats");
            Map<?, ?> response = toMap(client().performRequest(statsRequest));
            response = (Map<?, ?>) response.get("auto_follow_stats");
            assertThat(response.get("number_of_successful_follow_indices"), equalTo(1));

            ensureYellow("logs-20190101");
            verifyDocuments("logs-20190101", 5, "filtered_field:true");
        });
        assertBusy(() -> {
            verifyCcrMonitoring("logs-20190101", "logs-20190101");
            verifyAutoFollowMonitoring();
        }, 30, TimeUnit.SECONDS);
    }

}
