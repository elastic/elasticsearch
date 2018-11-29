/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class ChainIT extends ESCCRRestTestCase {

    public void testFollowIndex() throws Exception {
        final int numDocs = 128;
        final String leaderIndexName = "leader";
        final String middleIndexName = "middle";
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
        } else if ("middle".equals(targetCluster)) {
            logger.info("Running against middle cluster");
            followIndex("leader_cluster", leaderIndexName, middleIndexName);
            assertBusy(() -> verifyDocuments(middleIndexName, numDocs, "filtered_field:true"));
            try (RestClient leaderClient = buildLeaderClient()) {
                int id = numDocs;
                index(leaderClient, leaderIndexName, Integer.toString(id), "field", id, "filtered_field", "true");
                index(leaderClient, leaderIndexName, Integer.toString(id + 1), "field", id + 1, "filtered_field", "true");
                index(leaderClient, leaderIndexName, Integer.toString(id + 2), "field", id + 2, "filtered_field", "true");
            }
            assertBusy(() -> verifyDocuments(middleIndexName, numDocs + 3, "filtered_field:true"));
        } else if ("follow".equals(targetCluster)) {
            logger.info("Running against follow cluster");
            final String followIndexName = "follow";
            followIndex("middle_cluster", middleIndexName, followIndexName);
            assertBusy(() -> verifyDocuments(followIndexName, numDocs + 3, "filtered_field:true"));

            try (RestClient leaderClient = buildLeaderClient()) {
                int id = numDocs + 3;
                index(leaderClient, leaderIndexName, Integer.toString(id), "field", id, "filtered_field", "true");
                index(leaderClient, leaderIndexName, Integer.toString(id + 1), "field", id + 1, "filtered_field", "true");
                index(leaderClient, leaderIndexName, Integer.toString(id + 2), "field", id + 2, "filtered_field", "true");
            }

            try (RestClient middleClient = buildMiddleClient()) {
                assertBusy(() -> verifyDocuments(middleIndexName, numDocs + 6, "filtered_field:true", middleClient));
            }

            assertBusy(() -> verifyDocuments(followIndexName, numDocs + 6, "filtered_field:true"));
        } else {
            fail("unexpected target cluster [" + targetCluster + "]");
        }
    }

    public void testAutoFollowPatterns() throws Exception {
        if ("follow".equals(targetCluster) == false) {
            return;
        }
        Request putPatternRequest = new Request("PUT", "/_ccr/auto_follow/leader_cluster_pattern");
        putPatternRequest.setJsonEntity("{\"leader_index_patterns\": [\"logs-*\"], \"remote_cluster\": \"leader_cluster\"}");
        assertOK(client().performRequest(putPatternRequest));
        putPatternRequest = new Request("PUT", "/_ccr/auto_follow/middle_cluster_pattern");
        putPatternRequest.setJsonEntity("{\"leader_index_patterns\": [\"logs-*\"], \"remote_cluster\": \"middle_cluster\"}");
        assertOK(client().performRequest(putPatternRequest));
        try (RestClient leaderClient = buildLeaderClient()) {
            Settings settings = Settings.builder()
                .put("index.soft_deletes.enabled", true)
                .build();
            Request request = new Request("PUT", "/logs-20190101");
            request.setJsonEntity("{\"settings\": " + Strings.toString(settings) +
                ", \"mappings\": {\"_doc\": {\"properties\": {\"field\": {\"type\": \"keyword\"}}}} }");
            assertOK(leaderClient.performRequest(request));
            for (int i = 0; i < 5; i++) {
                String id = Integer.toString(i);
                index(leaderClient, "logs-20190101", id, "field", i, "filtered_field", "true");
            }
        }
        try (RestClient middleClient = buildMiddleClient()) {
            Settings settings = Settings.builder()
                .put("index.soft_deletes.enabled", true)
                .build();
            Request request = new Request("PUT", "/logs-20200101");
            request.setJsonEntity("{\"settings\": " + Strings.toString(settings) +
                ", \"mappings\": {\"_doc\": {\"properties\": {\"field\": {\"type\": \"keyword\"}}}} }");
            assertOK(middleClient.performRequest(request));
            for (int i = 0; i < 5; i++) {
                String id = Integer.toString(i);
                index(middleClient, "logs-20200101", id, "field", i, "filtered_field", "true");
            }
        }
        assertBusy(() -> {
            Request statsRequest = new Request("GET", "/_ccr/stats");
            Map<?, ?> response = toMap(client().performRequest(statsRequest));
            Map<?, ?> autoFollowStats = (Map<?, ?>) response.get("auto_follow_stats");
            assertThat(autoFollowStats.get("number_of_successful_follow_indices"), equalTo(2));

            ensureYellow("logs-20190101");
            ensureYellow("logs-20200101");
            verifyDocuments("logs-20190101", 5, "filtered_field:true");
            verifyDocuments("logs-20200101", 5, "filtered_field:true");
        });
    }

}
