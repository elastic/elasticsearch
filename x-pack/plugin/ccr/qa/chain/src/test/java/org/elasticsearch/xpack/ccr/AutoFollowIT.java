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

public class AutoFollowIT extends ESCCRRestTestCase {

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
