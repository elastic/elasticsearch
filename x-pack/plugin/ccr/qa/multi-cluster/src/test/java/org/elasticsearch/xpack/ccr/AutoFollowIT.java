/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

public class AutoFollowIT extends ESCCRRestTestCase {

    public void testMultipleAutoFollowPatternsDifferentClusters() throws Exception {
        if ("follow".equals(targetCluster) == false) {
            logger.info("skipping test, waiting for target cluster [follow]" );
            return;
        }

        int initialNumberOfSuccessfulFollowedIndices = getNumberOfSuccessfulFollowedIndices();
        Request putPatternRequest = new Request("PUT", "/_ccr/auto_follow/leader_cluster_pattern");
        putPatternRequest.setJsonEntity("{\"leader_index_patterns\": [\"logs-*\"], \"remote_cluster\": \"leader_cluster\"}");
        assertOK(client().performRequest(putPatternRequest));
        putPatternRequest = new Request("PUT", "/_ccr/auto_follow/middle_cluster_pattern");
        putPatternRequest.setJsonEntity("{\"leader_index_patterns\": [\"logs-*\"], \"remote_cluster\": \"middle_cluster\"}");
        assertOK(client().performRequest(putPatternRequest));
        try (RestClient leaderClient = buildLeaderClient()) {
            Request request = new Request("PUT", "/logs-20190101");
            request.setJsonEntity("{\"mappings\": {\"properties\": {\"field\": {\"type\": \"keyword\"}}}}");
            assertOK(leaderClient.performRequest(request));
            for (int i = 0; i < 5; i++) {
                String id = Integer.toString(i);
                index(leaderClient, "logs-20190101", id, "field", i, "filtered_field", "true");
            }
        }
        try (RestClient middleClient = buildMiddleClient()) {
            Request request = new Request("PUT", "/logs-20200101");
            request.setJsonEntity("{\"mappings\": {\"properties\": {\"field\": {\"type\": \"keyword\"}}}}");
            assertOK(middleClient.performRequest(request));
            for (int i = 0; i < 5; i++) {
                String id = Integer.toString(i);
                index(middleClient, "logs-20200101", id, "field", i, "filtered_field", "true");
            }
        }
        assertBusy(() -> {
            assertThat(getNumberOfSuccessfulFollowedIndices(), equalTo(initialNumberOfSuccessfulFollowedIndices + 2));

            ensureYellow("logs-20190101");
            ensureYellow("logs-20200101");
            verifyDocuments("logs-20190101", 5, "filtered_field:true");
            verifyDocuments("logs-20200101", 5, "filtered_field:true");
        });
    }

    public void testAutoFollowPatterns() throws Exception {
        if ("follow".equals(targetCluster) == false) {
            logger.info("skipping test, waiting for target cluster [follow]" );
            return;
        }

        int initialNumberOfSuccessfulFollowedIndices = getNumberOfSuccessfulFollowedIndices();
        Request request = new Request("PUT", "/_ccr/auto_follow/test_pattern");
        request.setJsonEntity("{\"leader_index_patterns\": [\"metrics-*\"], \"remote_cluster\": \"leader_cluster\"}");
        assertOK(client().performRequest(request));

        try (RestClient leaderClient = buildLeaderClient()) {
            request = new Request("PUT", "/metrics-20210101");
            request.setJsonEntity("{\"mappings\": {\"properties\": {\"field\": {\"type\": \"keyword\"}}}}");
            assertOK(leaderClient.performRequest(request));

            for (int i = 0; i < 5; i++) {
                String id = Integer.toString(i);
                index(leaderClient, "metrics-20210101", id, "field", i, "filtered_field", "true");
            }
        }

        assertBusy(() -> {
            assertThat(getNumberOfSuccessfulFollowedIndices(), equalTo(initialNumberOfSuccessfulFollowedIndices + 1));
            ensureYellow("metrics-20210101");
            verifyDocuments("metrics-20210101", 5, "filtered_field:true");
        });
        assertBusy(() -> {
            verifyCcrMonitoring("metrics-20210101", "metrics-20210101");
            verifyAutoFollowMonitoring();
        }, 30, TimeUnit.SECONDS);
    }

    private int getNumberOfSuccessfulFollowedIndices() throws IOException {
        Request statsRequest = new Request("GET", "/_ccr/stats");
        Map<?, ?> response = toMap(client().performRequest(statsRequest));
        response = (Map<?, ?>) response.get("auto_follow_stats");
        return (Integer) response.get("number_of_successful_follow_indices");
    }


}
