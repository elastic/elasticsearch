/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;

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
        final boolean overrideNumberOfReplicas = randomBoolean();
        try (XContentBuilder bodyBuilder = JsonXContent.contentBuilder()) {
            bodyBuilder.startObject();
            {
                bodyBuilder.startArray("leader_index_patterns");
                {
                    bodyBuilder.value("metrics-*");
                }
                bodyBuilder.endArray();
                bodyBuilder.field("remote_cluster", "leader_cluster");
                if (overrideNumberOfReplicas) {
                    bodyBuilder.startObject("settings");
                    {
                        bodyBuilder.field("index.number_of_replicas", 0);
                    }
                    bodyBuilder.endObject();
                }
            }
            bodyBuilder.endObject();
            request.setJsonEntity(Strings.toString(bodyBuilder));
        }
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
            if (overrideNumberOfReplicas) {
                assertThat(getIndexSettingsAsMap("metrics-20210101"), hasEntry("index.number_of_replicas", "0"));
            } else {
                assertThat(getIndexSettingsAsMap("metrics-20210101"), hasEntry("index.number_of_replicas", "1"));
            }
        });
        assertBusy(() -> {
            verifyCcrMonitoring("metrics-20210101", "metrics-20210101");
            verifyAutoFollowMonitoring();
        }, 30, TimeUnit.SECONDS);
    }

    public void testPutAutoFollowPatternThatOverridesRequiredLeaderSetting() throws IOException {
        if ("follow".equals(targetCluster) == false) {
            logger.info("skipping test, waiting for target cluster [follow]" );
            return;
        }

        final Request request = new Request("PUT", "/_ccr/auto_follow/test_pattern");
        try (XContentBuilder bodyBuilder = JsonXContent.contentBuilder()) {
            bodyBuilder.startObject();
            {
                bodyBuilder.startArray("leader_index_patterns");
                {
                    bodyBuilder.value("metrics-*");
                }
                bodyBuilder.endArray();
                bodyBuilder.field("remote_cluster", "leader_cluster");
                bodyBuilder.startObject("settings");
                {
                    bodyBuilder.field("index.number_of_shards", 5);
                }
                bodyBuilder.endObject();
            }
            bodyBuilder.endObject();
            request.setJsonEntity(Strings.toString(bodyBuilder));
        }
        final ResponseException responseException = expectThrows(ResponseException.class, () -> client().performRequest(request));
        final Response response = responseException.getResponse();
        assertThat(response.getStatusLine().getStatusCode(), equalTo(400));
        final Map<String, Object> responseAsMap = entityAsMap(response);
        assertThat(responseAsMap, hasKey("error"));
        assertThat(responseAsMap.get("error"), instanceOf(Map.class));
        @SuppressWarnings("unchecked") final Map<Object, Object> error = (Map<Object, Object>) responseAsMap.get("error");
        assertThat(error, hasEntry("type", "illegal_argument_exception"));
        assertThat(
            error,
            hasEntry("reason", "can not put auto-follow pattern that could override leader settings {\"index.number_of_shards\":\"5\"}")
        );
    }

    private int getNumberOfSuccessfulFollowedIndices() throws IOException {
        Request statsRequest = new Request("GET", "/_ccr/stats");
        Map<?, ?> response = toMap(client().performRequest(statsRequest));
        response = (Map<?, ?>) response.get("auto_follow_stats");
        return (Integer) response.get("number_of_successful_follow_indices");
    }


}
