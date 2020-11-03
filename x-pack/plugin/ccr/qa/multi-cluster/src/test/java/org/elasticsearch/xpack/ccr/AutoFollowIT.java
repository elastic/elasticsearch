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
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.rest.action.search.RestSearchAction.TOTAL_HITS_AS_INT_PARAM;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class AutoFollowIT extends ESCCRRestTestCase {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss", Locale.ROOT);

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
        deleteAutoFollowPattern("leader_cluster_pattern");
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
        deleteAutoFollowPattern("test_pattern");
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

    public void testDataStreams() throws Exception {
        if ("follow".equals(targetCluster) == false) {
            return;
        }

        final int numDocs = 64;
        final String dataStreamName = "logs-mysql-error";

        int initialNumberOfSuccessfulFollowedIndices = getNumberOfSuccessfulFollowedIndices();

        // Create auto follow pattern
        Request request = new Request("PUT", "/_ccr/auto_follow/test_pattern");
        try (XContentBuilder bodyBuilder = JsonXContent.contentBuilder()) {
            bodyBuilder.startObject();
            {
                bodyBuilder.startArray("leader_index_patterns");
                {
                    bodyBuilder.value("logs-*");
                }
                bodyBuilder.endArray();
                bodyBuilder.field("remote_cluster", "leader_cluster");
            }
            bodyBuilder.endObject();
            request.setJsonEntity(Strings.toString(bodyBuilder));
        }
        assertOK(client().performRequest(request));

        // Create data stream and ensure that is is auto followed
        {
            try (RestClient leaderClient = buildLeaderClient()) {
                for (int i = 0; i < numDocs; i++) {
                    Request indexRequest = new Request("POST", "/" + dataStreamName + "/_doc");
                    indexRequest.addParameter("refresh", "true");
                    indexRequest.setJsonEntity("{\"@timestamp\": \"" + DATE_FORMAT.format(new Date()) + "\",\"message\":\"abc\"}");
                    assertOK(leaderClient.performRequest(indexRequest));
                }
                verifyDataStream(leaderClient, dataStreamName, ".ds-logs-mysql-error-000001");
                verifyDocuments(leaderClient, dataStreamName, numDocs);
            }
            assertBusy(() -> {
                assertThat(getNumberOfSuccessfulFollowedIndices(), equalTo(initialNumberOfSuccessfulFollowedIndices + 1));
                verifyDataStream(client(), dataStreamName, ".ds-logs-mysql-error-000001");
                ensureYellow(dataStreamName);
                verifyDocuments(client(), dataStreamName, numDocs);
            });
        }

        // First rollover and ensure second backing index is replicated:
        {
            try (RestClient leaderClient = buildLeaderClient()) {
                Request rolloverRequest = new Request("POST", "/" +  dataStreamName + "/_rollover");
                assertOK(leaderClient.performRequest(rolloverRequest));
                verifyDataStream(leaderClient, dataStreamName, ".ds-logs-mysql-error-000001", ".ds-logs-mysql-error-000002");

                Request indexRequest = new Request("POST", "/" + dataStreamName + "/_doc");
                indexRequest.addParameter("refresh", "true");
                indexRequest.setJsonEntity("{\"@timestamp\": \"" + DATE_FORMAT.format(new Date()) + "\",\"message\":\"abc\"}");
                assertOK(leaderClient.performRequest(indexRequest));
                verifyDocuments(leaderClient, dataStreamName, numDocs + 1);
            }
            assertBusy(() -> {
                assertThat(getNumberOfSuccessfulFollowedIndices(), equalTo(initialNumberOfSuccessfulFollowedIndices + 2));
                verifyDataStream(client(), dataStreamName, ".ds-logs-mysql-error-000001", ".ds-logs-mysql-error-000002");
                ensureYellow(dataStreamName);
                verifyDocuments(client(), dataStreamName, numDocs + 1);
            });
        }

        // Second rollover and ensure third backing index is replicated:
        {
            try (RestClient leaderClient = buildLeaderClient()) {
                Request rolloverRequest = new Request("POST", "/" +  dataStreamName + "/_rollover");
                assertOK(leaderClient.performRequest(rolloverRequest));
                verifyDataStream(leaderClient, dataStreamName, ".ds-logs-mysql-error-000001", ".ds-logs-mysql-error-000002", "" +
                    ".ds-logs-mysql-error-000003");

                Request indexRequest = new Request("POST", "/" + dataStreamName + "/_doc");
                indexRequest.addParameter("refresh", "true");
                indexRequest.setJsonEntity("{\"@timestamp\": \"" + DATE_FORMAT.format(new Date()) + "\",\"message\":\"abc\"}");
                assertOK(leaderClient.performRequest(indexRequest));
                verifyDocuments(leaderClient, dataStreamName, numDocs + 2);
            }
            assertBusy(() -> {
                assertThat(getNumberOfSuccessfulFollowedIndices(), equalTo(initialNumberOfSuccessfulFollowedIndices + 3));
                verifyDataStream(client(), dataStreamName, ".ds-logs-mysql-error-000001", ".ds-logs-mysql-error-000002",
                    ".ds-logs-mysql-error-000003");
                ensureYellow(dataStreamName);
                verifyDocuments(client(), dataStreamName, numDocs + 2);
            });
        }
        // Cleanup:
        {
            deleteAutoFollowPattern("test_pattern");
            deleteDataStream(dataStreamName);
        }
    }

    public void testDataStreams_autoFollowAfterDataStreamCreated() throws Exception {
        if ("follow".equals(targetCluster) == false) {
            return;
        }

        final int initialNumDocs = 16;
        final String dataStreamName = "logs-syslog-prod";
        int initialNumberOfSuccessfulFollowedIndices = getNumberOfSuccessfulFollowedIndices();
        // Initialize data stream prior to auto following
        {
            try (RestClient leaderClient = buildLeaderClient()) {
                for (int i = 0; i < initialNumDocs; i++) {
                    Request indexRequest = new Request("POST", "/" + dataStreamName + "/_doc");
                    indexRequest.addParameter("refresh", "true");
                    indexRequest.setJsonEntity("{\"@timestamp\": \"" + DATE_FORMAT.format(new Date()) + "\",\"message\":\"abc\"}");
                    assertOK(leaderClient.performRequest(indexRequest));
                }
                verifyDataStream(leaderClient, dataStreamName, ".ds-logs-syslog-prod-000001");
                verifyDocuments(leaderClient, dataStreamName, initialNumDocs);
            }
        }
        // Create auto follow pattern
        {
            Request request = new Request("PUT", "/_ccr/auto_follow/test_pattern");
            try (XContentBuilder bodyBuilder = JsonXContent.contentBuilder()) {
                bodyBuilder.startObject();
                {
                    bodyBuilder.startArray("leader_index_patterns");
                    {
                        bodyBuilder.value("logs-*");
                    }
                    bodyBuilder.endArray();
                    bodyBuilder.field("remote_cluster", "leader_cluster");
                }
                bodyBuilder.endObject();
                request.setJsonEntity(Strings.toString(bodyBuilder));
            }
            assertOK(client().performRequest(request));
        }
        // Rollover and ensure only second backing index is replicated:
        {
            try (RestClient leaderClient = buildLeaderClient()) {
                Request rolloverRequest = new Request("POST", "/" +  dataStreamName + "/_rollover");
                assertOK(leaderClient.performRequest(rolloverRequest));
                verifyDataStream(leaderClient, dataStreamName, ".ds-logs-syslog-prod-000001", ".ds-logs-syslog-prod-000002");

                Request indexRequest = new Request("POST", "/" + dataStreamName + "/_doc");
                indexRequest.addParameter("refresh", "true");
                indexRequest.setJsonEntity("{\"@timestamp\": \"" + DATE_FORMAT.format(new Date()) + "\",\"message\":\"abc\"}");
                assertOK(leaderClient.performRequest(indexRequest));
                verifyDocuments(leaderClient, dataStreamName, initialNumDocs + 1);
            }
            assertBusy(() -> {
                assertThat(getNumberOfSuccessfulFollowedIndices(), equalTo(initialNumberOfSuccessfulFollowedIndices + 1));
                verifyDataStream(client(), dataStreamName, ".ds-logs-syslog-prod-000002");
                ensureYellow(dataStreamName);
                verifyDocuments(client(), dataStreamName, 1);
            });
        }
        // Explicitly follow the first backing index and check that the data stream in follow cluster is updated correctly:
        {
            followIndex(".ds-logs-syslog-prod-000001", ".ds-logs-syslog-prod-000001");
            assertBusy(() -> {
                assertThat(getNumberOfSuccessfulFollowedIndices(), equalTo(initialNumberOfSuccessfulFollowedIndices + 1));
                verifyDataStream(client(), dataStreamName, ".ds-logs-syslog-prod-000001", ".ds-logs-syslog-prod-000002");
                ensureYellow(dataStreamName);
                verifyDocuments(client(), dataStreamName, initialNumDocs + 1);
            });
        }
        // Cleanup:
        {
            deleteAutoFollowPattern("test_pattern");
            deleteDataStream(dataStreamName);
        }
    }

    private int getNumberOfSuccessfulFollowedIndices() throws IOException {
        Request statsRequest = new Request("GET", "/_ccr/stats");
        Map<?, ?> response = toMap(client().performRequest(statsRequest));
        response = (Map<?, ?>) response.get("auto_follow_stats");
        return (Integer) response.get("number_of_successful_follow_indices");
    }

    private static void verifyDocuments(final RestClient client,
                                        final String index,
                                        final int expectedNumDocs) throws IOException {
        final Request request = new Request("GET", "/" + index + "/_search");
        request.addParameter(TOTAL_HITS_AS_INT_PARAM, "true");
        Map<String, ?> response = toMap(client.performRequest(request));

        int numDocs = (int) XContentMapValues.extractValue("hits.total", response);
        assertThat(index, numDocs, equalTo(expectedNumDocs));
    }

    static void verifyDataStream(final RestClient client,
                                         final String name,
                                         final String... expectedBackingIndices) throws IOException {
        Request request = new Request("GET", "/_data_stream/" + name);
        Map<String, ?> response = toMap(client.performRequest(request));
        List<?> retrievedDataStreams = (List<?>) response.get("data_streams");
        assertThat(retrievedDataStreams, hasSize(1));
        List<?> actualBackingIndices = (List<?>) ((Map<?, ?>) retrievedDataStreams.get(0)).get("indices");
        assertThat(actualBackingIndices, hasSize(expectedBackingIndices.length));
        for (int i = 0; i < expectedBackingIndices.length; i++) {
            Map<?, ?> actualBackingIndex = (Map<?, ?>) actualBackingIndices.get(i);
            String expectedBackingIndex = expectedBackingIndices[i];
            assertThat(actualBackingIndex.get("index_name"), equalTo(expectedBackingIndex));
        }
    }

    private void deleteDataStream(String name) throws IOException {
        try (RestClient leaderClient = buildLeaderClient()) {
            Request deleteTemplateRequest = new Request("DELETE", "/_data_stream/" + name);
            assertOK(leaderClient.performRequest(deleteTemplateRequest));
        }
    }

}
