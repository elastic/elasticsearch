/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.WarningFailureException;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xcontent.ObjectPath.eval;
import static org.elasticsearch.xpack.core.ilm.ShrinkIndexNameSupplier.SHRUNKEN_INDEX_PREFIX;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class AutoFollowIT extends ESCCRRestTestCase {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss", Locale.ROOT);

    public void testMultipleAutoFollowPatternsDifferentClusters() throws Exception {
        if ("follow".equals(targetCluster) == false) {
            logger.info("skipping test, waiting for target cluster [follow]");
            return;
        }

        try {
            int initialNumberOfSuccessfulFollowedIndices = getNumberOfSuccessfulFollowedIndices();
            Request putPatternRequest = new Request("PUT", "/_ccr/auto_follow/leader_cluster_pattern");
            putPatternRequest.setJsonEntity("""
                {"leader_index_patterns": ["index-*"], "remote_cluster": "leader_cluster"}""");
            assertOK(client().performRequest(putPatternRequest));
            putPatternRequest = new Request("PUT", "/_ccr/auto_follow/middle_cluster_pattern");
            putPatternRequest.setJsonEntity("""
                {"leader_index_patterns": ["index-*"], "remote_cluster": "middle_cluster"}""");
            assertOK(client().performRequest(putPatternRequest));
            try (RestClient leaderClient = buildLeaderClient()) {
                Request request = new Request("PUT", "/index-20190101");
                request.setJsonEntity("""
                    {"mappings": {"properties": {"field": {"type": "keyword"}}}}""");
                assertOK(leaderClient.performRequest(request));
                for (int i = 0; i < 5; i++) {
                    String id = Integer.toString(i);
                    index(leaderClient, "index-20190101", id, "field", i, "filtered_field", "true");
                }
            }
            try (RestClient middleClient = buildMiddleClient()) {
                Request request = new Request("PUT", "/index-20200101");
                request.setJsonEntity("""
                    {"mappings": {"properties": {"field": {"type": "keyword"}}}}""");
                assertOK(middleClient.performRequest(request));
                for (int i = 0; i < 5; i++) {
                    String id = Integer.toString(i);
                    index(middleClient, "index-20200101", id, "field", i, "filtered_field", "true");
                }
            }
            assertBusy(() -> {
                assertThat(getNumberOfSuccessfulFollowedIndices(), equalTo(initialNumberOfSuccessfulFollowedIndices + 2));

                ensureYellow("index-20190101");
                ensureYellow("index-20200101");
                verifyDocuments("index-20190101", 5, "filtered_field:true");
                verifyDocuments("index-20200101", 5, "filtered_field:true");
            });

        } finally {
            cleanUpFollower(
                List.of("index-20190101", "index-20200101"),
                List.of(),
                List.of("leader_cluster_pattern", "middle_cluster_pattern")
            );
            cleanUpMiddle(List.of("index-20200101"), List.of(), List.of());
            cleanUpLeader(List.of("index-20190101"), List.of(), List.of());
        }
    }

    public void testAutoFollowPatterns() throws Exception {
        if ("follow".equals(targetCluster) == false) {
            logger.info("skipping test, waiting for target cluster [follow]");
            return;
        }

        final String autoFollowPatternName = getTestName().toLowerCase(Locale.ROOT);
        final String excludedIndex = "metrics-20210102";
        try {
            int initialNumberOfSuccessfulFollowedIndices = getNumberOfSuccessfulFollowedIndices();
            Request request = new Request("PUT", "/_ccr/auto_follow/" + autoFollowPatternName);
            final boolean overrideNumberOfReplicas = randomBoolean();
            try (XContentBuilder bodyBuilder = JsonXContent.contentBuilder()) {
                bodyBuilder.startObject();
                {
                    bodyBuilder.startArray("leader_index_patterns");
                    {
                        bodyBuilder.value("metrics-*");
                    }
                    bodyBuilder.endArray();
                    bodyBuilder.startArray("leader_index_exclusion_patterns");
                    {
                        bodyBuilder.value(excludedIndex);
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
                request = new Request("PUT", "/" + excludedIndex);
                request.setJsonEntity("""
                    {"mappings": {"properties": {"field": {"type": "keyword"}}}}""");
                assertOK(leaderClient.performRequest(request));
            }

            try (RestClient leaderClient = buildLeaderClient()) {
                request = new Request("PUT", "/metrics-20210101");
                request.setJsonEntity("""
                    {"mappings": {"properties": {"field": {"type": "keyword"}}}}""");
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
                assertThat(indexExists(excludedIndex), is(false));
            });

            assertLongBusy(() -> verifyCcrMonitoring("metrics-20210101", "metrics-20210101"));
            assertLongBusy(ESCCRRestTestCase::verifyAutoFollowMonitoring);

        } finally {
            cleanUpFollower(List.of("metrics-20210101"), List.of(), List.of(autoFollowPatternName));
            cleanUpLeader(List.of("metrics-20210101", excludedIndex), List.of(), List.of());
        }
    }

    public void testPutAutoFollowPatternThatOverridesRequiredLeaderSetting() throws IOException {
        if ("follow".equals(targetCluster) == false) {
            logger.info("skipping test, waiting for target cluster [follow]");
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
        @SuppressWarnings("unchecked")
        final Map<Object, Object> error = (Map<Object, Object>) responseAsMap.get("error");
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
        final String autoFollowPatternName = getTestName().toLowerCase(Locale.ROOT);

        int initialNumberOfSuccessfulFollowedIndices = getNumberOfSuccessfulFollowedIndices();
        try {
            // Create auto follow pattern
            createAutoFollowPattern(client(), autoFollowPatternName, "logs-mysql-*", "leader_cluster", null);

            // Create data stream and ensure that is is auto followed
            try (RestClient leaderClient = buildLeaderClient()) {
                for (int i = 0; i < numDocs; i++) {
                    Request indexRequest = new Request("POST", "/" + dataStreamName + "/_doc");
                    indexRequest.addParameter("refresh", "true");
                    indexRequest.setJsonEntity("{\"@timestamp\": \"" + DATE_FORMAT.format(new Date()) + "\",\"message\":\"abc\"}");
                    assertOK(leaderClient.performRequest(indexRequest));
                }
                verifyDataStream(leaderClient, dataStreamName, backingIndexName(dataStreamName, 1));
                verifyDocuments(leaderClient, dataStreamName, numDocs);
            }
            assertBusy(() -> {
                assertThat(getNumberOfSuccessfulFollowedIndices(), equalTo(initialNumberOfSuccessfulFollowedIndices + 1));
                verifyDataStream(client(), dataStreamName, backingIndexName(dataStreamName, 1));
                ensureYellow(dataStreamName);
                verifyDocuments(client(), dataStreamName, numDocs);
            });

            // First rollover and ensure second backing index is replicated:
            try (RestClient leaderClient = buildLeaderClient()) {
                Request rolloverRequest = new Request("POST", "/" + dataStreamName + "/_rollover");
                assertOK(leaderClient.performRequest(rolloverRequest));
                verifyDataStream(leaderClient, dataStreamName, backingIndexName(dataStreamName, 1), backingIndexName(dataStreamName, 2));

                Request indexRequest = new Request("POST", "/" + dataStreamName + "/_doc");
                indexRequest.addParameter("refresh", "true");
                indexRequest.setJsonEntity("{\"@timestamp\": \"" + DATE_FORMAT.format(new Date()) + "\",\"message\":\"abc\"}");
                assertOK(leaderClient.performRequest(indexRequest));
                verifyDocuments(leaderClient, dataStreamName, numDocs + 1);
            }
            assertBusy(() -> {
                assertThat(getNumberOfSuccessfulFollowedIndices(), equalTo(initialNumberOfSuccessfulFollowedIndices + 2));
                verifyDataStream(client(), dataStreamName, backingIndexName(dataStreamName, 1), backingIndexName(dataStreamName, 2));
                ensureYellow(dataStreamName);
                verifyDocuments(client(), dataStreamName, numDocs + 1);
            });

            // Second rollover and ensure third backing index is replicated:
            try (RestClient leaderClient = buildLeaderClient()) {
                Request rolloverRequest = new Request("POST", "/" + dataStreamName + "/_rollover");
                assertOK(leaderClient.performRequest(rolloverRequest));
                verifyDataStream(
                    leaderClient,
                    dataStreamName,
                    backingIndexName(dataStreamName, 1),
                    backingIndexName(dataStreamName, 2),
                    backingIndexName(dataStreamName, 3)
                );

                Request indexRequest = new Request("POST", "/" + dataStreamName + "/_doc");
                indexRequest.addParameter("refresh", "true");
                indexRequest.setJsonEntity("{\"@timestamp\": \"" + DATE_FORMAT.format(new Date()) + "\",\"message\":\"abc\"}");
                assertOK(leaderClient.performRequest(indexRequest));
                verifyDocuments(leaderClient, dataStreamName, numDocs + 2);
            }
            assertBusy(() -> {
                assertThat(getNumberOfSuccessfulFollowedIndices(), equalTo(initialNumberOfSuccessfulFollowedIndices + 3));
                verifyDataStream(
                    client(),
                    dataStreamName,
                    backingIndexName(dataStreamName, 1),
                    backingIndexName(dataStreamName, 2),
                    backingIndexName(dataStreamName, 3)
                );
                ensureYellow(dataStreamName);
                verifyDocuments(client(), dataStreamName, numDocs + 2);
            });

        } finally {
            cleanUpFollower(
                List.of(backingIndexName(dataStreamName, 1), backingIndexName(dataStreamName, 2), backingIndexName(dataStreamName, 3)),
                List.of(dataStreamName),
                List.of(autoFollowPatternName)
            );
            cleanUpLeader(
                List.of(backingIndexName(dataStreamName, 1), backingIndexName(dataStreamName, 2), backingIndexName(dataStreamName, 3)),
                List.of(dataStreamName),
                List.of()
            );
        }
    }

    public void testDataStreamsRenameFollowDataStream() throws Exception {
        if ("follow".equals(targetCluster) == false) {
            return;
        }

        final int numDocs = 64;
        final String dataStreamName = "logs-mysql-error";
        final String dataStreamNameFollower = "logs-mysql-error_copy";
        final String autoFollowPatternName = getTestName().toLowerCase(Locale.ROOT);

        int initialNumberOfSuccessfulFollowedIndices = getNumberOfSuccessfulFollowedIndices();
        try {
            // Create auto follow pattern
            createAutoFollowPattern(client(), autoFollowPatternName, "logs-mysql-*", "leader_cluster", "{{leader_index}}_copy");

            // Create data stream and ensure that is is auto followed
            try (RestClient leaderClient = buildLeaderClient()) {
                for (int i = 0; i < numDocs; i++) {
                    Request indexRequest = new Request("POST", "/" + dataStreamName + "/_doc");
                    indexRequest.addParameter("refresh", "true");
                    indexRequest.setJsonEntity("{\"@timestamp\": \"" + DATE_FORMAT.format(new Date()) + "\",\"message\":\"abc\"}");
                    assertOK(leaderClient.performRequest(indexRequest));
                }
                verifyDataStream(leaderClient, dataStreamName, backingIndexName(dataStreamName, 1));
                verifyDocuments(leaderClient, dataStreamName, numDocs);
            }
            logger.info(
                "--> checking {} with index {} has been auto followed to {} with backing index {}",
                dataStreamName,
                backingIndexName(dataStreamName, 1),
                dataStreamNameFollower,
                backingIndexName(dataStreamNameFollower, 1)
            );
            assertBusy(() -> {
                assertThat(getNumberOfSuccessfulFollowedIndices(), equalTo(initialNumberOfSuccessfulFollowedIndices + 1));
                verifyDataStream(client(), dataStreamNameFollower, backingIndexName(dataStreamNameFollower, 1));
                ensureYellow(dataStreamNameFollower);
                verifyDocuments(client(), dataStreamNameFollower, numDocs);
            });

            // First rollover and ensure second backing index is replicated:
            logger.info("--> rolling over");
            try (RestClient leaderClient = buildLeaderClient()) {
                Request rolloverRequest = new Request("POST", "/" + dataStreamName + "/_rollover");
                assertOK(leaderClient.performRequest(rolloverRequest));
                verifyDataStream(leaderClient, dataStreamName, backingIndexName(dataStreamName, 1), backingIndexName(dataStreamName, 2));

                Request indexRequest = new Request("POST", "/" + dataStreamName + "/_doc");
                indexRequest.addParameter("refresh", "true");
                indexRequest.setJsonEntity("{\"@timestamp\": \"" + DATE_FORMAT.format(new Date()) + "\",\"message\":\"abc\"}");
                assertOK(leaderClient.performRequest(indexRequest));
                verifyDocuments(leaderClient, dataStreamName, numDocs + 1);
            }
            assertBusy(() -> {
                assertThat(getNumberOfSuccessfulFollowedIndices(), equalTo(initialNumberOfSuccessfulFollowedIndices + 2));
                verifyDataStream(
                    client(),
                    dataStreamNameFollower,
                    backingIndexName(dataStreamNameFollower, 1),
                    backingIndexName(dataStreamNameFollower, 2)
                );
                ensureYellow(dataStreamNameFollower);
                verifyDocuments(client(), dataStreamNameFollower, numDocs + 1);
            });

            // Second rollover and ensure third backing index is replicated:
            logger.info("--> rolling over");
            try (RestClient leaderClient = buildLeaderClient()) {
                Request rolloverRequest = new Request("POST", "/" + dataStreamName + "/_rollover");
                assertOK(leaderClient.performRequest(rolloverRequest));
                verifyDataStream(
                    leaderClient,
                    dataStreamName,
                    backingIndexName(dataStreamName, 1),
                    backingIndexName(dataStreamName, 2),
                    backingIndexName(dataStreamName, 3)
                );

                Request indexRequest = new Request("POST", "/" + dataStreamName + "/_doc");
                indexRequest.addParameter("refresh", "true");
                indexRequest.setJsonEntity("{\"@timestamp\": \"" + DATE_FORMAT.format(new Date()) + "\",\"message\":\"abc\"}");
                assertOK(leaderClient.performRequest(indexRequest));
                verifyDocuments(leaderClient, dataStreamName, numDocs + 2);
            }
            assertBusy(() -> {
                assertThat(getNumberOfSuccessfulFollowedIndices(), equalTo(initialNumberOfSuccessfulFollowedIndices + 3));
                verifyDataStream(
                    client(),
                    dataStreamNameFollower,
                    backingIndexName(dataStreamNameFollower, 1),
                    backingIndexName(dataStreamNameFollower, 2),
                    backingIndexName(dataStreamNameFollower, 3)
                );
                ensureYellow(dataStreamNameFollower);
                verifyDocuments(client(), dataStreamNameFollower, numDocs + 2);
            });

        } finally {
            cleanUpFollower(
                List.of(
                    backingIndexName(dataStreamNameFollower, 1),
                    backingIndexName(dataStreamNameFollower, 2),
                    backingIndexName(dataStreamNameFollower, 3)
                ),
                List.of(dataStreamNameFollower),
                List.of(autoFollowPatternName)
            );
            cleanUpLeader(
                List.of(backingIndexName(dataStreamName, 1), backingIndexName(dataStreamName, 2), backingIndexName(dataStreamName, 3)),
                List.of(dataStreamName),
                List.of()
            );
        }
    }

    public void testDataStreams_autoFollowAfterDataStreamCreated() throws Exception {
        if ("follow".equals(targetCluster) == false) {
            return;
        }

        final int initialNumDocs = 16;
        int initialNumberOfSuccessfulFollowedIndices = getNumberOfSuccessfulFollowedIndices();
        // The data stream name shouldn't match with builtin ilm policies to avoid test instabilities.
        // (the manual rollover that happens in this test, may cause ilm to add `index.lifecycle.indexing_complete` setting,
        // which causes explicit follow index api call to fail in this test)
        final String dataStreamName = getTestName().toLowerCase(Locale.ROOT) + "-logs-syslog-prod";
        // Because the builtin logs template isn't used, a template should be defined here.
        Request putComposableIndexTemplateRequest = new Request("POST", "/_index_template/" + getTestName().toLowerCase(Locale.ROOT));
        putComposableIndexTemplateRequest.setJsonEntity("{\"index_patterns\":[\"" + dataStreamName + "*\"],\"data_stream\":{}}");
        assertOK(client().performRequest(putComposableIndexTemplateRequest));

        final String autoFollowPatternName = getTestName().toLowerCase(Locale.ROOT);
        try {
            // Initialize data stream prior to auto following
            try (RestClient leaderClient = buildLeaderClient()) {
                assertOK(leaderClient.performRequest(putComposableIndexTemplateRequest));

                for (int i = 0; i < initialNumDocs; i++) {
                    Request indexRequest = new Request("POST", "/" + dataStreamName + "/_doc");
                    indexRequest.addParameter("refresh", "true");
                    indexRequest.setJsonEntity("{\"@timestamp\": \"" + DATE_FORMAT.format(new Date()) + "\",\"message\":\"abc\"}");
                    assertOK(leaderClient.performRequest(indexRequest));
                }
                verifyDataStream(leaderClient, dataStreamName, backingIndexName(dataStreamName, 1));
                verifyDocuments(leaderClient, dataStreamName, initialNumDocs);
            }

            // Create auto follow pattern
            createAutoFollowPattern(client(), autoFollowPatternName, dataStreamName + "*", "leader_cluster", null);

            // Rollover and ensure only second backing index is replicated:
            try (RestClient leaderClient = buildLeaderClient()) {
                Request rolloverRequest = new Request("POST", "/" + dataStreamName + "/_rollover");
                assertOK(leaderClient.performRequest(rolloverRequest));
                verifyDataStream(leaderClient, dataStreamName, backingIndexName(dataStreamName, 1), backingIndexName(dataStreamName, 2));

                Request indexRequest = new Request("POST", "/" + dataStreamName + "/_doc");
                indexRequest.addParameter("refresh", "true");
                indexRequest.setJsonEntity("{\"@timestamp\": \"" + DATE_FORMAT.format(new Date()) + "\",\"message\":\"abc\"}");
                assertOK(leaderClient.performRequest(indexRequest));
                verifyDocuments(leaderClient, dataStreamName, initialNumDocs + 1);
            }
            assertBusy(() -> {
                assertThat(getNumberOfSuccessfulFollowedIndices(), equalTo(initialNumberOfSuccessfulFollowedIndices + 1));
                verifyDataStream(client(), dataStreamName, backingIndexName(dataStreamName, 2));
                ensureYellow(dataStreamName);
                verifyDocuments(client(), dataStreamName, 1);
            });

            // Explicitly follow the first backing index and check that the data stream in follow cluster is updated correctly:
            followIndex(backingIndexName(dataStreamName, 1), backingIndexName(dataStreamName, 1));
            assertBusy(() -> {
                assertThat(getNumberOfSuccessfulFollowedIndices(), equalTo(initialNumberOfSuccessfulFollowedIndices + 1));
                verifyDataStream(client(), dataStreamName, backingIndexName(dataStreamName, 1), backingIndexName(dataStreamName, 2));
                ensureYellow(dataStreamName);
                verifyDocuments(client(), dataStreamName, initialNumDocs + 1);
            });

        } finally {
            cleanUpFollower(
                List.of(backingIndexName(dataStreamName, 1), backingIndexName(dataStreamName, 2)),
                List.of(dataStreamName),
                List.of(autoFollowPatternName)
            );
            cleanUpLeader(
                List.of(backingIndexName(dataStreamName, 1), backingIndexName(dataStreamName, 2)),
                List.of(dataStreamName),
                List.of()
            );
        }
    }

    @SuppressWarnings("unchecked")
    public void testDataStreamsBackingIndicesOrdering() throws Exception {
        if ("follow".equals(targetCluster) == false) {
            return;
        }

        final int initialNumDocs = 16;
        int initialNumberOfSuccessfulFollowedIndices = getNumberOfSuccessfulFollowedIndices();
        final String dataStreamName = getTestName().toLowerCase(Locale.ROOT) + "-logs-syslog-prod";
        Request putComposableIndexTemplateRequest = new Request("POST", "/_index_template/" + getTestName().toLowerCase(Locale.ROOT));
        putComposableIndexTemplateRequest.setJsonEntity("{\"index_patterns\":[\"" + dataStreamName + "*\"],\"data_stream\":{}}");
        assertOK(client().performRequest(putComposableIndexTemplateRequest));

        final String autoFollowPatternName = getTestName().toLowerCase(Locale.ROOT);
        // Initialize data stream prior to auto following
        try (RestClient leaderClient = buildLeaderClient()) {
            assertOK(leaderClient.performRequest(putComposableIndexTemplateRequest));

            for (int i = 0; i < initialNumDocs; i++) {
                Request indexRequest = new Request("POST", "/" + dataStreamName + "/_doc");
                indexRequest.addParameter("refresh", "true");
                indexRequest.setJsonEntity("{\"@timestamp\": \"" + DATE_FORMAT.format(new Date()) + "\",\"message\":\"abc\"}");
                assertOK(leaderClient.performRequest(indexRequest));
            }
            verifyDataStream(leaderClient, dataStreamName, backingIndexName(dataStreamName, 1));
            verifyDocuments(leaderClient, dataStreamName, initialNumDocs);
        }

        // Create auto follow pattern
        createAutoFollowPattern(client(), autoFollowPatternName, dataStreamName + "*", "leader_cluster", null);

        // Rollover and ensure only second backing index is replicated:
        try (RestClient leaderClient = buildLeaderClient()) {
            Request rolloverRequest = new Request("POST", "/" + dataStreamName + "/_rollover");
            assertOK(leaderClient.performRequest(rolloverRequest));
            verifyDataStream(leaderClient, dataStreamName, backingIndexName(dataStreamName, 1), backingIndexName(dataStreamName, 2));

            Request indexRequest = new Request("POST", "/" + dataStreamName + "/_doc");
            indexRequest.addParameter("refresh", "true");
            indexRequest.setJsonEntity("{\"@timestamp\": \"" + DATE_FORMAT.format(new Date()) + "\",\"message\":\"abc\"}");
            assertOK(leaderClient.performRequest(indexRequest));
            verifyDocuments(leaderClient, dataStreamName, initialNumDocs + 1);

            assertOK(leaderClient.performRequest(rolloverRequest));
            assertOK(leaderClient.performRequest(indexRequest));
            verifyDataStream(
                leaderClient,
                dataStreamName,
                backingIndexName(dataStreamName, 1),
                backingIndexName(dataStreamName, 2),
                backingIndexName(dataStreamName, 3)
            );

        }

        assertBusy(() -> assertThat(indexExists(backingIndexName(dataStreamName, 2)), is(true)));
        assertBusy(() -> assertThat(indexExists(backingIndexName(dataStreamName, 3)), is(true)));

        // Replace a backing index in the follower data stream with one that has a prefix (simulating a shrink)

        String shrunkIndexName = SHRUNKEN_INDEX_PREFIX + DataStream.getDefaultBackingIndexName(dataStreamName, 2);
        Request indexRequest = new Request("POST", "/" + shrunkIndexName + "/_doc");
        indexRequest.addParameter("refresh", "true");
        indexRequest.setJsonEntity("{\"@timestamp\": \"" + DATE_FORMAT.format(new Date()) + "\",\"message\":\"abc\"}");
        client().performRequest(indexRequest);

        Request modifyDataStream = new Request("POST", "_data_stream/_modify");
        modifyDataStream.setJsonEntity(
            "{\n"
                + "  \"actions\": [\n"
                + "    {\n"
                + "      \"remove_backing_index\": {\n"
                + "        \"data_stream\": \""
                + dataStreamName
                + "\",\n"
                + "        \"index\": \""
                + DataStream.getDefaultBackingIndexName(dataStreamName, 2)
                + "\"\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"add_backing_index\": {\n"
                + "        \"data_stream\": \""
                + dataStreamName
                + "\",\n"
                + "        \"index\": \""
                + shrunkIndexName
                + "\"\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}"
        );
        client().performRequest(modifyDataStream);

        // Rollover the leader data stream so we replicate a new index to the following data stream (the 4th generation)
        try (RestClient leaderClient = buildLeaderClient()) {
            Request rolloverRequest = new Request("POST", "/" + dataStreamName + "/_rollover");
            assertOK(leaderClient.performRequest(rolloverRequest));
            verifyDataStream(
                leaderClient,
                dataStreamName,
                backingIndexName(dataStreamName, 1),
                backingIndexName(dataStreamName, 2),
                backingIndexName(dataStreamName, 3),
                backingIndexName(dataStreamName, 4)
            );

            indexRequest = new Request("POST", "/" + dataStreamName + "/_doc");
            indexRequest.addParameter("refresh", "true");
            indexRequest.setJsonEntity("{\"@timestamp\": \"" + DATE_FORMAT.format(new Date()) + "\",\"message\":\"abc\"}");
            assertOK(leaderClient.performRequest(indexRequest));
        }

        assertBusy(() -> {
            Request request = new Request("GET", "/_data_stream/" + dataStreamName);
            Map<String, ?> response = toMap(client().performRequest(request));
            List<?> retrievedDataStreams = (List<?>) response.get("data_streams");
            List<?> actualBackingIndexItems = (List<?>) ((Map<?, ?>) retrievedDataStreams.get(0)).get("indices");
            assertThat(actualBackingIndexItems.size(), is(3));
            Map<String, String> writeIndexMap = (Map<String, String>) actualBackingIndexItems.get(2);
            assertThat(writeIndexMap.get("index_name"), not(shrunkIndexName));
            assertThat(writeIndexMap.get("index_name"), is(backingIndexName(dataStreamName, 4)));
            assertThat(getNumberOfSuccessfulFollowedIndices(), equalTo(initialNumberOfSuccessfulFollowedIndices + 3));
        });
    }

    public void testRolloverDataStreamInFollowClusterForbidden() throws Exception {
        if ("follow".equals(targetCluster) == false) {
            return;
        }

        final int numDocs = 64;
        final var dataStreamName = "logs-tomcat-prod";
        final String autoFollowPatternName = getTestName().toLowerCase(Locale.ROOT);

        int initialNumberOfSuccessfulFollowedIndices = getNumberOfSuccessfulFollowedIndices();
        List<String> backingIndexNames = null;
        try {
            // Create auto follow pattern
            createAutoFollowPattern(client(), autoFollowPatternName, "logs-tomcat-*", "leader_cluster", null);

            // Create data stream and ensure that is is auto followed
            try (var leaderClient = buildLeaderClient()) {
                for (int i = 0; i < numDocs; i++) {
                    var indexRequest = new Request("POST", "/" + dataStreamName + "/_doc");
                    indexRequest.addParameter("refresh", "true");
                    indexRequest.setJsonEntity("{\"@timestamp\": \"" + DATE_FORMAT.format(new Date()) + "\",\"message\":\"abc\"}");
                    assertOK(leaderClient.performRequest(indexRequest));
                }
                verifyDataStream(leaderClient, dataStreamName, backingIndexName(dataStreamName, 1));
                verifyDocuments(leaderClient, dataStreamName, numDocs);
            }
            assertBusy(() -> {
                assertThat(getNumberOfSuccessfulFollowedIndices(), equalTo(initialNumberOfSuccessfulFollowedIndices + 1));
                verifyDataStream(client(), dataStreamName, backingIndexName(dataStreamName, 1));
                ensureYellow(dataStreamName);
                verifyDocuments(client(), dataStreamName, numDocs);
            });

            // Rollover in leader cluster and ensure second backing index is replicated:
            try (var leaderClient = buildLeaderClient()) {
                var rolloverRequest = new Request("POST", "/" + dataStreamName + "/_rollover");
                assertOK(leaderClient.performRequest(rolloverRequest));
                verifyDataStream(leaderClient, dataStreamName, backingIndexName(dataStreamName, 1), backingIndexName(dataStreamName, 2));

                var indexRequest = new Request("POST", "/" + dataStreamName + "/_doc");
                indexRequest.addParameter("refresh", "true");
                indexRequest.setJsonEntity("{\"@timestamp\": \"" + DATE_FORMAT.format(new Date()) + "\",\"message\":\"abc\"}");
                assertOK(leaderClient.performRequest(indexRequest));
                verifyDocuments(leaderClient, dataStreamName, numDocs + 1);
            }
            assertBusy(() -> {
                assertThat(getNumberOfSuccessfulFollowedIndices(), equalTo(initialNumberOfSuccessfulFollowedIndices + 2));
                verifyDataStream(client(), dataStreamName, backingIndexName(dataStreamName, 1), backingIndexName(dataStreamName, 2));
                ensureYellow(dataStreamName);
                verifyDocuments(client(), dataStreamName, numDocs + 1);
            });

            // Try rollover in follow cluster
            {
                var rolloverRequest1 = new Request("POST", "/" + dataStreamName + "/_rollover");
                var e = expectThrows(ResponseException.class, () -> client().performRequest(rolloverRequest1));
                assertThat(
                    e.getMessage(),
                    containsString(
                        "data stream [" + dataStreamName + "] cannot be rolled over, " + "because it is a replicated data stream"
                    )
                );
                verifyDataStream(client(), dataStreamName, backingIndexName(dataStreamName, 1), backingIndexName(dataStreamName, 2));

                // Unfollow .ds-logs-tomcat-prod-000001
                pauseFollow(backingIndexName(dataStreamName, 1));
                closeIndex(backingIndexName(dataStreamName, 1));
                unfollow(backingIndexName(dataStreamName, 1));

                // Try again
                var rolloverRequest2 = new Request("POST", "/" + dataStreamName + "/_rollover");
                e = expectThrows(ResponseException.class, () -> client().performRequest(rolloverRequest2));
                assertThat(
                    e.getMessage(),
                    containsString(
                        "data stream [" + dataStreamName + "] cannot be rolled over, " + "because it is a replicated data stream"
                    )
                );
                verifyDataStream(client(), dataStreamName, backingIndexName(dataStreamName, 1), backingIndexName(dataStreamName, 2));

                // Promote local data stream
                var promoteRequest = new Request("POST", "/_data_stream/_promote/" + dataStreamName);
                assertOK(client().performRequest(promoteRequest));

                // Try again and now the rollover should be successful because local data stream is now :
                var rolloverRequest3 = new Request("POST", "/" + dataStreamName + "/_rollover");
                assertOK(client().performRequest(rolloverRequest3));
                backingIndexNames = verifyDataStream(
                    client(),
                    dataStreamName,
                    backingIndexName(dataStreamName, 1),
                    backingIndexName(dataStreamName, 2),
                    backingIndexName(dataStreamName, 3)
                );

                // TODO: verify that following a backing index for logs-tomcat-prod data stream in remote cluster fails,
                // because local data stream isn't a replicated data stream anymore.

                // Unfollow .ds-logs-tomcat-prod-000002,
                // which is now possible because this index can now be closed as it is no longer the write index.
                // use the backing index name returned from the verify call so we are guaranteed to use the correct index name even if the
                // date rolled over
                final String backingIndexNameGen2 = backingIndexNames.get(1);
                pauseFollow(backingIndexNameGen2);
                closeIndex(backingIndexNameGen2);
                unfollow(backingIndexNameGen2);
            }

        } finally {
            if (backingIndexNames == null) {
                // we failed to compute the actual backing index names in the test because we failed earlier on, guessing them on a
                // best-effort basis
                backingIndexNames = List.of(
                    backingIndexName(dataStreamName, 1),
                    backingIndexName(dataStreamName, 2),
                    backingIndexName(dataStreamName, 3)
                );
            }
            cleanUpFollower(backingIndexNames, List.of(dataStreamName), List.of(autoFollowPatternName));
            cleanUpLeader(backingIndexNames.subList(0, 2), List.of(dataStreamName), List.of());
        }
    }

    public void testRolloverAliasInFollowClusterForbidden() throws Exception {
        if ("follow".equals(targetCluster) == false) {
            return;
        }

        final int numDocs = 64;
        final var aliasName = "log-tomcat-prod";

        int initialNumberOfSuccessfulFollowedIndices = getNumberOfSuccessfulFollowedIndices();
        try {
            // Create auto follow pattern
            createAutoFollowPattern(client(), "test_pattern", "log-*", "leader_cluster", null);

            // Create leader index and write alias:
            try (var leaderClient = buildLeaderClient()) {
                var createFirstIndexRequest = new Request("PUT", "/" + aliasName + "-000001");
                createFirstIndexRequest.setJsonEntity("{\"aliases\": {\"" + aliasName + "\":{\"is_write_index\":true}}}");
                leaderClient.performRequest(createFirstIndexRequest);

                for (int i = 0; i < numDocs; i++) {
                    var indexRequest = new Request("POST", "/" + aliasName + "/_doc");
                    indexRequest.addParameter("refresh", "true");
                    indexRequest.setJsonEntity("{\"@timestamp\": \"" + DATE_FORMAT.format(new Date()) + "\",\"message\":\"abc\"}");
                    assertOK(leaderClient.performRequest(indexRequest));
                }
                verifyAlias(leaderClient, aliasName, true, aliasName + "-000001");
                verifyDocuments(leaderClient, aliasName, numDocs);
            }
            assertBusy(() -> {
                assertThat(getNumberOfSuccessfulFollowedIndices(), equalTo(initialNumberOfSuccessfulFollowedIndices + 1));
                verifyAlias(client(), aliasName, false, aliasName + "-000001");
                ensureYellow(aliasName);
                verifyDocuments(client(), aliasName, numDocs);
            });

            // Rollover in leader cluster and ensure second backing index is replicated:
            try (var leaderClient = buildLeaderClient()) {
                var rolloverRequest = new Request("POST", "/" + aliasName + "/_rollover");
                assertOK(leaderClient.performRequest(rolloverRequest));
                verifyAlias(leaderClient, aliasName, true, aliasName + "-000002", aliasName + "-000001");

                var indexRequest = new Request("POST", "/" + aliasName + "/_doc");
                indexRequest.addParameter("refresh", "true");
                indexRequest.setJsonEntity("{\"@timestamp\": \"" + DATE_FORMAT.format(new Date()) + "\",\"message\":\"abc\"}");
                assertOK(leaderClient.performRequest(indexRequest));
                verifyDocuments(leaderClient, aliasName, numDocs + 1);
            }
            assertBusy(() -> {
                assertThat(getNumberOfSuccessfulFollowedIndices(), equalTo(initialNumberOfSuccessfulFollowedIndices + 2));
                verifyAlias(client(), aliasName, false, aliasName + "-000002", aliasName + "-000001");
                ensureYellow(aliasName);
                verifyDocuments(client(), aliasName, numDocs + 1);
            });

            // Try rollover in follow cluster, this should fail, because is_write_index property of an alias isn't
            // replicated to follow cluster.
            {
                var rolloverRequest1 = new Request("POST", "/" + aliasName + "/_rollover");
                var e = expectThrows(ResponseException.class, () -> client().performRequest(rolloverRequest1));
                assertThat(e.getMessage(), containsString("rollover target [" + aliasName + "] does not point to a write index"));
                verifyAlias(client(), aliasName, false, aliasName + "-000002", aliasName + "-000001");
            }

        } finally {
            cleanUpFollower(List.of(aliasName + "-000001", aliasName + "-000002"), List.of(), List.of("test_pattern"));
        }
    }

    private static void verifyAlias(RestClient client, String aliasName, boolean checkWriteIndex, String... otherIndices)
        throws IOException {
        try {
            var getAliasRequest = new Request("GET", "/_alias/" + aliasName);
            var responseBody = toMap(client.performRequest(getAliasRequest));
            if (checkWriteIndex) {
                assertThat(ObjectPath.eval(otherIndices[0] + ".aliases." + aliasName + ".is_write_index", responseBody), is(true));
            }
            for (String otherIndex : otherIndices) {
                assertThat(ObjectPath.eval(otherIndex + ".aliases." + aliasName, responseBody), notNullValue());
            }
        } catch (ResponseException e) {
            throw new AssertionError("get alias call failed", e);
        }
    }

    public void testDataStreamsBiDirectionalReplication() throws Exception {
        if ("follow".equals(targetCluster) == false) {
            return;
        }

        int initialNumberOfSuccessfulFollowedIndicesInFollowCluster = getNumberOfSuccessfulFollowedIndices();
        int initialNumberOfSuccessfulFollowedIndicesInLeaderCluster;

        var aliasName = "logs-http";
        var leaderDataStreamName = "logs-http-eu";
        var followerDataStreamName = "logs-http-na";

        try {
            // Create auto follow pattern in follow cluster
            createAutoFollowPattern(client(), "id1", "logs-*-eu", "leader_cluster", null);

            // Create auto follow pattern in leader cluster:
            try (var leaderClient = buildLeaderClient()) {
                initialNumberOfSuccessfulFollowedIndicesInLeaderCluster = getNumberOfSuccessfulFollowedIndices(leaderClient);
                // First add remote cluster to leader cluster:
                var request = new Request("PUT", "/_cluster/settings");
                try (var bodyBuilder = JsonXContent.contentBuilder()) {
                    bodyBuilder.startObject();
                    {
                        bodyBuilder.startObject("persistent");
                        {
                            bodyBuilder.startObject("cluster");
                            {
                                bodyBuilder.startObject("remote");
                                {
                                    bodyBuilder.startObject("follower_cluster");
                                    {
                                        bodyBuilder.startArray("seeds");
                                        var nodesInfoRequest = new Request("GET", "/_nodes/_local");
                                        var nodesInfoResponse = toMap(client().performRequest(nodesInfoRequest));
                                        var node = (Map<?, ?>) ((Map<?, ?>) nodesInfoResponse.get("nodes")).values().iterator().next();
                                        var transportMetrics = (Map<?, ?>) node.get("transport");
                                        var address = (String) transportMetrics.get("publish_address");
                                        bodyBuilder.value(address);
                                        bodyBuilder.endArray();
                                    }
                                    bodyBuilder.endObject();
                                }
                                bodyBuilder.endObject();
                            }
                            bodyBuilder.endObject();
                        }
                        bodyBuilder.endObject();
                    }
                    bodyBuilder.endObject();
                    request.setJsonEntity(Strings.toString(bodyBuilder));
                }
                assertOK(leaderClient.performRequest(request));
                // Then create the actual auto follow pattern:
                createAutoFollowPattern(leaderClient, "id2", "logs-*-na", "follower_cluster", null);
            }

            var numDocs = 128;
            // Create data stream in leader cluster and ensure it is followed in follow cluster
            try (var leaderClient = buildLeaderClient()) {
                // Setting up data stream and alias with write flag in leader cluster:
                Request createDataStreamRequest = new Request("PUT", "/_data_stream/" + leaderDataStreamName);
                assertOK(leaderClient.performRequest(createDataStreamRequest));
                Request updateAliasesRequest = new Request("POST", "/_aliases");
                updateAliasesRequest.setJsonEntity(format("""
                    {
                      "actions": [ { "add": { "index": "%s", "alias": "logs-http", "is_write_index": true } } ]
                    }""", leaderDataStreamName));
                assertOK(leaderClient.performRequest(updateAliasesRequest));

                for (int i = 0; i < numDocs; i++) {
                    Request indexRequest = new Request("POST", "/" + aliasName + "/_doc");
                    indexRequest.addParameter("refresh", "true");
                    indexRequest.setJsonEntity("{\"@timestamp\": \"" + DATE_FORMAT.format(new Date()) + "\",\"message\":\"abc\"}");
                    assertOK(leaderClient.performRequest(indexRequest));
                }
                verifyDataStream(leaderClient, leaderDataStreamName, backingIndexName(leaderDataStreamName, 1));
                verifyDocuments(leaderClient, leaderDataStreamName, numDocs);
            }
            assertBusy(() -> {
                assertThat(getNumberOfSuccessfulFollowedIndices(), equalTo(initialNumberOfSuccessfulFollowedIndicesInFollowCluster + 1));
                verifyDataStream(client(), leaderDataStreamName, backingIndexName(leaderDataStreamName, 1));
                ensureYellow(leaderDataStreamName);
                verifyDocuments(client(), leaderDataStreamName, numDocs);
            });
            // Setting up data stream and alias with write flag in follower cluster:
            Request createDataStreamRequest = new Request("PUT", "/_data_stream/" + followerDataStreamName);
            assertOK(client().performRequest(createDataStreamRequest));
            Request updateAliasesRequest = new Request("POST", "/_aliases");
            updateAliasesRequest.setJsonEntity(format("""
                {
                  "actions": [ { "add": { "index": "%s", "alias": "logs-http", "is_write_index": true } } ]
                }""", followerDataStreamName));
            assertOK(client().performRequest(updateAliasesRequest));

            for (int i = 0; i < numDocs; i++) {
                var indexRequest = new Request("POST", "/" + aliasName + "/_doc");
                indexRequest.addParameter("refresh", "true");
                indexRequest.setJsonEntity("{\"@timestamp\": \"" + DATE_FORMAT.format(new Date()) + "\",\"message\":\"abc\"}");
                assertOK(client().performRequest(indexRequest));
            }
            verifyDocuments(client(), followerDataStreamName, numDocs);

            // TODO: Don't update logs-http alias in follower cluster when data streams are automatically replicated
            // from leader to follower cluster:
            // (only set the write flag to logs-http-na)
            // Create alias in follower cluster that point to leader and follower data streams:
            updateAliasesRequest = new Request("POST", "/_aliases");
            updateAliasesRequest.setJsonEntity(format("""
                {
                  "actions": [ { "add": { "index": "%s", "alias": "logs-http" } } ]
                }""", leaderDataStreamName));
            assertOK(client().performRequest(updateAliasesRequest));

            try (var leaderClient = buildLeaderClient()) {
                assertBusy(() -> {
                    assertThat(
                        getNumberOfSuccessfulFollowedIndices(leaderClient),
                        equalTo(initialNumberOfSuccessfulFollowedIndicesInLeaderCluster + 1)
                    );
                    verifyDataStream(leaderClient, followerDataStreamName, backingIndexName(followerDataStreamName, 1));
                    ensureYellow(followerDataStreamName);
                    verifyDocuments(leaderClient, followerDataStreamName, numDocs);
                });
                updateAliasesRequest = new Request("POST", "/_aliases");
                updateAliasesRequest.setJsonEntity(format("""
                    {
                      "actions": [ { "add": { "index": "%s", "alias": "logs-http" } } ]
                    }""", followerDataStreamName));
                assertOK(leaderClient.performRequest(updateAliasesRequest));
            }

            // See all eu and na logs in leader and follower cluster:
            verifyDocuments(client(), aliasName, numDocs * 2);
            try (var leaderClient = buildLeaderClient()) {
                verifyDocuments(leaderClient, aliasName, numDocs * 2);
            }

            int moreDocs = 48;
            // Index more docs into leader cluster
            {
                try (var leaderClient = buildLeaderClient()) {
                    for (int i = 0; i < moreDocs; i++) {
                        var indexRequest = new Request("POST", "/" + aliasName + "/_doc");
                        indexRequest.addParameter("refresh", "true");
                        indexRequest.setJsonEntity("{\"@timestamp\": \"" + DATE_FORMAT.format(new Date()) + "\",\"message\":\"abc\"}");
                        assertOK(leaderClient.performRequest(indexRequest));
                    }
                    verifyDocuments(leaderClient, leaderDataStreamName, numDocs + moreDocs);
                }
                assertBusy(() -> { verifyDocuments(client(), leaderDataStreamName, numDocs + moreDocs); });
            }
            // Index more docs into follower cluster
            {
                for (int i = 0; i < moreDocs; i++) {
                    var indexRequest = new Request("POST", "/" + aliasName + "/_doc");
                    indexRequest.addParameter("refresh", "true");
                    indexRequest.setJsonEntity("{\"@timestamp\": \"" + DATE_FORMAT.format(new Date()) + "\",\"message\":\"abc\"}");
                    assertOK(client().performRequest(indexRequest));
                }
                verifyDocuments(client(), followerDataStreamName, numDocs + moreDocs);
                try (var leaderClient = buildLeaderClient()) {
                    assertBusy(() -> { verifyDocuments(leaderClient, followerDataStreamName, numDocs + moreDocs); });
                }
            }

            // See all eu and na logs in leader and follower cluster:
            verifyDocuments(client(), aliasName, (numDocs + moreDocs) * 2);
            try (RestClient leaderClient = buildLeaderClient()) {
                verifyDocuments(leaderClient, aliasName, (numDocs + moreDocs) * 2);
            }
        } finally {
            cleanUpFollower(
                List.of(backingIndexName(followerDataStreamName, 1), backingIndexName(leaderDataStreamName, 1)),
                List.of(followerDataStreamName, leaderDataStreamName),
                List.of("id1")
            );
            cleanUpLeader(
                List.of(backingIndexName(leaderDataStreamName, 1), backingIndexName(followerDataStreamName, 1)),
                List.of(leaderDataStreamName, followerDataStreamName),
                List.of("id2")
            );
        }
    }

    public void testAutoFollowSearchableSnapshotsFails() throws Exception {
        if ("follow".equals(targetCluster) == false) {
            return;
        }

        final String testPrefix = getTestName().toLowerCase(Locale.ROOT);
        final String repository = testPrefix + "-repository";
        final String snapshot = testPrefix + "-snapshot";

        // Create a repository and a snapshot of a 5 docs index on leader
        final String indexName = testPrefix + "-index";
        try {
            try (var leaderClient = buildLeaderClient()) {
                final String systemPropertyRepoPath = System.getProperty("tests.leader_cluster_repository_path");
                assertThat(
                    "Missing system property [tests.leader_cluster_repository_path]",
                    systemPropertyRepoPath,
                    not(emptyOrNullString())
                );
                final String repositoryPath = systemPropertyRepoPath + '/' + testPrefix;

                registerRepository(leaderClient, repository, "fs", true, Settings.builder().put("location", repositoryPath).build());

                for (int i = 0; i < 5; i++) {
                    var indexRequest = new Request("POST", "/" + indexName + "/_doc");
                    indexRequest.addParameter("refresh", "true");
                    indexRequest.setJsonEntity("{\"value\":" + i + "}");
                    assertOK(leaderClient.performRequest(indexRequest));
                }
                verifyDocuments(leaderClient, indexName, 5);

                deleteSnapshot(leaderClient, repository, snapshot, true);
                createSnapshot(leaderClient, repository, snapshot, true);
            }
        } finally {
            cleanUpLeader(List.of(indexName), List.of(), List.of());
        }

        final String autoFollowPattern = "pattern-" + testPrefix;
        final String regularIndex = testPrefix + "-regular";
        final String mountedIndex = testPrefix + "-mounted";

        try {
            createAutoFollowPattern(client(), autoFollowPattern, testPrefix + "-*", "leader_cluster", null);

            // Create a regular index on leader
            try (var leaderClient = buildLeaderClient()) {
                for (int i = 0; i < 10; i++) {
                    var indexRequest = new Request("POST", "/" + regularIndex + "/_doc");
                    indexRequest.addParameter("refresh", "true");
                    indexRequest.setJsonEntity("{\"value\":" + i + "}");
                    assertOK(leaderClient.performRequest(indexRequest));
                }
                verifyDocuments(leaderClient, regularIndex, 10);
            }

            // Mount the snapshot on leader
            try (var leaderClient = buildLeaderClient()) {
                final Request mountRequest = new Request(HttpPost.METHOD_NAME, "/_snapshot/" + repository + '/' + snapshot + "/_mount");
                mountRequest.setJsonEntity("{\"index\": \"" + indexName + "\",\"renamed_index\": \"" + mountedIndex + "\"}");
                final Response mountResponse = leaderClient.performRequest(mountRequest);
                assertThat(mountResponse.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
                ensureYellow(mountedIndex, leaderClient);
            }

            assertLongBusy(() -> {
                Map<?, ?> response = toMap(getAutoFollowStats());
                assertThat(eval("auto_follow_stats.number_of_failed_follow_indices", response), greaterThanOrEqualTo(1));
                assertThat(eval("auto_follow_stats.recent_auto_follow_errors", response), hasSize(greaterThanOrEqualTo(1)));
                assertThat(
                    eval("auto_follow_stats.recent_auto_follow_errors.0.auto_follow_exception.reason", response),
                    containsString(
                        "index to follow ["
                            + mountedIndex
                            + "] is a searchable snapshot index and cannot be used "
                            + "for cross-cluster replication purpose"
                    )
                );
                ensureYellow(regularIndex);
                verifyDocuments(client(), regularIndex, 10);
            });
        } finally {
            cleanUpLeader(List.of(regularIndex, mountedIndex), List.of(), List.of());
            cleanUpFollower(List.of(regularIndex), List.of(), List.of(autoFollowPattern));
        }
    }

    public void testNoWarningOnPromoteDatastreamWhenTemplateExistsOnFollower() throws Exception {
        if ("follow".equals(targetCluster) == false) {
            return;
        }
        testDataStreamPromotionWarnings(true);
    }

    public void testWarningOnPromoteDatastreamWhenTemplateDoesNotExistsOnFollower() {
        if ("follow".equals(targetCluster) == false) {
            return;
        }
        WarningFailureException exception = assertThrows(WarningFailureException.class, () -> testDataStreamPromotionWarnings(false));
        assertThat(
            exception.getMessage(),
            containsString(
                "does not have a matching index template. This will cause rollover to fail until a matching index template is created]"
            )
        );
    }

    private void testDataStreamPromotionWarnings(Boolean createFollowerTemplate) throws Exception {
        final int numDocs = 64;
        final String dataStreamName = getTestName().toLowerCase(Locale.ROOT) + "-dopromo";
        final String autoFollowPatternName = getTestName().toLowerCase(Locale.ROOT);

        int initialNumberOfSuccessfulFollowedIndices = getNumberOfSuccessfulFollowedIndices();
        List<String> backingIndexNames = null;
        try {
            // Create index template
            Request putComposableIndexTemplateRequest = new Request("POST", "/_index_template/" + getTestName().toLowerCase(Locale.ROOT));
            putComposableIndexTemplateRequest.setJsonEntity("{\"index_patterns\":[\"" + dataStreamName + "*\"],\"data_stream\":{}}");

            if (createFollowerTemplate) {
                assertOK(client().performRequest(putComposableIndexTemplateRequest));
            }

            // Create auto follow pattern
            createAutoFollowPattern(client(), autoFollowPatternName, dataStreamName + "*", "leader_cluster", null);

            // Create data stream and ensure that it is auto followed
            try (var leaderClient = buildLeaderClient()) {
                assertOK(leaderClient.performRequest(putComposableIndexTemplateRequest));

                for (int i = 0; i < numDocs; i++) {
                    var indexRequest = new Request("POST", "/" + dataStreamName + "/_doc");
                    indexRequest.addParameter("refresh", "true");
                    indexRequest.setJsonEntity("{\"@timestamp\": \"" + DATE_FORMAT.format(new Date()) + "\",\"message\":\"abc\"}");
                    assertOK(leaderClient.performRequest(indexRequest));
                }
                verifyDataStream(leaderClient, dataStreamName, backingIndexName(dataStreamName, 1));
                verifyDocuments(leaderClient, dataStreamName, numDocs);
            }
            assertBusy(() -> {
                assertThat(getNumberOfSuccessfulFollowedIndices(), equalTo(initialNumberOfSuccessfulFollowedIndices + 1));
                verifyDataStream(client(), dataStreamName, backingIndexName(dataStreamName, 1));
                ensureYellow(dataStreamName);
                verifyDocuments(client(), dataStreamName, numDocs);
            });

            // Rollover in leader cluster and ensure second backing index is replicated:
            try (var leaderClient = buildLeaderClient()) {
                var rolloverRequest = new Request("POST", "/" + dataStreamName + "/_rollover");
                assertOK(leaderClient.performRequest(rolloverRequest));
                verifyDataStream(leaderClient, dataStreamName, backingIndexName(dataStreamName, 1), backingIndexName(dataStreamName, 2));

                var indexRequest = new Request("POST", "/" + dataStreamName + "/_doc");
                indexRequest.addParameter("refresh", "true");
                indexRequest.setJsonEntity("{\"@timestamp\": \"" + DATE_FORMAT.format(new Date()) + "\",\"message\":\"abc\"}");
                assertOK(leaderClient.performRequest(indexRequest));
                verifyDocuments(leaderClient, dataStreamName, numDocs + 1);
            }
            assertBusy(() -> {
                assertThat(getNumberOfSuccessfulFollowedIndices(), equalTo(initialNumberOfSuccessfulFollowedIndices + 2));
                verifyDataStream(client(), dataStreamName, backingIndexName(dataStreamName, 1), backingIndexName(dataStreamName, 2));
                ensureYellow(dataStreamName);
                verifyDocuments(client(), dataStreamName, numDocs + 1);
            });

            backingIndexNames = verifyDataStream(
                client(),
                dataStreamName,
                backingIndexName(dataStreamName, 1),
                backingIndexName(dataStreamName, 2)
            );

            // Promote local data stream
            var promoteRequest = new Request("POST", "/_data_stream/_promote/" + dataStreamName);
            Response response = client().performRequest(promoteRequest);
            assertOK(response);
        } finally {
            if (backingIndexNames == null) {
                // we failed to compute the actual backing index names in the test because we failed earlier on, guessing them on a
                // best-effort basis
                backingIndexNames = List.of(backingIndexName(dataStreamName, 1), backingIndexName(dataStreamName, 2));
            }

            // These cleanup methods are copied from the finally block of other Data Stream tests in this class however
            // they may no longer be required but have been included for completeness
            cleanUpFollower(backingIndexNames, List.of(dataStreamName), List.of(autoFollowPatternName));
            cleanUpLeader(backingIndexNames.subList(0, 1), List.of(dataStreamName), List.of());
            Request deleteTemplateRequest = new Request("DELETE", "/_index_template/" + getTestName().toLowerCase(Locale.ROOT));
            if (createFollowerTemplate) {
                assertOK(client().performRequest(deleteTemplateRequest));
            }
            try (var leaderClient = buildLeaderClient()) {
                assertOK(leaderClient.performRequest(deleteTemplateRequest));
            }
        }

    }

    private int getNumberOfSuccessfulFollowedIndices() throws IOException {
        return getNumberOfSuccessfulFollowedIndices(client());
    }

    private int getNumberOfSuccessfulFollowedIndices(RestClient client) throws IOException {
        Request statsRequest = new Request("GET", "/_ccr/stats");
        Map<?, ?> response = toMap(client.performRequest(statsRequest));
        response = (Map<?, ?>) response.get("auto_follow_stats");
        return (Integer) response.get("number_of_successful_follow_indices");
    }

    private void deleteDataStream(RestClient client, String name) throws IOException {
        Request deleteTemplateRequest = new Request("DELETE", "/_data_stream/" + name);
        assertOK(client.performRequest(deleteTemplateRequest));
    }

    private Response getAutoFollowStats() throws IOException {
        final Request statsRequest = new Request("GET", "/_ccr/stats");
        statsRequest.addParameter("pretty", Boolean.TRUE.toString());
        return client().performRequest(statsRequest);
    }

    private void assertLongBusy(CheckedRunnable<Exception> runnable) throws Exception {
        try {
            assertBusy(runnable, 120L, TimeUnit.SECONDS);
        } catch (AssertionError ae) {
            try {
                final String autoFollowStats = EntityUtils.toString(getAutoFollowStats().getEntity());
                logger.warn(() -> format("AssertionError when waiting for auto-follower, auto-follow stats are: %s", autoFollowStats), ae);
            } catch (Exception e) {
                ae.addSuppressed(e);
            }
            throw ae;
        }
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    private void cleanUpFollower(final List<String> indices, final List<String> dataStreams, final List<String> autoFollowPatterns) {
        cleanUp(adminClient(), indices, dataStreams, autoFollowPatterns);
    }

    private void cleanUpMiddle(final List<String> indices, final List<String> dataStreams, final List<String> autoFollowPatterns)
        throws IOException {
        try (RestClient middleClient = buildMiddleClient()) {
            cleanUp(middleClient, indices, dataStreams, autoFollowPatterns);
        }
    }

    private void cleanUpLeader(final List<String> indices, final List<String> dataStreams, final List<String> autoFollowPatterns)
        throws IOException {
        try (RestClient leaderClient = buildLeaderClient()) {
            cleanUp(leaderClient, indices, dataStreams, autoFollowPatterns);
        }
    }

    private void cleanUp(
        final RestClient client,
        final List<String> indices,
        final List<String> dataStreams,
        final List<String> autoFollowPatterns
    ) {
        for (String autoFollowPattern : autoFollowPatterns) {
            try {
                deleteAutoFollowPattern(client, autoFollowPattern);
            } catch (IOException e) {
                if (isNotFoundResponseException(e)) {
                    continue;
                }
                logger.warn(() -> "failed to delete auto-follow pattern [" + autoFollowPattern + "] after test", e);
            }
        }
        for (String dataStream : dataStreams) {
            try {
                deleteDataStream(client, dataStream);
            } catch (IOException e) {
                if (isNotFoundResponseException(e)) {
                    continue;
                }
                logger.warn(() -> "failed to delete data stream [" + dataStream + "] after test", e);
            }
        }
        for (String index : indices) {
            try {
                deleteIndex(client, index);
            } catch (IOException e) {
                if (isNotFoundResponseException(e)) {
                    continue;
                }
                logger.warn(() -> "failed to delete index [" + index + "] after test", e);
            }
        }
    }
}
