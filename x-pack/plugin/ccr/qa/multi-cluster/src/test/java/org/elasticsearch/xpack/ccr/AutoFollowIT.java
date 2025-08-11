/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xcontent.ObjectPath.eval;
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
            putPatternRequest.setJsonEntity("{\"leader_index_patterns\": [\"index-*\"], \"remote_cluster\": \"leader_cluster\"}");
            assertOK(client().performRequest(putPatternRequest));
            putPatternRequest = new Request("PUT", "/_ccr/auto_follow/middle_cluster_pattern");
            putPatternRequest.setJsonEntity("{\"leader_index_patterns\": [\"index-*\"], \"remote_cluster\": \"middle_cluster\"}");
            assertOK(client().performRequest(putPatternRequest));
            try (RestClient leaderClient = buildLeaderClient()) {
                Request request = new Request("PUT", "/index-20190101");
                request.setJsonEntity("{\"mappings\": {\"properties\": {\"field\": {\"type\": \"keyword\"}}}}");
                assertOK(leaderClient.performRequest(request));
                for (int i = 0; i < 5; i++) {
                    String id = Integer.toString(i);
                    index(leaderClient, "index-20190101", id, "field", i, "filtered_field", "true");
                }
            }
            try (RestClient middleClient = buildMiddleClient()) {
                Request request = new Request("PUT", "/index-20200101");
                request.setJsonEntity("{\"mappings\": {\"properties\": {\"field\": {\"type\": \"keyword\"}}}}");
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
                asList("index-20190101", "index-20200101"),
                emptyList(),
                asList("leader_cluster_pattern", "middle_cluster_pattern")
            );
            cleanUpMiddle(asList("index-20200101"), emptyList(), emptyList());
            cleanUpLeader(asList("index-20190101"), emptyList(), emptyList());
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
                request.setJsonEntity("{\"mappings\": {\"properties\": {\"field\": {\"type\": \"keyword\"}}}}");
                assertOK(leaderClient.performRequest(request));
            }

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
                assertThat(indexExists(excludedIndex), is(false));
            });

            assertLongBusy(() -> verifyCcrMonitoring("metrics-20210101", "metrics-20210101"));
            assertLongBusy(ESCCRRestTestCase::verifyAutoFollowMonitoring);

        } finally {
            cleanUpFollower(singletonList("metrics-20210101"), emptyList(), singletonList(autoFollowPatternName));
            cleanUpLeader(org.elasticsearch.core.List.of("metrics-20210101", excludedIndex), emptyList(), emptyList());
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
            createAutoFollowPattern(client(), autoFollowPatternName, "logs-mysql-*", "leader_cluster");

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
                asList(backingIndexName(dataStreamName, 1), backingIndexName(dataStreamName, 2), backingIndexName(dataStreamName, 3)),
                singletonList(dataStreamName),
                singletonList(autoFollowPatternName)
            );
            cleanUpLeader(
                asList(backingIndexName(dataStreamName, 1), backingIndexName(dataStreamName, 2), backingIndexName(dataStreamName, 3)),
                singletonList(dataStreamName),
                emptyList()
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
            createAutoFollowPattern(client(), autoFollowPatternName, dataStreamName + "*", "leader_cluster");

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
                asList(backingIndexName(dataStreamName, 1), backingIndexName(dataStreamName, 2)),
                singletonList(dataStreamName),
                singletonList(autoFollowPatternName)
            );
            cleanUpLeader(
                asList(backingIndexName(dataStreamName, 1), backingIndexName(dataStreamName, 2)),
                singletonList(dataStreamName),
                emptyList()
            );
        }
    }

    public void testRolloverDataStreamInFollowClusterForbidden() throws Exception {
        if ("follow".equals(targetCluster) == false) {
            return;
        }

        final int numDocs = 64;
        final String dataStreamName = "logs-tomcat-prod";
        final String autoFollowPatternName = getTestName().toLowerCase(Locale.ROOT);

        int initialNumberOfSuccessfulFollowedIndices = getNumberOfSuccessfulFollowedIndices();
        List<String> backingIndexNames = null;
        try {
            // Create auto follow pattern
            createAutoFollowPattern(client(), autoFollowPatternName, "logs-tomcat-*", "leader_cluster");

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

            // Rollover in leader cluster and ensure second backing index is replicated:
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

            // Try rollover in follow cluster
            {
                Request rolloverRequest1 = new Request("POST", "/" + dataStreamName + "/_rollover");
                Exception e = expectThrows(ResponseException.class, () -> client().performRequest(rolloverRequest1));
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
                Request rolloverRequest2 = new Request("POST", "/" + dataStreamName + "/_rollover");
                e = expectThrows(ResponseException.class, () -> client().performRequest(rolloverRequest2));
                assertThat(
                    e.getMessage(),
                    containsString(
                        "data stream [" + dataStreamName + "] cannot be rolled over, " + "because it is a replicated data stream"
                    )
                );
                verifyDataStream(client(), dataStreamName, backingIndexName(dataStreamName, 1), backingIndexName(dataStreamName, 2));

                // Promote local data stream
                Request promoteRequest = new Request("POST", "/_data_stream/_promote/" + dataStreamName);
                assertOK(client().performRequest(promoteRequest));

                // Try again and now the rollover should be successful because local data stream is now :
                Request rolloverRequest3 = new Request("POST", "/" + dataStreamName + "/_rollover");
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
                backingIndexNames = org.elasticsearch.core.List.of(
                    backingIndexName(dataStreamName, 1),
                    backingIndexName(dataStreamName, 2),
                    backingIndexName(dataStreamName, 3)
                );
            }
            cleanUpFollower(backingIndexNames, singletonList(dataStreamName), singletonList(autoFollowPatternName));
            cleanUpLeader(backingIndexNames.subList(0, 2), singletonList(dataStreamName), emptyList());
        }
    }

    public void testRolloverAliasInFollowClusterForbidden() throws Exception {
        if ("follow".equals(targetCluster) == false) {
            return;
        }

        final int numDocs = 64;
        final String aliasName = "log-tomcat-prod";

        int initialNumberOfSuccessfulFollowedIndices = getNumberOfSuccessfulFollowedIndices();
        try {
            // Create auto follow pattern
            createAutoFollowPattern(client(), "test_pattern", "log-*", "leader_cluster");

            // Create leader index and write alias:
            try (RestClient leaderClient = buildLeaderClient()) {
                Request createFirstIndexRequest = new Request("PUT", "/" + aliasName + "-000001");
                createFirstIndexRequest.setJsonEntity("{\"aliases\": {\"" + aliasName + "\":{\"is_write_index\":true}}}");
                leaderClient.performRequest(createFirstIndexRequest);

                for (int i = 0; i < numDocs; i++) {
                    Request indexRequest = new Request("POST", "/" + aliasName + "/_doc");
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
            try (RestClient leaderClient = buildLeaderClient()) {
                Request rolloverRequest = new Request("POST", "/" + aliasName + "/_rollover");
                assertOK(leaderClient.performRequest(rolloverRequest));
                verifyAlias(leaderClient, aliasName, true, aliasName + "-000002", aliasName + "-000001");

                Request indexRequest = new Request("POST", "/" + aliasName + "/_doc");
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
                Request rolloverRequest1 = new Request("POST", "/" + aliasName + "/_rollover");
                Exception e = expectThrows(ResponseException.class, () -> client().performRequest(rolloverRequest1));
                assertThat(e.getMessage(), containsString("rollover target [" + aliasName + "] does not point to a write index"));
                verifyAlias(client(), aliasName, false, aliasName + "-000002", aliasName + "-000001");
            }

        } finally {
            cleanUpFollower(asList(aliasName + "-000001", aliasName + "-000002"), emptyList(), singletonList("test_pattern"));
        }
    }

    private static void verifyAlias(RestClient client, String aliasName, boolean checkWriteIndex, String... otherIndices)
        throws IOException {
        try {
            Request getAliasRequest = new Request("GET", "/_alias/" + aliasName);
            Map<?, ?> responseBody = toMap(client.performRequest(getAliasRequest));
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

        final String aliasName = "logs-http";
        final String leaderDataStreamName = "logs-http-eu";
        final String followerDataStreamName = "logs-http-na";

        try {
            // Create auto follow pattern in follow cluster
            createAutoFollowPattern(client(), "id1", "logs-*-eu", "leader_cluster");

            // Create auto follow pattern in leader cluster:
            try (RestClient leaderClient = buildLeaderClient()) {
                initialNumberOfSuccessfulFollowedIndicesInLeaderCluster = getNumberOfSuccessfulFollowedIndices(leaderClient);
                // First add remote cluster to leader cluster:
                Request request = new Request("PUT", "/_cluster/settings");
                try (XContentBuilder bodyBuilder = JsonXContent.contentBuilder()) {
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
                                        Request nodesInfoRequest = new Request("GET", "/_nodes/_local");
                                        Map<?, ?> nodesInfoResponse = toMap(client().performRequest(nodesInfoRequest));
                                        Map<?, ?> node = (Map<?, ?>) ((Map<?, ?>) nodesInfoResponse.get("nodes")).values()
                                            .iterator()
                                            .next();
                                        Map<?, ?> transportMetrics = (Map<?, ?>) node.get("transport");
                                        String address = (String) transportMetrics.get("publish_address");
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
                createAutoFollowPattern(leaderClient, "id2", "logs-*-na", "follower_cluster");
            }

            int numDocs = 128;
            // Create data stream in leader cluster and ensure it is followed in follow cluster
            try (RestClient leaderClient = buildLeaderClient()) {
                // Setting up data stream and alias with write flag in leader cluster:
                Request createDataStreamRequest = new Request("PUT", "/_data_stream/" + leaderDataStreamName);
                assertOK(leaderClient.performRequest(createDataStreamRequest));
                Request updateAliasesRequest = new Request("POST", "/_aliases");
                updateAliasesRequest.setJsonEntity(
                    "{\"actions\":["
                        + "{\"add\":{\"index\":\""
                        + leaderDataStreamName
                        + "\",\"alias\":\"logs-http\",\"is_write_index\":true}}"
                        + "]}"
                );
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
            updateAliasesRequest.setJsonEntity(
                "{\"actions\":["
                    + "{\"add\":{\"index\":\""
                    + followerDataStreamName
                    + "\",\"alias\":\"logs-http\",\"is_write_index\":true}}"
                    + "]}"
            );
            assertOK(client().performRequest(updateAliasesRequest));

            for (int i = 0; i < numDocs; i++) {
                Request indexRequest = new Request("POST", "/" + aliasName + "/_doc");
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
            updateAliasesRequest.setJsonEntity(
                "{\"actions\":[" + "{\"add\":{\"index\":\"" + leaderDataStreamName + "\",\"alias\":\"logs-http\"}}" + "]}"
            );
            assertOK(client().performRequest(updateAliasesRequest));

            try (RestClient leaderClient = buildLeaderClient()) {
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
                updateAliasesRequest.setJsonEntity(
                    "{\"actions\":[" + "{\"add\":{\"index\":\"" + followerDataStreamName + "\",\"alias\":\"logs-http\"}}" + "]}"
                );
                assertOK(leaderClient.performRequest(updateAliasesRequest));
            }

            // See all eu and na logs in leader and follower cluster:
            verifyDocuments(client(), aliasName, numDocs * 2);
            try (RestClient leaderClient = buildLeaderClient()) {
                verifyDocuments(leaderClient, aliasName, numDocs * 2);
            }

            int moreDocs = 48;
            // Index more docs into leader cluster
            {
                try (RestClient leaderClient = buildLeaderClient()) {
                    for (int i = 0; i < moreDocs; i++) {
                        Request indexRequest = new Request("POST", "/" + aliasName + "/_doc");
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
                    Request indexRequest = new Request("POST", "/" + aliasName + "/_doc");
                    indexRequest.addParameter("refresh", "true");
                    indexRequest.setJsonEntity("{\"@timestamp\": \"" + DATE_FORMAT.format(new Date()) + "\",\"message\":\"abc\"}");
                    assertOK(client().performRequest(indexRequest));
                }
                verifyDocuments(client(), followerDataStreamName, numDocs + moreDocs);
                try (RestClient leaderClient = buildLeaderClient()) {
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
                asList(backingIndexName(followerDataStreamName, 1), backingIndexName(leaderDataStreamName, 1)),
                asList(followerDataStreamName, leaderDataStreamName),
                singletonList("id1")
            );
            cleanUpLeader(
                asList(backingIndexName(leaderDataStreamName, 1), backingIndexName(followerDataStreamName, 1)),
                asList(leaderDataStreamName, followerDataStreamName),
                singletonList("id2")
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
            try (RestClient leaderClient = buildLeaderClient()) {
                final String systemPropertyRepoPath = System.getProperty("tests.leader_cluster_repository_path");
                assertThat(
                    "Missing system property [tests.leader_cluster_repository_path]",
                    systemPropertyRepoPath,
                    not(emptyOrNullString())
                );
                final String repositoryPath = systemPropertyRepoPath + '/' + testPrefix;

                registerRepository(leaderClient, repository, "fs", true, Settings.builder().put("location", repositoryPath).build());

                for (int i = 0; i < 5; i++) {
                    Request indexRequest = new Request("POST", "/" + indexName + "/_doc");
                    indexRequest.addParameter("refresh", "true");
                    indexRequest.setJsonEntity("{\"value\":" + i + "}");
                    assertOK(leaderClient.performRequest(indexRequest));
                }
                verifyDocuments(leaderClient, indexName, 5);

                deleteSnapshot(leaderClient, repository, snapshot, true);
                createSnapshot(leaderClient, repository, snapshot, true);
            }
        } finally {
            cleanUpLeader(singletonList(indexName), emptyList(), emptyList());
        }

        final String autoFollowPattern = "pattern-" + testPrefix;
        final String regularIndex = testPrefix + "-regular";
        final String mountedIndex = testPrefix + "-mounted";

        try {
            createAutoFollowPattern(client(), autoFollowPattern, testPrefix + "-*", "leader_cluster");

            // Create a regular index on leader
            try (RestClient leaderClient = buildLeaderClient()) {
                for (int i = 0; i < 10; i++) {
                    Request indexRequest = new Request("POST", "/" + regularIndex + "/_doc");
                    indexRequest.addParameter("refresh", "true");
                    indexRequest.setJsonEntity("{\"value\":" + i + "}");
                    assertOK(leaderClient.performRequest(indexRequest));
                }
                verifyDocuments(leaderClient, regularIndex, 10);
            }

            // Mount the snapshot on leader
            try (RestClient leaderClient = buildLeaderClient()) {
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
            cleanUpLeader(asList(regularIndex, mountedIndex), emptyList(), emptyList());
            cleanUpFollower(singletonList(regularIndex), emptyList(), singletonList(autoFollowPattern));
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
                logger.warn(
                    () -> new ParameterizedMessage(
                        "AssertionError when waiting for auto-follower, auto-follow stats are: {}",
                        autoFollowStats
                    ),
                    ae
                );
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
                logger.warn(() -> new ParameterizedMessage("failed to delete auto-follow pattern [{}] after test", autoFollowPattern), e);
            }
        }
        for (String dataStream : dataStreams) {
            try {
                deleteDataStream(client, dataStream);
            } catch (IOException e) {
                if (isNotFoundResponseException(e)) {
                    continue;
                }
                logger.warn(() -> new ParameterizedMessage("failed to delete data stream [{}] after test", dataStream), e);
            }
        }
        for (String index : indices) {
            try {
                deleteIndex(client, index);
            } catch (IOException e) {
                if (isNotFoundResponseException(e)) {
                    continue;
                }
                logger.warn(() -> new ParameterizedMessage("failed to delete index [{}] after test", index), e);
            }
        }
    }
}
