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
import org.elasticsearch.common.xcontent.ObjectPath;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

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
        createAutoFollowPattern(client(), "test_pattern", "logs-*", "leader_cluster");

        // Create data stream and ensure that is is auto followed
        {
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
        }

        // First rollover and ensure second backing index is replicated:
        {
            try (RestClient leaderClient = buildLeaderClient()) {
                Request rolloverRequest = new Request("POST", "/" +  dataStreamName + "/_rollover");
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
        }

        // Second rollover and ensure third backing index is replicated:
        {
            try (RestClient leaderClient = buildLeaderClient()) {
                Request rolloverRequest = new Request("POST", "/" +  dataStreamName + "/_rollover");
                assertOK(leaderClient.performRequest(rolloverRequest));
                verifyDataStream(leaderClient, dataStreamName, backingIndexName(dataStreamName, 1), backingIndexName(dataStreamName, 2),
                    backingIndexName(dataStreamName, 3));

                Request indexRequest = new Request("POST", "/" + dataStreamName + "/_doc");
                indexRequest.addParameter("refresh", "true");
                indexRequest.setJsonEntity("{\"@timestamp\": \"" + DATE_FORMAT.format(new Date()) + "\",\"message\":\"abc\"}");
                assertOK(leaderClient.performRequest(indexRequest));
                verifyDocuments(leaderClient, dataStreamName, numDocs + 2);
            }
            assertBusy(() -> {
                assertThat(getNumberOfSuccessfulFollowedIndices(), equalTo(initialNumberOfSuccessfulFollowedIndices + 3));
                verifyDataStream(client(), dataStreamName, backingIndexName(dataStreamName, 1), backingIndexName(dataStreamName, 2),
                    backingIndexName(dataStreamName, 3));
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
                verifyDataStream(leaderClient, dataStreamName, backingIndexName(dataStreamName, 1));
                verifyDocuments(leaderClient, dataStreamName, initialNumDocs);
            }
        }
        // Create auto follow pattern
        createAutoFollowPattern(client(), "test_pattern", "logs-*", "leader_cluster");
        // Rollover and ensure only second backing index is replicated:
        {
            try (RestClient leaderClient = buildLeaderClient()) {
                Request rolloverRequest = new Request("POST", "/" +  dataStreamName + "/_rollover");
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
        }
        // Explicitly follow the first backing index and check that the data stream in follow cluster is updated correctly:
        {
            followIndex(backingIndexName(dataStreamName, 1), backingIndexName(dataStreamName, 1));
            assertBusy(() -> {
                assertThat(getNumberOfSuccessfulFollowedIndices(), equalTo(initialNumberOfSuccessfulFollowedIndices + 1));
                verifyDataStream(client(), dataStreamName, backingIndexName(dataStreamName, 1), backingIndexName(dataStreamName, 2));
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

    public void testRolloverDataStreamInFollowClusterForbidden() throws Exception {
        if ("follow".equals(targetCluster) == false) {
            return;
        }

        final int numDocs = 64;
        final var dataStreamName = "logs-tomcat-prod";

        int initialNumberOfSuccessfulFollowedIndices = getNumberOfSuccessfulFollowedIndices();

        // Create auto follow pattern
        createAutoFollowPattern(client(), "test_pattern", "logs-*", "leader_cluster");

        // Create data stream and ensure that is is auto followed
        {
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
        }

        // Rollover in leader cluster and ensure second backing index is replicated:
        {
            try (var leaderClient = buildLeaderClient()) {
                var rolloverRequest = new Request("POST", "/" +  dataStreamName + "/_rollover");
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
        }

        // Try rollover in follow cluster
        {
            var rolloverRequest1 = new Request("POST", "/" +  dataStreamName + "/_rollover");
            var e = expectThrows(ResponseException.class, () -> client().performRequest(rolloverRequest1));
            assertThat(e.getMessage(), containsString("data stream [" + dataStreamName + "] cannot be rolled over, " +
                "because it is a replicated data stream"));
            verifyDataStream(client(), dataStreamName, backingIndexName(dataStreamName, 1), backingIndexName(dataStreamName, 2));

            // Unfollow .ds-logs-tomcat-prod-000001
            pauseFollow(backingIndexName(dataStreamName, 1));
            closeIndex(backingIndexName(dataStreamName, 1));
            unfollow(backingIndexName(dataStreamName, 1));

            // Try again
            var rolloverRequest2 = new Request("POST", "/" +  dataStreamName + "/_rollover");
            e = expectThrows(ResponseException.class, () -> client().performRequest(rolloverRequest2));
            assertThat(e.getMessage(), containsString("data stream [" + dataStreamName + "] cannot be rolled over, " +
                "because it is a replicated data stream"));
            verifyDataStream(client(), dataStreamName, backingIndexName(dataStreamName, 1), backingIndexName(dataStreamName, 2));

            // Promote local data stream
            var promoteRequest = new Request("POST", "/_data_stream/_promote/" + dataStreamName);
            assertOK(client().performRequest(promoteRequest));

            // Try again and now the rollover should be successful because local data stream is now :
            var rolloverRequest3 = new Request("POST", "/" +  dataStreamName + "/_rollover");
            assertOK(client().performRequest(rolloverRequest3));
            verifyDataStream(client(), dataStreamName, backingIndexName(dataStreamName, 1), backingIndexName(dataStreamName, 2),
                backingIndexName(dataStreamName, 3));

            // TODO: verify that following a backing index for logs-tomcat-prod data stream in remote cluster fails,
            // because local data stream isn't a replicated data stream anymore.

            // Unfollow .ds-logs-tomcat-prod-000002,
            // which is now possible because this index can now be closed as it is no longer the write index.
            pauseFollow(backingIndexName(dataStreamName, 2));
            closeIndex(backingIndexName(dataStreamName, 2));
            unfollow(backingIndexName(dataStreamName, 2));
        }
        // Cleanup:
        {
            deleteAutoFollowPattern("test_pattern");
            deleteDataStream(dataStreamName);
        }
    }

    public void testRolloverAliasInFollowClusterForbidden() throws Exception {
        if ("follow".equals(targetCluster) == false) {
            return;
        }

        final int numDocs = 64;
        final var aliasName = "log-tomcat-prod";

        int initialNumberOfSuccessfulFollowedIndices = getNumberOfSuccessfulFollowedIndices();

        // Create auto follow pattern
        createAutoFollowPattern(client(), "test_pattern", "log-*", "leader_cluster");

        // Create leader index and write alias:
        {
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
        }

        // Rollover in leader cluster and ensure second backing index is replicated:
        {
            try (var leaderClient = buildLeaderClient()) {
                var rolloverRequest = new Request("POST", "/" +  aliasName + "/_rollover");
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
        }

        // Try rollover in follow cluster, this should fail, because is_write_index property of an alias isn't
        // replicated to follow cluster.
        {
            var rolloverRequest1 = new Request("POST", "/" +  aliasName + "/_rollover");
            var e = expectThrows(ResponseException.class, () -> client().performRequest(rolloverRequest1));
            assertThat(e.getMessage(), containsString("rollover target [" + aliasName + "] does not point to a write index"));
            verifyAlias(client(), aliasName, false, aliasName + "-000002", aliasName + "-000001");
        }
        // Cleanup:
        {
            deleteAutoFollowPattern("test_pattern");
        }
    }

    private static void verifyAlias(RestClient client,
                                    String aliasName,
                                    boolean checkWriteIndex,
                                    String... otherIndices) throws IOException {
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

        // Create auto follow pattern in follow cluster
        createAutoFollowPattern(client(), "id1", "logs-*-eu", "leader_cluster");
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
            createAutoFollowPattern(leaderClient, "id2", "logs-*-na", "follower_cluster");
        }

        var numDocs = 128;
        var leaderDataStreamName = "logs-http-eu";
        // Create data stream in leader cluster and ensure it is followed in follow cluster
        {
            try (var leaderClient = buildLeaderClient()) {
                for (int i = 0; i < numDocs; i++) {
                    Request indexRequest = new Request("POST", "/" + leaderDataStreamName + "/_doc");
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
        }
        var followerDataStreamName = "logs-http-na";
        {
            for (int i = 0; i < numDocs; i++) {
                var indexRequest = new Request("POST", "/" + followerDataStreamName + "/_doc");
                indexRequest.addParameter("refresh", "true");
                indexRequest.setJsonEntity("{\"@timestamp\": \"" + DATE_FORMAT.format(new Date()) + "\",\"message\":\"abc\"}");
                assertOK(client().performRequest(indexRequest));
            }
            verifyDocuments(client(), followerDataStreamName, numDocs);
            try (var leaderClient = buildLeaderClient()) {
                assertBusy(() -> {
                    assertThat(getNumberOfSuccessfulFollowedIndices(leaderClient),
                        equalTo(initialNumberOfSuccessfulFollowedIndicesInLeaderCluster + 1));
                    verifyDataStream(leaderClient, followerDataStreamName, backingIndexName(followerDataStreamName, 1));
                    ensureYellow(followerDataStreamName);
                    verifyDocuments(leaderClient, followerDataStreamName, numDocs);
                });
            }
        }

        // TODO: Replace these verifyDocuments(...) assertions with searches via 'logs-http' alias and
        // writes via 'logs-http' alias (ensuring write goes to write data stream).
        // Currently aliases can't refer to data streams, so we can't fully test the bi-direction replication scenario.
        // See: https://github.com/elastic/elasticsearch/pull/64710#discussion_r537210322

        // See all eu and na logs in leader and follower cluster:
        verifyDocuments(client(), "logs-http*", numDocs * 2);
        try (var leaderClient = buildLeaderClient()) {
            verifyDocuments(leaderClient, "logs-http*", numDocs * 2);
        }

        int moreDocs = 48;
        // Index more docs into leader cluster
        {
            try (var leaderClient = buildLeaderClient()) {
                for (int i = 0; i < moreDocs; i++) {
                    var indexRequest = new Request("POST", "/" + leaderDataStreamName + "/_doc");
                    indexRequest.addParameter("refresh", "true");
                    indexRequest.setJsonEntity("{\"@timestamp\": \"" + DATE_FORMAT.format(new Date()) + "\",\"message\":\"abc\"}");
                    assertOK(leaderClient.performRequest(indexRequest));
                }
                verifyDocuments(leaderClient, leaderDataStreamName, numDocs + moreDocs);
            }
            assertBusy(() -> {
                verifyDocuments(client(), leaderDataStreamName, numDocs + moreDocs);
            });
        }
        // Index more docs into follower cluster
        {
            for (int i = 0; i < moreDocs; i++) {
                var indexRequest = new Request("POST", "/" + followerDataStreamName + "/_doc");
                indexRequest.addParameter("refresh", "true");
                indexRequest.setJsonEntity("{\"@timestamp\": \"" + DATE_FORMAT.format(new Date()) + "\",\"message\":\"abc\"}");
                assertOK(client().performRequest(indexRequest));
            }
            verifyDocuments(client(), followerDataStreamName, numDocs + moreDocs);
            try (var leaderClient = buildLeaderClient()) {
                assertBusy(() -> {
                    verifyDocuments(leaderClient, followerDataStreamName, numDocs + moreDocs);
                });
            }
        }

        // TODO: Replace these verifyDocuments(...) assertions with searches via 'logs-http' alias and writes via 'logs-http'
        // (see previous TODO)

        // See all eu and na logs in leader and follower cluster:
        verifyDocuments(client(), "logs-http*", (numDocs + moreDocs) * 2);
        try (RestClient leaderClient = buildLeaderClient()) {
            verifyDocuments(leaderClient, "logs-http*", (numDocs + moreDocs) * 2);
        }

        // Cleanup:
        {
            deleteAutoFollowPattern(client(), "id1");
            deleteDataStream(client(), followerDataStreamName);
            try (RestClient leaderClient = buildLeaderClient()) {
                deleteDataStream(leaderClient, leaderDataStreamName);
                deleteAutoFollowPattern(leaderClient, "id2");
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

    private void deleteDataStream(String name) throws IOException {
        try (RestClient leaderClient = buildLeaderClient()) {
            Request deleteTemplateRequest = new Request("DELETE", "/_data_stream/" + name);
            assertOK(leaderClient.performRequest(deleteTemplateRequest));
        }
    }

    private void deleteDataStream(RestClient client, String name) throws IOException {
        Request deleteTemplateRequest = new Request("DELETE", "/_data_stream/" + name);
        assertOK(client.performRequest(deleteTemplateRequest));
    }

}
