/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class FollowIndexIT extends ESRestTestCase {

    private final boolean runningAgainstLeaderCluster = Booleans.parseBoolean(System.getProperty("tests.is_leader_cluster"));

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    public void testFollowIndex() throws Exception {
        final int numDocs = 128;
        final String leaderIndexName = "test_index1";
        if (runningAgainstLeaderCluster) {
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
            verifyDocuments(leaderIndexName, numDocs);
        } else {
            logger.info("Running against follow cluster");
            final String followIndexName = "test_index2";
            createAndFollowIndex("leader_cluster:" + leaderIndexName, followIndexName);
            assertBusy(() -> verifyDocuments(followIndexName, numDocs));
            // unfollow and then follow and then index a few docs in leader index:
            unfollowIndex(followIndexName);
            followIndex("leader_cluster:" + leaderIndexName, followIndexName);
            try (RestClient leaderClient = buildLeaderClient()) {
                int id = numDocs;
                index(leaderClient, leaderIndexName, Integer.toString(id), "field", id, "filtered_field", "true");
                index(leaderClient, leaderIndexName, Integer.toString(id + 1), "field", id + 1, "filtered_field", "true");
                index(leaderClient, leaderIndexName, Integer.toString(id + 2), "field", id + 2, "filtered_field", "true");
            }
            assertBusy(() -> verifyDocuments(followIndexName, numDocs + 3));
            assertBusy(() -> verifyCcrMonitoring(leaderIndexName, followIndexName));
        }
    }

    public void testAutoFollowPatterns() throws Exception {
        assumeFalse("Test should only run when both clusters are running", runningAgainstLeaderCluster);

        Request request = new Request("PUT", "/_ccr/auto_follow/leader_cluster");
        request.setJsonEntity("{\"leader_index_patterns\": [\"logs-*\"]}");
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
            ensureYellow("logs-20190101");
            verifyDocuments("logs-20190101", 5);
        });
        assertBusy(() -> verifyCcrMonitoring("logs-20190101", "logs-20190101"));
    }

    private static void index(RestClient client, String index, String id, Object... fields) throws IOException {
        XContentBuilder document = jsonBuilder().startObject();
        for (int i = 0; i < fields.length; i += 2) {
            document.field((String) fields[i], fields[i + 1]);
        }
        document.endObject();
        final Request request = new Request("POST", "/" + index + "/_doc/" + id);
        request.setJsonEntity(Strings.toString(document));
        assertOK(client.performRequest(request));
    }

    private static void refresh(String index) throws IOException {
        assertOK(client().performRequest(new Request("POST", "/" + index + "/_refresh")));
    }

    private static void followIndex(String leaderIndex, String followIndex) throws IOException {
        final Request request = new Request("POST", "/" + followIndex + "/_ccr/follow");
        request.setJsonEntity("{\"leader_index\": \"" + leaderIndex + "\", \"idle_shard_retry_delay\": \"10ms\"}");
        assertOK(client().performRequest(request));
    }

    private static void createAndFollowIndex(String leaderIndex, String followIndex) throws IOException {
        final Request request = new Request("POST", "/" + followIndex + "/_ccr/create_and_follow");
        request.setJsonEntity("{\"leader_index\": \"" + leaderIndex + "\", \"idle_shard_retry_delay\": \"10ms\"}");
        assertOK(client().performRequest(request));
    }

    private static void unfollowIndex(String followIndex) throws IOException {
        assertOK(client().performRequest(new Request("POST", "/" + followIndex + "/_ccr/unfollow")));
    }

    private static void verifyDocuments(String index, int expectedNumDocs) throws IOException {
        final Request request = new Request("GET", "/" + index + "/_search");
        request.addParameter("size", Integer.toString(expectedNumDocs));
        request.addParameter("sort", "field:asc");
        request.addParameter("q", "filtered_field:true");
        Map<String, ?> response = toMap(client().performRequest(request));

        int numDocs = (int) XContentMapValues.extractValue("hits.total", response);
        assertThat(numDocs, equalTo(expectedNumDocs));

        List<?> hits = (List<?>) XContentMapValues.extractValue("hits.hits", response);
        assertThat(hits.size(), equalTo(expectedNumDocs));
        for (int i = 0; i < expectedNumDocs; i++) {
            int value = (int) XContentMapValues.extractValue("_source.field", (Map<?, ?>) hits.get(i));
            assertThat(i, equalTo(value));
        }
    }

    private static void verifyCcrMonitoring(final String expectedLeaderIndex, final String expectedFollowerIndex) throws IOException {
        Request request = new Request("GET", "/.monitoring-*/_search");
        request.setJsonEntity("{\"query\": {\"term\": {\"ccr_stats.leader_index\": \"leader_cluster:" + expectedLeaderIndex + "\"}}}");
        Map<String, ?> response;
        try {
            response = toMap(client().performRequest(request));
        } catch (ResponseException e) {
            throw new AssertionError("error while searching", e);
        }

        int numberOfOperationsReceived = 0;
        int numberOfOperationsIndexed = 0;

        List<?> hits = (List<?>) XContentMapValues.extractValue("hits.hits", response);
        assertThat(hits.size(), greaterThanOrEqualTo(1));

        for (int i = 0; i < hits.size(); i++) {
            Map<?, ?> hit = (Map<?, ?>) hits.get(i);
            String leaderIndex = (String) XContentMapValues.extractValue("_source.ccr_stats.leader_index", hit);
            assertThat(leaderIndex, endsWith(expectedLeaderIndex));

            final String followerIndex = (String) XContentMapValues.extractValue("_source.ccr_stats.follower_index", hit);
            assertThat(followerIndex, equalTo(expectedFollowerIndex));

            int foundNumberOfOperationsReceived =
                (int) XContentMapValues.extractValue("_source.ccr_stats.operations_received", hit);
            numberOfOperationsReceived = Math.max(numberOfOperationsReceived, foundNumberOfOperationsReceived);
            int foundNumberOfOperationsIndexed =
                (int) XContentMapValues.extractValue("_source.ccr_stats.number_of_operations_indexed", hit);
            numberOfOperationsIndexed = Math.max(numberOfOperationsIndexed, foundNumberOfOperationsIndexed);
        }

        assertThat(numberOfOperationsReceived, greaterThanOrEqualTo(1));
        assertThat(numberOfOperationsIndexed, greaterThanOrEqualTo(1));
    }

    private static Map<String, Object> toMap(Response response) throws IOException {
        return toMap(EntityUtils.toString(response.getEntity()));
    }

    private static Map<String, Object> toMap(String response) {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, response, false);
    }

    private static void ensureYellow(String index) throws IOException {
        Request request = new Request("GET", "/_cluster/health/" + index);
        request.addParameter("wait_for_status", "yellow");
        request.addParameter("wait_for_no_relocating_shards", "true");
        request.addParameter("wait_for_no_initializing_shards", "true");
        request.addParameter("timeout", "70s");
        request.addParameter("level", "shards");
        client().performRequest(request);
    }

    private RestClient buildLeaderClient() throws IOException {
        assert runningAgainstLeaderCluster == false;
        String leaderUrl = System.getProperty("tests.leader_host");
        int portSeparator = leaderUrl.lastIndexOf(':');
        HttpHost httpHost = new HttpHost(leaderUrl.substring(0, portSeparator),
                Integer.parseInt(leaderUrl.substring(portSeparator + 1)), getProtocol());
        return buildClient(Settings.EMPTY, new HttpHost[]{httpHost});
    }

}
