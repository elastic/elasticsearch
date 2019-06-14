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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.rest.action.search.RestSearchAction.TOTAL_HITS_AS_INT_PARAM;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class ESCCRRestTestCase extends ESRestTestCase {

    protected final String targetCluster = System.getProperty("tests.target_cluster");

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    protected static void index(String index, String id, Object... fields) throws IOException {
        index(adminClient(), index, id, fields);
    }

    protected static void index(RestClient client, String index, String id, Object... fields) throws IOException {
        XContentBuilder document = jsonBuilder().startObject();
        for (int i = 0; i < fields.length; i += 2) {
            document.field((String) fields[i], fields[i + 1]);
        }
        document.endObject();
        final Request request = new Request("POST", "/" + index + "/_doc/" + id);
        request.setJsonEntity(Strings.toString(document));
        assertOK(client.performRequest(request));
    }

    protected static void refresh(String index) throws IOException {
        assertOK(adminClient().performRequest(new Request("POST", "/" + index + "/_refresh")));
    }

    protected static void resumeFollow(String followIndex) throws IOException {
        final Request request = new Request("POST", "/" + followIndex + "/_ccr/resume_follow");
        request.setJsonEntity("{\"read_poll_timeout\": \"10ms\"}");
        assertOK(client().performRequest(request));
    }

    protected static void followIndex(String leaderIndex, String followIndex) throws IOException {
        followIndex("leader_cluster", leaderIndex, followIndex);
    }

    protected static void followIndex(String leaderCluster, String leaderIndex, String followIndex) throws IOException {
        followIndex(client(), leaderCluster, leaderIndex, followIndex);
    }

    protected static void followIndex(RestClient client, String leaderCluster, String leaderIndex, String followIndex) throws IOException {
        final Request request = new Request("PUT", "/" + followIndex + "/_ccr/follow?wait_for_active_shards=1");
        request.setJsonEntity("{\"remote_cluster\": \"" + leaderCluster + "\", \"leader_index\": \"" + leaderIndex +
            "\", \"read_poll_timeout\": \"10ms\"}");
        assertOK(client.performRequest(request));
    }

    protected static void pauseFollow(String followIndex) throws IOException {
        pauseFollow(client(), followIndex);
    }

    protected static void pauseFollow(RestClient client, String followIndex) throws IOException {
        assertOK(client.performRequest(new Request("POST", "/" + followIndex + "/_ccr/pause_follow")));
    }

    protected static void putAutoFollowPattern(String patternName, String remoteCluster, String indexPattern) throws IOException {
        Request putPatternRequest = new Request("PUT", "/_ccr/auto_follow/" + patternName);
        putPatternRequest.setJsonEntity("{\"leader_index_patterns\": [\"" + indexPattern + "\"], \"remote_cluster\": \"" +
            remoteCluster + "\"}");
        assertOK(client().performRequest(putPatternRequest));
    }

    protected static void deleteAutoFollowPattern(String patternName) throws IOException {
        Request putPatternRequest = new Request("DELETE", "/_ccr/auto_follow/" + patternName);
        assertOK(client().performRequest(putPatternRequest));
    }

    protected static void unfollow(String followIndex) throws IOException {
        assertOK(client().performRequest(new Request("POST", "/" + followIndex + "/_ccr/unfollow")));
    }

    protected static void verifyDocuments(final String index, final int expectedNumDocs, final String query) throws IOException {
        verifyDocuments(index, expectedNumDocs, query, adminClient());
    }

    protected static void verifyDocuments(final String index,
                                          final int expectedNumDocs,
                                          final String query,
                                          final RestClient client) throws IOException {
        final Request request = new Request("GET", "/" + index + "/_search");
        request.addParameter("size", Integer.toString(expectedNumDocs));
        request.addParameter("sort", "field:asc");
        request.addParameter("q", query);
        request.addParameter(TOTAL_HITS_AS_INT_PARAM, "true");
        Map<String, ?> response = toMap(client.performRequest(request));

        int numDocs = (int) XContentMapValues.extractValue("hits.total", response);
        assertThat(index, numDocs, equalTo(expectedNumDocs));

        List<?> hits = (List<?>) XContentMapValues.extractValue("hits.hits", response);
        assertThat(hits.size(), equalTo(expectedNumDocs));
        for (int i = 0; i < expectedNumDocs; i++) {
            int value = (int) XContentMapValues.extractValue("_source.field", (Map<?, ?>) hits.get(i));
            assertThat(index, i, equalTo(value));
        }
    }

    protected static void verifyCcrMonitoring(final String expectedLeaderIndex, final String expectedFollowerIndex) throws IOException {
        Request request = new Request("GET", "/.monitoring-*/_search");
        request.setJsonEntity("{\"query\": {\"term\": {\"ccr_stats.leader_index\": \"" + expectedLeaderIndex + "\"}}}");
        Map<String, ?> response;
        try {
            response = toMap(adminClient().performRequest(request));
        } catch (ResponseException e) {
            throw new AssertionError("error while searching", e);
        }

        int followerMaxSeqNo = 0;
        int followerMappingVersion = 0;
        int followerSettingsVersion = 0;
        int followerAliasesVersion = 0;

        List<?> hits = (List<?>) XContentMapValues.extractValue("hits.hits", response);
        assertThat(hits.size(), greaterThanOrEqualTo(1));

        for (int i = 0; i < hits.size(); i++) {
            Map<?, ?> hit = (Map<?, ?>) hits.get(i);
            String leaderIndex = (String) XContentMapValues.extractValue("_source.ccr_stats.leader_index", hit);
            assertThat(leaderIndex, endsWith(expectedLeaderIndex));

            final String followerIndex = (String) XContentMapValues.extractValue("_source.ccr_stats.follower_index", hit);
            assertThat(followerIndex, equalTo(expectedFollowerIndex));

            int foundFollowerMaxSeqNo =
                (int) XContentMapValues.extractValue("_source.ccr_stats.follower_max_seq_no", hit);
            followerMaxSeqNo = Math.max(followerMaxSeqNo, foundFollowerMaxSeqNo);
            int foundFollowerMappingVersion =
                (int) XContentMapValues.extractValue("_source.ccr_stats.follower_mapping_version", hit);
            followerMappingVersion = Math.max(followerMappingVersion, foundFollowerMappingVersion);
            int foundFollowerSettingsVersion =
                    (int) XContentMapValues.extractValue("_source.ccr_stats.follower_settings_version", hit);
            followerSettingsVersion = Math.max(followerSettingsVersion, foundFollowerSettingsVersion);
            int foundFollowerAliasesVersion =
                    (int) XContentMapValues.extractValue("_source.ccr_stats.follower_aliases_version", hit);
            followerAliasesVersion = Math.max(followerAliasesVersion, foundFollowerAliasesVersion);
        }

        assertThat(followerMaxSeqNo, greaterThan(0));
        assertThat(followerMappingVersion, greaterThan(0));
        assertThat(followerSettingsVersion, greaterThan(0));
        assertThat(followerAliasesVersion, greaterThan(0));
    }

    protected static void verifyAutoFollowMonitoring() throws IOException {
        Request request = new Request("GET", "/.monitoring-*/_search");
        request.setJsonEntity("{\"query\": {\"term\": {\"type\": \"ccr_auto_follow_stats\"}}}");
        Map<String, ?> response;
        try {
            response = toMap(adminClient().performRequest(request));
        } catch (ResponseException e) {
            throw new AssertionError("error while searching", e);
        }

        int numberOfSuccessfulFollowIndices = 0;

        List<?> hits = (List<?>) XContentMapValues.extractValue("hits.hits", response);
        assertThat(hits.size(), greaterThanOrEqualTo(1));

        for (int i = 0; i < hits.size(); i++) {
            Map<?, ?> hit = (Map<?, ?>) hits.get(i);

            int foundNumberOfOperationsReceived =
                (int) XContentMapValues.extractValue("_source.ccr_auto_follow_stats.number_of_successful_follow_indices", hit);
            numberOfSuccessfulFollowIndices = Math.max(numberOfSuccessfulFollowIndices, foundNumberOfOperationsReceived);
        }

        assertThat(numberOfSuccessfulFollowIndices, greaterThanOrEqualTo(1));
    }

    protected static Map<String, Object> toMap(Response response) throws IOException {
        return toMap(EntityUtils.toString(response.getEntity()));
    }

    protected static Map<String, Object> toMap(String response) {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, response, false);
    }

    protected static void ensureYellow(final String index) throws IOException {
        ensureYellow(index, adminClient());
    }

    protected static void ensureYellow(final String index, final RestClient client) throws IOException {
        final Request request = new Request("GET", "/_cluster/health/" + index);
        request.addParameter("wait_for_status", "yellow");
        request.addParameter("wait_for_active_shards", "1");
        request.addParameter("wait_for_no_relocating_shards", "true");
        request.addParameter("wait_for_no_initializing_shards", "true");
        request.addParameter("timeout", "5s");
        request.addParameter("level", "shards");
        client.performRequest(request);
    }

    protected int countCcrNodeTasks() throws IOException {
        final Request request = new Request("GET", "/_tasks");
        request.addParameter("detailed", "true");
        Map<String, Object> rsp1 = toMap(adminClient().performRequest(request));
        Map<?, ?> nodes = (Map<?, ?>) rsp1.get("nodes");
        assertThat(nodes.size(), equalTo(1));
        Map<?, ?> node = (Map<?, ?>) nodes.values().iterator().next();
        Map<?, ?> nodeTasks = (Map<?, ?>) node.get("tasks");
        int numNodeTasks = 0;
        for (Map.Entry<?, ?> entry : nodeTasks.entrySet()) {
            Map<?, ?> nodeTask = (Map<?, ?>) entry.getValue();
            String action = (String) nodeTask.get("action");
            if (action.startsWith("xpack/ccr/shard_follow_task")) {
                numNodeTasks++;
            }
        }
        return numNodeTasks;
    }

    protected static void createIndex(String name, Settings settings) throws IOException {
        createIndex(name, settings, "");
    }

    protected static void createIndex(String name, Settings settings, String mapping) throws IOException {
        final Request request = new Request("PUT", "/" + name);
        request.setJsonEntity("{ \"settings\": " + Strings.toString(settings) + ", \"mappings\" : {" + mapping + "} }");
        assertOK(adminClient().performRequest(request));
    }

    protected static boolean indexExists(String index) throws IOException {
        Response response = adminClient().performRequest(new Request("HEAD", "/" + index));
        return RestStatus.OK.getStatus() == response.getStatusLine().getStatusCode();
    }

    protected RestClient buildLeaderClient() throws IOException {
        assert "leader".equals(targetCluster) == false;
        return buildClient(System.getProperty("tests.leader_host"));
    }

    protected RestClient buildLeaderClient(final Settings settings) throws IOException {
        assert "leader".equals(targetCluster) == false;
        return buildClient(System.getProperty("tests.leader_host"), settings);
    }

    protected RestClient buildMiddleClient() throws IOException {
        assert "middle".equals(targetCluster) == false;
        return buildClient(System.getProperty("tests.middle_host"));
    }

    private RestClient buildClient(final String url) throws IOException {
        return buildClient(url, restAdminSettings());
    }

    private RestClient buildClient(final String url, final Settings settings) throws IOException {
        int portSeparator = url.lastIndexOf(':');
        HttpHost httpHost = new HttpHost(url.substring(0, portSeparator),
                Integer.parseInt(url.substring(portSeparator + 1)), getProtocol());
        return buildClient(settings, new HttpHost[]{httpHost});
    }

}
