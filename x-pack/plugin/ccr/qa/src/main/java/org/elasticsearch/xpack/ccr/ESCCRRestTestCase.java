/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.rest.action.search.RestSearchAction.TOTAL_HITS_AS_INT_PARAM;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

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
        final Request request = new Request("POST", "/" + index + "/_doc" + (id == null ? "" : "/" + id));
        request.setJsonEntity(Strings.toString(document));
        assertOK(client.performRequest(request));
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
        followIndex(client, leaderCluster, leaderIndex, followIndex, null);
    }

    protected static void followIndex(
        final RestClient client,
        final String leaderCluster,
        final String leaderIndex,
        final String followIndex,
        final Settings settings
    ) throws IOException {
        final Request request = new Request("PUT", "/" + followIndex + "/_ccr/follow?wait_for_active_shards=1");
        try (XContentBuilder bodyBuilder = JsonXContent.contentBuilder()) {
            bodyBuilder.startObject();
            {
                bodyBuilder.field("remote_cluster", leaderCluster);
                bodyBuilder.field("leader_index", leaderIndex);
                bodyBuilder.field("read_poll_timeout", "10ms");
                if (settings != null) {
                    bodyBuilder.startObject("settings");
                    {
                        settings.toXContent(bodyBuilder, ToXContent.EMPTY_PARAMS);
                    }
                    bodyBuilder.endObject();
                }
            }
            bodyBuilder.endObject();
            request.setJsonEntity(Strings.toString(bodyBuilder));
        }
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
        putPatternRequest.setJsonEntity(String.format(Locale.ROOT, """
            {"leader_index_patterns": ["%s"], "remote_cluster": "%s"}
            """, indexPattern, remoteCluster));
        assertOK(client().performRequest(putPatternRequest));
    }

    protected static void deleteAutoFollowPattern(String patternName) throws IOException {
        deleteAutoFollowPattern(client(), patternName);
    }

    protected static void deleteAutoFollowPattern(RestClient client, String patternName) throws IOException {
        Request putPatternRequest = new Request("DELETE", "/_ccr/auto_follow/" + patternName);
        assertOK(client.performRequest(putPatternRequest));
    }

    protected static void unfollow(String followIndex) throws IOException {
        assertOK(client().performRequest(new Request("POST", "/" + followIndex + "/_ccr/unfollow")));
    }

    protected static void verifyDocuments(final String index, final int expectedNumDocs, final String query) throws IOException {
        verifyDocuments(index, expectedNumDocs, query, adminClient());
    }

    protected static void verifyDocuments(final String index, final int expectedNumDocs, final String query, final RestClient client)
        throws IOException {
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

    protected static void verifyDocuments(final RestClient client, final String index, final int expectedNumDocs) throws IOException {
        final Request request = new Request("GET", "/" + index + "/_search");
        request.addParameter(TOTAL_HITS_AS_INT_PARAM, "true");
        Map<String, ?> response = toMap(client.performRequest(request));

        int numDocs = (int) XContentMapValues.extractValue("hits.total", response);
        assertThat(index, numDocs, equalTo(expectedNumDocs));
    }

    protected static void verifyCcrMonitoring(final String expectedLeaderIndex, final String expectedFollowerIndex) throws IOException {
        Request request = new Request("GET", "/.monitoring-*/_search");
        request.setJsonEntity(String.format(Locale.ROOT, """
            {"query": {"term": {"ccr_stats.leader_index": "%s"}}}
            """, expectedLeaderIndex));
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

            int foundFollowerMaxSeqNo = (int) XContentMapValues.extractValue("_source.ccr_stats.follower_max_seq_no", hit);
            followerMaxSeqNo = Math.max(followerMaxSeqNo, foundFollowerMaxSeqNo);
            int foundFollowerMappingVersion = (int) XContentMapValues.extractValue("_source.ccr_stats.follower_mapping_version", hit);
            followerMappingVersion = Math.max(followerMappingVersion, foundFollowerMappingVersion);
            int foundFollowerSettingsVersion = (int) XContentMapValues.extractValue("_source.ccr_stats.follower_settings_version", hit);
            followerSettingsVersion = Math.max(followerSettingsVersion, foundFollowerSettingsVersion);
            int foundFollowerAliasesVersion = (int) XContentMapValues.extractValue("_source.ccr_stats.follower_aliases_version", hit);
            followerAliasesVersion = Math.max(followerAliasesVersion, foundFollowerAliasesVersion);
        }

        assertThat(followerMaxSeqNo, greaterThan(0));
        assertThat(followerMappingVersion, greaterThan(0));
        assertThat(followerSettingsVersion, greaterThan(0));
        assertThat(followerAliasesVersion, greaterThan(0));
    }

    protected static void verifyAutoFollowMonitoring() throws IOException {
        Request request = new Request("GET", "/.monitoring-*/_count");
        request.setJsonEntity("""
                {
                  "query": {
                    "bool" : {
                      "filter": {
                        "term" : { "type" : "ccr_auto_follow_stats" }
                      },
                      "must" : {
                        "range" : {
                          "ccr_auto_follow_stats.number_of_successful_follow_indices" : { "gt" : 0 }
                        }
                      }
                    }
                  }
                }
            """);
        String responseEntity;
        Map<String, ?> response;
        try {
            responseEntity = EntityUtils.toString(adminClient().performRequest(request).getEntity());
            response = toMap(responseEntity);
        } catch (ResponseException e) {
            throw new AssertionError("error while searching", e);
        }
        assertNotNull(responseEntity);

        final Number count = (Number) XContentMapValues.extractValue("count", response);
        assertThat(
            "Expected at least 1 successfully followed index but found none, count returned [" + responseEntity + ']',
            count.longValue(),
            greaterThanOrEqualTo(1L)
        );
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
        ensureHealth(client, index, request -> {
            request.addParameter("wait_for_status", "yellow");
            request.addParameter("wait_for_active_shards", "1");
            request.addParameter("wait_for_no_relocating_shards", "true");
            // follower index can be yellow even when its primary shards are still initializing as we bootstrap them using snapshot/restore.
            request.addParameter("wait_for_no_initializing_shards", "true");
            request.addParameter("timeout", "5s");
            request.addParameter("level", "shards");
        });
    }

    protected Set<CcrNodeTask> getCcrNodeTasks() throws IOException {
        final Request request = new Request("GET", "/_tasks");
        request.addParameter("detailed", "true");
        Map<String, Object> rsp1 = toMap(adminClient().performRequest(request));
        Map<?, ?> nodes = (Map<?, ?>) rsp1.get("nodes");
        assertThat(nodes.size(), equalTo(1));
        Map<?, ?> node = (Map<?, ?>) nodes.values().iterator().next();
        Map<?, ?> nodeTasks = (Map<?, ?>) node.get("tasks");
        var ccrNodeTasks = new HashSet<CcrNodeTask>();
        for (Map.Entry<?, ?> entry : nodeTasks.entrySet()) {
            Map<?, ?> nodeTask = (Map<?, ?>) entry.getValue();
            String action = (String) nodeTask.get("action");
            if (action.startsWith("xpack/ccr/shard_follow_task")) {
                var status = (Map<?, ?>) nodeTask.get("status");
                ccrNodeTasks.add(
                    new CcrNodeTask(
                        (String) status.get("remote_cluster"),
                        (String) status.get("leader_index"),
                        (String) status.get("follower_index"),
                        (Integer) status.get("shard_id")
                    )
                );
            }
        }
        return ccrNodeTasks;
    }

    protected record CcrNodeTask(String remoteCluster, String leaderIndex, String followerIndex, int shardId) {}

    protected static boolean indexExists(String index) throws IOException {
        Response response = adminClient().performRequest(new Request("HEAD", "/" + index));
        return RestStatus.OK.getStatus() == response.getStatusLine().getStatusCode();
    }

    protected static List<String> verifyDataStream(final RestClient client, final String name, final String... expectedBackingIndices)
        throws IOException {
        Request request = new Request("GET", "/_data_stream/" + name);
        Map<String, ?> response = toMap(client.performRequest(request));
        List<?> retrievedDataStreams = (List<?>) response.get("data_streams");
        assertThat(retrievedDataStreams, hasSize(1));
        List<?> actualBackingIndexItems = (List<?>) ((Map<?, ?>) retrievedDataStreams.get(0)).get("indices");
        assertThat(actualBackingIndexItems, hasSize(expectedBackingIndices.length));
        final List<String> actualBackingIndices = new ArrayList<>();
        for (int i = 0; i < expectedBackingIndices.length; i++) {
            Map<?, ?> actualBackingIndexItem = (Map<?, ?>) actualBackingIndexItems.get(i);
            String actualBackingIndex = (String) actualBackingIndexItem.get("index_name");
            String expectedBackingIndex = expectedBackingIndices[i];

            String actualDataStreamName = actualBackingIndex.substring(5, actualBackingIndex.indexOf('-', 5));
            String expectedDataStreamName = expectedBackingIndex.substring(5, expectedBackingIndex.indexOf('-', 5));
            assertThat(actualDataStreamName, equalTo(expectedDataStreamName));

            int actualGeneration = Integer.parseInt(actualBackingIndex.substring(actualBackingIndex.lastIndexOf('-')));
            int expectedGeneration = Integer.parseInt(expectedBackingIndex.substring(expectedBackingIndex.lastIndexOf('-')));
            assertThat(actualGeneration, equalTo(expectedGeneration));
            actualBackingIndices.add(actualBackingIndex);
        }
        return List.copyOf(actualBackingIndices);
    }

    protected static void createAutoFollowPattern(
        RestClient client,
        String name,
        String pattern,
        String remoteCluster,
        String followIndexPattern
    ) throws IOException {
        Request request = new Request("PUT", "/_ccr/auto_follow/" + name);
        try (XContentBuilder bodyBuilder = JsonXContent.contentBuilder()) {
            bodyBuilder.startObject();
            {
                bodyBuilder.array("leader_index_patterns", pattern);
                if (followIndexPattern != null) {
                    bodyBuilder.field("follow_index_pattern", followIndexPattern);
                }
                bodyBuilder.field("remote_cluster", remoteCluster);
            }
            bodyBuilder.endObject();
            request.setJsonEntity(Strings.toString(bodyBuilder));
        }
        assertOK(client.performRequest(request));
    }

    /**
     * Fix point in time when data stream backing index is first time queried.
     * This is required to avoid failures when running test at midnight.
     * (index is created for day0, but assertions are executed for day1 assuming different time based index name that does not exist)
     */
    private final LazyInitializable<Long, RuntimeException> time = new LazyInitializable<>(System::currentTimeMillis);

    protected String backingIndexName(String dataStreamName, int generation) {
        return DataStream.getDefaultBackingIndexName(dataStreamName, generation, time.getOrCompute());
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
        HttpHost httpHost = new HttpHost(
            url.substring(0, portSeparator),
            Integer.parseInt(url.substring(portSeparator + 1)),
            getProtocol()
        );
        return buildClient(settings, new HttpHost[] { httpHost });
    }

}
