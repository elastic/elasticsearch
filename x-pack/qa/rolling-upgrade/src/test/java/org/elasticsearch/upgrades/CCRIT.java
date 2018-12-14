/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.upgrades;

import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ObjectPath;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class CCRIT extends AbstractUpgradeTestCase {

    private static final Logger LOGGER = LogManager.getLogger(CCRIT.class);

    private static final Version UPGRADE_FROM_VERSION =
        Version.fromString(System.getProperty("tests.upgrade_from_version"));

    @Override
    protected boolean preserveClusterSettings() {
        return true;
    }

    public void testIndexFollowing() throws Exception {
        assumeTrue("CCR became available in 6.5", UPGRADE_FROM_VERSION.onOrAfter(Version.V_6_5_0));
        setupRemoteCluster();

        final String leaderIndex = "my-leader-index";
        final String followerIndex = "my-follower-index";

        switch (CLUSTER_TYPE) {
            case OLD:
                Settings indexSettings = Settings.builder()
                    .put("index.soft_deletes.enabled", true)
                    .put("index.number_of_shards", 1)
                    .build();
                createIndex(leaderIndex, indexSettings);
                followIndex(leaderIndex, followerIndex);
                index(leaderIndex, "1");
                assertDocumentExists(leaderIndex, "1");
                assertBusy(() -> {
                    assertFollowerGlobalCheckpoint(followerIndex, 0);
                    assertDocumentExists(followerIndex, "1");
                });
                break;
            case MIXED:
                index(leaderIndex, "2");
                assertDocumentExists(leaderIndex, "2");
                assertBusy(() -> {
                    assertFollowerGlobalCheckpoint(followerIndex, 1);
                    assertDocumentExists(followerIndex, "2");
                });
                break;
            case UPGRADED:
                index(leaderIndex, "3");
                assertDocumentExists(leaderIndex, "3");
                assertBusy(() -> {
                    assertFollowerGlobalCheckpoint(followerIndex, 2);
                    assertDocumentExists(followerIndex, "3");
                });
                pauseFollow(followerIndex);
                closeIndex(followerIndex);
                unfollow(followerIndex);
                break;
            default:
                throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }
    }

    public void testAutoFollowing() throws Exception {
        assumeTrue("CCR became available in 6.5", UPGRADE_FROM_VERSION.onOrAfter(Version.V_6_5_0));
        setupRemoteCluster();

        final Settings indexSettings = Settings.builder()
            .put("index.soft_deletes.enabled", true)
            .put("index.number_of_shards", 1)
            .build();

        String leaderIndex1 = "logs-20200101";
        String leaderIndex2 = "logs-20200102";

        switch (CLUSTER_TYPE) {
            case OLD:
                putAutoFollowPattern("test_pattern");
                createIndex(leaderIndex1, indexSettings);
                index(leaderIndex1, "1");
                assertBusy(() -> {
                    String followerIndex = "copy-" + leaderIndex1;
                    assertDocumentExists(followerIndex, "1");
                    assertNumberOfSuccessfulFollowedIndices(1);
                    assertFollowerGlobalCheckpoint(followerIndex, 0);
                });
                break;
            case MIXED:
                index(leaderIndex1, "2");
                assertBusy(() -> {
                    String followerIndex = "copy-" + leaderIndex1;
                    assertDocumentExists(followerIndex, "2");
                    assertFollowerGlobalCheckpoint(followerIndex, 1);
                });

                createIndex(leaderIndex2, indexSettings);
                index(leaderIndex2, "1");
                assertBusy(() -> {
                    String followerIndex = "copy-" + leaderIndex2;
                    assertDocumentExists(followerIndex, "1");
                    assertNumberOfSuccessfulFollowedIndices(2);
                    assertFollowerGlobalCheckpoint(followerIndex, 0);
                });
                break;
            case UPGRADED:
                index(leaderIndex1, "3");
                assertBusy(() -> {
                    String followerIndex = "copy-" + leaderIndex1;
                    assertDocumentExists(followerIndex, "3");
                    assertFollowerGlobalCheckpoint(followerIndex, 2);
                });
                index(leaderIndex2, "2");
                assertBusy(() -> {
                    String followerIndex = "copy-" + leaderIndex2;
                    assertDocumentExists(followerIndex, "2");
                    assertFollowerGlobalCheckpoint(followerIndex, 1);
                });

                deleteAutoFollowPattern("test_pattern");

                pauseFollow("copy-" + leaderIndex1);
                closeIndex("copy-" + leaderIndex1);
                unfollow("copy-" + leaderIndex1);

                pauseFollow("copy-" + leaderIndex2);
                closeIndex("copy-" + leaderIndex2);
                unfollow("copy-" + leaderIndex2);
                break;
            default:
                throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }
    }

    private static void followIndex(String leaderIndex, String followIndex) throws IOException {
        final Request request = new Request("PUT", "/" + followIndex + "/_ccr/follow");
        request.setJsonEntity("{\"remote_cluster\": \"local\", \"leader_index\": \"" + leaderIndex +
            "\", \"read_poll_timeout\": \"10ms\"}");
        assertOK(client().performRequest(request));
    }

    private static void pauseFollow(String followIndex) throws IOException {
        assertOK(client().performRequest(new Request("POST", "/" + followIndex + "/_ccr/pause_follow")));
    }

    private static void unfollow(String followIndex) throws IOException {
        assertOK(client().performRequest(new Request("POST", "/" + followIndex + "/_ccr/unfollow")));
    }

    private static void putAutoFollowPattern(String patternName) throws IOException {
        Request request = new Request("PUT", "/_ccr/auto_follow/" + patternName);
        request.setJsonEntity("{\"leader_index_patterns\": [\"logs-*\"], \"remote_cluster\": \"local\"," +
            "\"follow_index_pattern\": \"copy-{{leader_index}}\", \"read_poll_timeout\": \"10ms\"}");
        assertOK(client().performRequest(request));
    }

    private static void deleteAutoFollowPattern(String patternName) throws IOException {
        Request request = new Request("DELETE", "/_ccr/auto_follow/" + patternName);
        assertOK(client().performRequest(request));
    }

    private static void index(String index, String id) throws IOException {
        Request request = new Request("POST", "/" + index + "/_doc/" + id);
        request.setJsonEntity("{}");
        assertOK(client().performRequest(request));
    }

    private static void assertDocumentExists(String index, String id) throws IOException {
        Request request = new Request("HEAD", "/" + index + "/_doc/" + id);
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
    }

    private static void setupRemoteCluster() throws IOException {
        Request request = new Request("GET", "/_nodes");
        Map<?, ?> nodesResponse = (Map<?, ?>) toMap(client().performRequest(request)).get("nodes");
        // Select node info of first node (we don't know the node id):
        nodesResponse = (Map<?, ?>) nodesResponse.get(nodesResponse.keySet().iterator().next());
        String transportAddress = (String) nodesResponse.get("transport_address");

        LOGGER.info("Configuring local remote cluster [{}]", transportAddress);
        request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"persistent\": {\"cluster.remote.local.seeds\": \"" + transportAddress + "\"}}");
        assertThat(client().performRequest(request).getStatusLine().getStatusCode(), equalTo(200));
    }

    private void assertNumberOfSuccessfulFollowedIndices(int expectedNumberOfSuccessfulFollowedIndices) throws IOException {
        Request statsRequest = new Request("GET", "/_ccr/stats");
        Map<?, ?> response = toMap(client().performRequest(statsRequest));
        Integer actualSuccessfulFollowedInidices = ObjectPath.eval("auto_follow_stats.number_of_successful_follow_indices", response);
        assertThat(actualSuccessfulFollowedInidices, equalTo(expectedNumberOfSuccessfulFollowedIndices));
    }

    private void assertFollowerGlobalCheckpoint(String followerIndex, int expectedFollowerCheckpoint) throws IOException {
        Request statsRequest = new Request("GET", "/" + followerIndex + "/_ccr/stats");
        Map<?, ?> response = toMap(client().performRequest(statsRequest));
        LOGGER.info("CHECK ME={}", response);
        String index = ObjectPath.eval("indices.0.index", response);
        assertThat(index, equalTo(followerIndex));
        Integer actualFollowerCheckpoint = ObjectPath.eval("indices.0.shards.0.follower_global_checkpoint", response);
        assertThat(actualFollowerCheckpoint, equalTo(expectedFollowerCheckpoint));
    }

    private static Map<String, Object> toMap(Response response) throws IOException {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
    }

}
