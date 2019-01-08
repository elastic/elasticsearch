/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.upgrades;

import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.LuceneTestCase;
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

@LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/37231")
public class CCRIT extends AbstractUpgradeTestCase {

    private static final Logger LOGGER = LogManager.getLogger(CCRIT.class);

    private static final Version UPGRADE_FROM_VERSION =
        Version.fromString(System.getProperty("tests.upgrade_from_version"));

    private static final boolean SECOND_ROUND = "false".equals(System.getProperty("tests.first_round"));

    @Override
    protected boolean preserveClusterSettings() {
        return true;
    }

    public void testIndexFollowing() throws Exception {
        assumeTrue("CCR became available in 6.5 and test relies on a fix that was shipped with 6.5.4",
            UPGRADE_FROM_VERSION.onOrAfter(Version.V_6_5_4));
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
                if (SECOND_ROUND == false) {
                    index(leaderIndex, "2");
                    assertDocumentExists(leaderIndex, "1", "2");
                    assertBusy(() -> {
                        assertFollowerGlobalCheckpoint(followerIndex, 1);
                        assertDocumentExists(followerIndex, "1", "2");
                    });
                } else {
                    index(leaderIndex, "3");
                    assertDocumentExists(leaderIndex, "1", "2", "3");
                    assertBusy(() -> {
                        assertFollowerGlobalCheckpoint(followerIndex, 2);
                        assertDocumentExists(followerIndex, "1", "2", "3");
                    });
                }
                break;
            case UPGRADED:
                index(leaderIndex, "4");
                assertDocumentExists(leaderIndex, "1", "2", "3", "4");
                assertBusy(() -> {
                    assertFollowerGlobalCheckpoint(followerIndex, 3);
                    assertDocumentExists(followerIndex, "1", "2", "3", "4");
                });
                stopIndexFollowing(followerIndex);
                break;
            default:
                throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }
    }

    public void testAutoFollowing() throws Exception {
        assumeTrue("CCR became available in 6.5 and test relies on a fix that was shipped with 6.5.4",
            UPGRADE_FROM_VERSION.onOrAfter(Version.V_6_5_4));
        setupRemoteCluster();

        final Settings indexSettings = Settings.builder()
            .put("index.soft_deletes.enabled", true)
            .put("index.number_of_shards", 1)
            .build();

        String leaderIndex1 = "logs-20200101";
        String leaderIndex2 = "logs-20200102";
        String leaderIndex3 = "logs-20200103";

        switch (CLUSTER_TYPE) {
            case OLD:
                putAutoFollowPattern("test_pattern", "logs-*");
                createIndex(leaderIndex1, indexSettings);
                index(leaderIndex1, "1");
                assertBusy(() -> {
                    String followerIndex = "copy-" + leaderIndex1;
                    assertThat(getNumberOfSuccessfulFollowedIndices(), equalTo(1));
                    assertFollowerGlobalCheckpoint(followerIndex, 0);
                    assertDocumentExists(followerIndex, "1");
                });
                break;
            case MIXED:
                if (SECOND_ROUND == false) {
                    index(leaderIndex1, "2");
                    assertBusy(() -> {
                        String followerIndex = "copy-" + leaderIndex1;
                        assertFollowerGlobalCheckpoint(followerIndex, 1);
                        assertDocumentExists(followerIndex, "2");
                    });
                    // Auto follow stats are kept in-memory on master elected node
                    // and if this node get updated then auto follow stats are reset
                    int previousNumberOfSuccessfulFollowedIndices = getNumberOfSuccessfulFollowedIndices();
                    createIndex(leaderIndex2, indexSettings);
                    index(leaderIndex2, "1");
                    assertBusy(() -> {
                        String followerIndex = "copy-" + leaderIndex2;
                        assertThat(getNumberOfSuccessfulFollowedIndices(), equalTo(previousNumberOfSuccessfulFollowedIndices + 1));
                        assertFollowerGlobalCheckpoint(followerIndex, 0);
                        assertDocumentExists(followerIndex, "1");
                    });
                } else {
                    index(leaderIndex1, "3");
                    assertBusy(() -> {
                        String followerIndex = "copy-" + leaderIndex1;
                        assertFollowerGlobalCheckpoint(followerIndex, 2);
                        assertDocumentExists(followerIndex, "3");
                    });
                    index(leaderIndex2, "2");
                    assertBusy(() -> {
                        String followerIndex = "copy-" + leaderIndex2;
                        assertFollowerGlobalCheckpoint(followerIndex, 1);
                        assertDocumentExists(followerIndex, "2");
                    });

                    // Auto follow stats are kept in-memory on master elected node
                    // and if this node get updated then auto follow stats are reset
                    int previousNumberOfSuccessfulFollowedIndices = getNumberOfSuccessfulFollowedIndices();
                    createIndex(leaderIndex3, indexSettings);
                    index(leaderIndex3, "1");
                    assertBusy(() -> {
                        String followerIndex = "copy-" + leaderIndex3;
                        assertThat(getNumberOfSuccessfulFollowedIndices(), equalTo(previousNumberOfSuccessfulFollowedIndices + 1));
                        assertFollowerGlobalCheckpoint(followerIndex, 0);
                        assertDocumentExists(followerIndex, "1");
                    });
                }
                break;
            case UPGRADED:
                index(leaderIndex1, "4");
                assertBusy(() -> {
                    String followerIndex = "copy-" + leaderIndex1;
                    assertFollowerGlobalCheckpoint(followerIndex, 3);
                    assertDocumentExists(followerIndex, "4");
                });
                index(leaderIndex2, "3");
                assertBusy(() -> {
                    String followerIndex = "copy-" + leaderIndex2;
                    assertFollowerGlobalCheckpoint(followerIndex, 2);
                    assertDocumentExists(followerIndex, "3");
                });
                index(leaderIndex3, "2");
                assertBusy(() -> {
                    String followerIndex = "copy-" + leaderIndex3;
                    assertFollowerGlobalCheckpoint(followerIndex, 1);
                    assertDocumentExists(followerIndex, "2");
                });

                deleteAutoFollowPattern("test_pattern");

                stopIndexFollowing("copy-" + leaderIndex1);
                stopIndexFollowing("copy-" + leaderIndex2);
                stopIndexFollowing("copy-" + leaderIndex3);
                break;
            default:
                throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }
    }

    private static void stopIndexFollowing(String followerIndex) throws IOException {
        pauseFollow(followerIndex);
        closeIndex(followerIndex);
        unfollow(followerIndex);
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

    private static void putAutoFollowPattern(String name, String pattern) throws IOException {
        Request request = new Request("PUT", "/_ccr/auto_follow/" + name);
        request.setJsonEntity("{\"leader_index_patterns\": [\"" + pattern + "\"], \"remote_cluster\": \"local\"," +
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

    private static void assertDocumentExists(String index, String... ids) throws IOException {
        for (String id : ids) {
            Request request = new Request("HEAD", "/" + index + "/_doc/" + id);
            Response response = client().performRequest(request);
            assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        }
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

    private int getNumberOfSuccessfulFollowedIndices() throws IOException {
        Request statsRequest = new Request("GET", "/_ccr/stats");
        Map<?, ?> response = toMap(client().performRequest(statsRequest));
        Integer actualSuccessfulFollowedIndices = ObjectPath.eval("auto_follow_stats.number_of_successful_follow_indices", response);
        if (actualSuccessfulFollowedIndices != null) {
            return actualSuccessfulFollowedIndices;
        } else {
            return -1;
        }
    }

    private void assertFollowerGlobalCheckpoint(String followerIndex, int expectedFollowerCheckpoint) throws IOException {
        Request statsRequest = new Request("GET", "/" + followerIndex + "/_stats");
        statsRequest.addParameter("level", "shards");
        Map<?, ?> response = toMap(client().performRequest(statsRequest));
        LOGGER.info("INDEX STATS={}", response);
        assertThat(((Map) response.get("indices")).size(), equalTo(1));
        Integer actualFollowerCheckpoint = ObjectPath.eval("indices." + followerIndex + ".shards.0.0.seq_no.global_checkpoint", response);
        assertThat(actualFollowerCheckpoint, equalTo(expectedFollowerCheckpoint));
    }

    private static Map<String, Object> toMap(Response response) throws IOException {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
    }

}
