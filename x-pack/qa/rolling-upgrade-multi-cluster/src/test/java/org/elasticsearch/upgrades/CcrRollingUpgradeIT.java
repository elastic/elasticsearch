/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.upgrades;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.rest.action.search.RestSearchAction.TOTAL_HITS_AS_INT_PARAM;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class CcrRollingUpgradeIT extends AbstractMultiClusterUpgradeTestCase {

    public void testIndexFollowing() throws Exception {
        logger.info("clusterName={}, upgradeState={}", clusterName, upgradeState);

        if (clusterName == ClusterName.LEADER) {
            switch (upgradeState) {
                case NONE:
                    createLeaderIndex(leaderClient(), "leader_index1");
                    index(leaderClient(), "leader_index1", 64);
                    createLeaderIndex(leaderClient(), "leader_index2");
                    index(leaderClient(), "leader_index2", 64);
                    break;
                case ONE_THIRD:
                    break;
                case TWO_THIRD:
                    break;
                case ALL:
                    // At this point all nodes in both clusters have been updated and
                    // the leader cluster can now will leader_index4 in the follower cluster:
                    followIndex(leaderClient(), "follower", "leader_index4", "follower_index4");
                    assertBusy(() -> verifyTotalHitCount("follower_index4", 64, leaderClient()));
                    break;
                default:
                    throw new AssertionError("unexpected upgrade_state [" + upgradeState + "]");
            }
        } else if (clusterName == ClusterName.FOLLOWER) {
            switch (upgradeState) {
                case NONE:
                    followIndex(followerClient(), "leader", "leader_index1", "follower_index1");
                    assertBusy(() -> verifyTotalHitCount("follower_index1", 64, followerClient()));
                    break;
                case ONE_THIRD:
                    index(leaderClient(), "leader_index1", 64);
                    assertBusy(() -> verifyTotalHitCount("follower_index1", 128, followerClient()));

                    followIndex(followerClient(), "leader", "leader_index2", "follower_index2");
                    assertBusy(() -> verifyTotalHitCount("follower_index2", 64, followerClient()));
                    break;
                case TWO_THIRD:
                    index(leaderClient(), "leader_index1", 64);
                    assertBusy(() -> verifyTotalHitCount("follower_index1", 192, followerClient()));

                    index(leaderClient(), "leader_index2", 64);
                    assertBusy(() -> verifyTotalHitCount("follower_index2", 128, followerClient()));

                    createLeaderIndex(leaderClient(), "leader_index3");
                    index(leaderClient(), "leader_index3", 64);
                    followIndex(followerClient(), "leader", "leader_index3", "follower_index3");
                    assertBusy(() -> verifyTotalHitCount("follower_index3", 64, followerClient()));
                    break;
                case ALL:
                    index(leaderClient(), "leader_index1", 64);
                    assertBusy(() -> verifyTotalHitCount("follower_index1", 256, followerClient()));

                    index(leaderClient(), "leader_index2", 64);
                    assertBusy(() -> verifyTotalHitCount("follower_index2", 192, followerClient()));

                    index(leaderClient(), "leader_index3", 64);
                    assertBusy(() -> verifyTotalHitCount("follower_index3", 128, followerClient()));

                    // At this point the leader cluster has not been upgraded, but follower cluster has been upgrade.
                    // Create a leader index in the follow cluster and try to follow it in the leader cluster.
                    // This should fail, because the leader cluster at this point in time can't do file based recovery from follower.
                    createLeaderIndex(followerClient(), "leader_index4");
                    index(followerClient(), "leader_index4", 64);
                    ResponseException e = expectThrows(ResponseException.class,
                        () -> followIndex(leaderClient(), "follower", "leader_index4", "follower_index4"));
                    assertThat(e.getMessage(), containsString("the snapshot was created with Elasticsearch version ["));
                    assertThat(e.getMessage(), containsString("] which is higher than the version of this node ["));
                    break;
                default:
                    throw new AssertionError("unexpected upgrade_state [" + upgradeState + "]");
            }
        } else {
            throw new AssertionError("unexpected cluster_name [" + clusterName + "]");
        }
    }

    private static void createLeaderIndex(RestClient client, String indexName) throws IOException {
        Settings indexSettings = Settings.builder()
            .put("index.soft_deletes.enabled", true)
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .build();
        createIndex(client, indexName, indexSettings);
    }

    private static void createIndex(RestClient client, String name, Settings settings) throws IOException {
        Request request = new Request("PUT", "/" + name);
        request.setJsonEntity("{\n \"settings\": " + Strings.toString(settings) + "}");
        client.performRequest(request);
    }

    private static void followIndex(RestClient client, String leaderCluster, String leaderIndex, String followIndex) throws IOException {
        final Request request = new Request("PUT", "/" + followIndex + "/_ccr/follow?wait_for_active_shards=1");
        request.setJsonEntity("{\"remote_cluster\": \"" + leaderCluster + "\", \"leader_index\": \"" + leaderIndex +
            "\", \"read_poll_timeout\": \"10ms\"}");
        assertOK(client.performRequest(request));
    }

    private static void index(RestClient client, String index, int numDocs) throws IOException {
        for (int i = 0; i < numDocs; i++) {
            final Request request = new Request("POST", "/" + index + "/_doc/");
            request.setJsonEntity("{}");
            assertOK(client.performRequest(request));
        }
    }

    private static void verifyTotalHitCount(final String index,
                                            final int expectedTotalHits,
                                            final RestClient client) throws IOException {
        final Request request = new Request("GET", "/" + index + "/_search");
        request.addParameter(TOTAL_HITS_AS_INT_PARAM, "true");
        Map<?, ?> response = toMap(client.performRequest(request));
        final int totalHits = (int) XContentMapValues.extractValue("hits.total", response);
        assertThat(totalHits, equalTo(expectedTotalHits));
    }

}
