/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;

import java.io.IOException;

public class RestartIT extends ESCCRRestTestCase {

    public void testRestart() throws Exception {
        final int numberOfDocuments = 128;
        final String testsTargetCluster = System.getProperty("tests.target_cluster");
        switch (testsTargetCluster) {
            case "leader": {
                // create a single index "leader" on the leader
                createIndexAndIndexDocuments("leader", numberOfDocuments, client());
                break;
            }
            case "follow": {
                // follow "leader" with "follow-leader" on the follower
                followIndex("leader", "follow-leader");
                verifyFollower("follow-leader", numberOfDocuments, client());

                // now create an auto-follow pattern for "leader-*"
                final Request putPatternRequest = new Request("PUT", "/_ccr/auto_follow/leader_cluster_pattern");
                putPatternRequest.setJsonEntity("{" +
                        "\"leader_index_patterns\": [\"leader-*\"]," +
                        "\"remote_cluster\": \"leader_cluster\"," +
                        "\"follow_index_pattern\":\"follow-{{leader_index}}\"}");
                assertOK(client().performRequest(putPatternRequest));
                try (RestClient leaderClient = buildLeaderClient()) {
                    // create "leader-1" on the leader, which should be replicated to "follow-leader-1" on the follower
                    createIndexAndIndexDocuments("leader-1", numberOfDocuments, leaderClient);
                    // the follower should catch up
                    verifyFollower("follow-leader-1", numberOfDocuments, client());
                }
                break;
            }
            case "follow-restart": {
                try (RestClient leaderClient = buildLeaderClient()) {
                    // create "leader-2" on the leader, and index some additional documents into existing indices
                    createIndexAndIndexDocuments("leader-2", numberOfDocuments, leaderClient);
                    for (final String index : new String[]{"leader", "leader-1", "leader-2"}) {
                        indexDocuments(index, numberOfDocuments, numberOfDocuments, leaderClient);
                    }
                    // the followers should catch up
                    for (final String index : new String[]{"follow-leader", "follow-leader-1", "follow-leader-2"}) {
                        logger.info("verifying {} using {}", index, client().getNodes());
                        verifyFollower(index, 2 * numberOfDocuments, client());
                    }
                    // one more index "leader-3" on the follower
                    createIndexAndIndexDocuments("leader-3", 2 * numberOfDocuments, leaderClient);
                    // the follower should catch up
                    verifyFollower("follow-leader-3", 2 * numberOfDocuments, client());
                }
                break;
            }
            default: {
                throw new IllegalArgumentException("unexpected value [" + testsTargetCluster + "] for tests.target_cluster");
            }
        }
    }

    private void createIndexAndIndexDocuments(final String index, final int numberOfDocuments, final RestClient client) throws IOException {
        final Settings settings = Settings.builder().put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true).build();
        final Request createIndexRequest = new Request("PUT", "/" + index);
        createIndexRequest.setJsonEntity("{\"settings\":" + Strings.toString(settings) + "}");
        assertOK(client.performRequest(createIndexRequest));
        indexDocuments(index, numberOfDocuments, 0, client);
    }

    private void indexDocuments(
            final String index,
            final int numberOfDocuments,
            final int initial,
            final RestClient client) throws IOException {
        for (int i = 0, j = initial; i < numberOfDocuments; i++, j++) {
            index(client, index, Integer.toString(j), "field", j);
        }
        assertOK(client.performRequest(new Request("POST", "/" + index + "/_refresh")));
    }

    private void verifyFollower(final String index, final int numberOfDocuments, final RestClient client) throws Exception {
        assertBusy(() -> {
            ensureYellow(index, client);
            verifyDocuments(index, numberOfDocuments, "*:*", client);
        });
    }

}
