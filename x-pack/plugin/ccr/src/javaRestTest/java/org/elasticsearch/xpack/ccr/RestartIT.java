/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;

import java.io.IOException;

public class RestartIT extends AbstractCCRRestTestCase {

    public static LocalClusterConfigProvider commonConfig = c -> c.module("x-pack-ccr")
        .module("analysis-common")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.license.self_generated.type", "trial")
        .user("admin", "admin-password", "superuser", false);

    public static ElasticsearchCluster leaderCluster = ElasticsearchCluster.local().name("leader-cluster").apply(commonConfig).build();

    public static ElasticsearchCluster followerCluster = ElasticsearchCluster.local()
        .name("follow-cluster")
        .apply(commonConfig)
        .setting("cluster.remote.leader_cluster.seeds", () -> "\"" + leaderCluster.getTransportEndpoints() + "\"")
        .build();

    @ClassRule
    public static RuleChain ruleChain = RuleChain.outerRule(leaderCluster).around(followerCluster);

    public RestartIT(@Name("targetCluster") TargetCluster targetCluster) {
        super(targetCluster);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return leaderFollower();
    }

    @Override
    protected ElasticsearchCluster getLeaderCluster() {
        return leaderCluster;
    }

    @Override
    protected ElasticsearchCluster getFollowerCluster() {
        return followerCluster;
    }

    public void testRestart() throws Exception {
        final int numberOfDocuments = 128;
        switch (targetCluster) {
            case LEADER -> {
                // create a single index "leader" on the leader
                createIndexAndIndexDocuments("leader", numberOfDocuments, client());
            }
            case FOLLOWER -> {
                // follow "leader" with "follow-leader" on the follower
                followIndex("leader", "follow-leader");
                verifyFollower("follow-leader", numberOfDocuments, client());

                // now create an auto-follow pattern for "leader-*"
                final Request putPatternRequest = new Request("PUT", "/_ccr/auto_follow/leader_cluster_pattern");
                putPatternRequest.setJsonEntity("""
                    {
                      "leader_index_patterns": [
                        "leader-*"
                      ],
                      "remote_cluster": "leader_cluster",
                      "follow_index_pattern": "follow-{{leader_index}}"
                    }""");
                assertOK(client().performRequest(putPatternRequest));
                try (RestClient leaderClient = buildLeaderClient()) {
                    // create "leader-1" on the leader, which should be replicated to "follow-leader-1" on the follower
                    createIndexAndIndexDocuments("leader-1", numberOfDocuments, leaderClient);
                    // the follower should catch up
                    verifyFollower("follow-leader-1", numberOfDocuments, client());
                }

                followerCluster.restart(false);
                closeClients();
                initClient();

                try (RestClient leaderClient = buildLeaderClient()) {
                    // create "leader-2" on the leader, and index some additional documents into existing indices
                    createIndexAndIndexDocuments("leader-2", numberOfDocuments, leaderClient);
                    for (final String index : new String[] { "leader", "leader-1", "leader-2" }) {
                        indexDocuments(index, numberOfDocuments, numberOfDocuments, leaderClient);
                    }
                    // the followers should catch up
                    for (final String index : new String[] { "follow-leader", "follow-leader-1", "follow-leader-2" }) {
                        logger.info("verifying {} using {}", index, client().getNodes());
                        verifyFollower(index, 2 * numberOfDocuments, client());
                    }
                    // one more index "leader-3" on the follower
                    createIndexAndIndexDocuments("leader-3", 2 * numberOfDocuments, leaderClient);
                    // the follower should catch up
                    verifyFollower("follow-leader-3", 2 * numberOfDocuments, client());
                }
            }
            default -> {
                throw new IllegalArgumentException("unexpected value [" + targetCluster + "] for targetCluster");
            }
        }
    }

    private void createIndexAndIndexDocuments(final String index, final int numberOfDocuments, final RestClient client) throws IOException {
        final Request createIndexRequest = new Request("PUT", "/" + index);
        createIndexRequest.setJsonEntity("{\"settings\":" + Strings.toString(Settings.EMPTY) + "}");
        assertOK(client.performRequest(createIndexRequest));
        indexDocuments(index, numberOfDocuments, 0, client);
    }

    private void indexDocuments(final String index, final int numberOfDocuments, final int initial, final RestClient client)
        throws IOException {
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

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

}
