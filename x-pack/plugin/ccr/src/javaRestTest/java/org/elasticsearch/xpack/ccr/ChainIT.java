/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;

public class ChainIT extends AbstractCCRRestTestCase {

    public static LocalClusterConfigProvider commonConfig = c -> c.module("x-pack-ccr")
        .module("analysis-common")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.license.self_generated.type", "trial")
        .feature(FeatureFlag.TIME_SERIES_MODE)
        .user("admin", "admin-password", "superuser", false);

    public static ElasticsearchCluster leaderCluster = ElasticsearchCluster.local().name("leader-cluster").apply(commonConfig).build();

    public static ElasticsearchCluster middleCluster = ElasticsearchCluster.local()
        .name("middle-cluster")
        .apply(commonConfig)
        .setting("cluster.remote.leader_cluster.seeds", () -> "\"" + leaderCluster.getTransportEndpoints() + "\"")
        .build();

    public static ElasticsearchCluster followerCluster = ElasticsearchCluster.local()
        .name("follow-cluster")
        .apply(commonConfig)
        .setting("cluster.remote.middle_cluster.seeds", () -> "\"" + middleCluster.getTransportEndpoints() + "\"")
        .setting("cluster.remote.leader_cluster.seeds", () -> "\"" + leaderCluster.getTransportEndpoints() + "\"")
        .build();

    @ClassRule
    public static RuleChain ruleChain = RuleChain.outerRule(leaderCluster).around(middleCluster).around(followerCluster);

    public ChainIT(@Name("targetCluster") AbstractCCRRestTestCase.TargetCluster targetCluster) {
        super(targetCluster);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return leaderMiddleFollower();
    }

    @Override
    protected ElasticsearchCluster getLeaderCluster() {
        return leaderCluster;
    }

    @Override
    protected ElasticsearchCluster getFollowerCluster() {
        return followerCluster;
    }

    @Override
    protected ElasticsearchCluster getMiddleCluster() {
        return middleCluster;
    }

    public void testFollowIndex() throws Exception {
        final int numDocs = 128;
        final String leaderIndexName = "leader";
        final String middleIndexName = "middle";
        switch (targetCluster) {
            case LEADER:
                logger.info("Running against leader cluster");
                String mapping = "";
                if (randomBoolean()) { // randomly do source filtering on indexing
                    mapping = """
                        "_source": {  "includes": ["field"],  "excludes": ["filtered_field"]}""";
                }
                createIndex(adminClient(), leaderIndexName, Settings.EMPTY, mapping, null);
                for (int i = 0; i < numDocs; i++) {
                    logger.info("Indexing doc [{}]", i);
                    index(client(), leaderIndexName, Integer.toString(i), "field", i, "filtered_field", "true");
                }
                refresh(adminClient(), leaderIndexName);
                verifyDocuments(leaderIndexName, numDocs, "filtered_field:true");
                break;
            case MIDDLE:
                logger.info("Running against middle cluster");
                followIndex("leader_cluster", leaderIndexName, middleIndexName);
                assertBusy(() -> verifyDocuments(middleIndexName, numDocs, "filtered_field:true"));
                try (RestClient leaderClient = buildLeaderClient()) {
                    int id = numDocs;
                    index(leaderClient, leaderIndexName, Integer.toString(id), "field", id, "filtered_field", "true");
                    index(leaderClient, leaderIndexName, Integer.toString(id + 1), "field", id + 1, "filtered_field", "true");
                    index(leaderClient, leaderIndexName, Integer.toString(id + 2), "field", id + 2, "filtered_field", "true");
                }
                assertBusy(() -> verifyDocuments(middleIndexName, numDocs + 3, "filtered_field:true"));
                break;
            case FOLLOWER:
                logger.info("Running against follow cluster");
                final String followIndexName = "follow";
                followIndex("middle_cluster", middleIndexName, followIndexName);
                assertBusy(() -> verifyDocuments(followIndexName, numDocs + 3, "filtered_field:true"));

                try (RestClient leaderClient = buildLeaderClient()) {
                    int id = numDocs + 3;
                    index(leaderClient, leaderIndexName, Integer.toString(id), "field", id, "filtered_field", "true");
                    index(leaderClient, leaderIndexName, Integer.toString(id + 1), "field", id + 1, "filtered_field", "true");
                    index(leaderClient, leaderIndexName, Integer.toString(id + 2), "field", id + 2, "filtered_field", "true");
                }

                try (RestClient middleClient = buildMiddleClient()) {
                    assertBusy(() -> verifyDocuments(middleIndexName, numDocs + 6, "filtered_field:true", middleClient));
                }

                assertBusy(() -> verifyDocuments(followIndexName, numDocs + 6, "filtered_field:true"));
                break;
        }
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

}
