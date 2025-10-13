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
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.logging.JsonLogLine;
import org.elasticsearch.common.logging.JsonLogsStream;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.LogType;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.elasticsearch.xcontent.ObjectPath.eval;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.Is.is;

public class DowngradeLicenseFollowIndexIT extends AbstractCCRRestTestCase {

    public static LocalClusterConfigProvider commonConfig = c -> c.module("x-pack-ccr")
        .module("analysis-common")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .user("admin", "admin-password", "superuser", false);

    public static ElasticsearchCluster leaderCluster = ElasticsearchCluster.local().name("leader-cluster").apply(commonConfig).build();

    public static ElasticsearchCluster followerCluster = ElasticsearchCluster.local()
        .name("follow-cluster")
        .apply(commonConfig)
        .setting("cluster.remote.leader_cluster.seeds", () -> "\"" + leaderCluster.getTransportEndpoints() + "\"")
        .build();

    @ClassRule
    public static RuleChain ruleChain = RuleChain.outerRule(leaderCluster).around(followerCluster);

    public DowngradeLicenseFollowIndexIT(@Name("targetCluster") TargetCluster targetCluster) {
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

    public void testDowngradeRemoteClusterToBasic() throws Exception {
        assumeTrue("Test should only run with target_cluster=follow", targetCluster == TargetCluster.FOLLOWER);

        {
            Request request = new Request("PUT", "/_ccr/auto_follow/test_pattern");
            request.setJsonEntity("""
                {"leader_index_patterns": ["logs-*"], "remote_cluster": "leader_cluster"}""");
            assertOK(client().performRequest(request));
        }

        String index1 = "logs-20190101";
        try (RestClient leaderClient = buildLeaderClient()) {
            createNewIndexAndIndexDocs(leaderClient, index1);
        }

        assertBusy(() -> {
            ensureYellow(index1);
            verifyDocuments(index1, 5, "filtered_field:true");
        }, 60, TimeUnit.SECONDS);

        String index2 = "logs-20190102";
        try (RestClient leaderClient = buildLeaderClient()) {
            Request request = new Request("POST", "/_license/start_basic");
            request.addParameter("acknowledge", "true");
            Map<?, ?> response = toMap(leaderClient.performRequest(request));
            assertThat(response.get("basic_was_started"), is(true));
            assertThat(response.get("acknowledged"), is(true));

            createNewIndexAndIndexDocs(leaderClient, index2);
            index(leaderClient, index1, "5", "field", 5, "filtered_field", "true");
        }

        assertBusy(() -> {
            Request statsRequest = new Request("GET", "/_ccr/stats");
            Map<?, ?> response = toMap(client().performRequest(statsRequest));
            assertThat(eval("auto_follow_stats.number_of_successful_follow_indices", response), equalTo(1));
            assertThat(eval("auto_follow_stats.number_of_failed_remote_cluster_state_requests", response), greaterThanOrEqualTo(1));
            assertThat(
                eval("auto_follow_stats.recent_auto_follow_errors.0.auto_follow_exception.reason", response),
                containsString("the license mode [BASIC] on cluster [leader_cluster] does not enable [ccr]")
            );

            // Follow indices actively following leader indices before the downgrade to basic license remain to follow
            // the leader index after the downgrade, so document with id 5 should be replicated to follower index:
            verifyDocuments(index1, 6, "filtered_field:true");

            // Index2 was created in leader cluster after the downgrade and therefor the auto follow coordinator in
            // follow cluster should not pick that index up:
            assertThat(indexExists(adminClient(), index2), is(false));

            // parse the logs and ensure that the auto-coordinator skipped coordination on the leader cluster
            assertBusy(() -> {
                try (
                    InputStream in = followerCluster.getNodeLog(0, LogType.SERVER_JSON);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
                    Stream<JsonLogLine> stream = JsonLogsStream.from(reader)
                ) {
                    assertTrue(stream.anyMatch(autoFollowCoordinatorWarn()::matches));
                }
            });
        }, 60, TimeUnit.SECONDS);

        // Manually following index2 also does not work after the downgrade:
        Exception e = expectThrows(ResponseException.class, () -> followIndex("leader_cluster", index2));
        assertThat(e.getMessage(), containsString("the license mode [BASIC] on cluster [leader_cluster] does not enable [ccr]"));
    }

    private Matcher<JsonLogLine> autoFollowCoordinatorWarn() {
        return new FeatureMatcher<JsonLogLine, Boolean>(Matchers.is(true), "autoFollowCoordinatorWarn", "autoFollowCoordinatorWarn") {

            @Override
            protected Boolean featureValueOf(JsonLogLine actual) {
                return actual.getLevel().equals("WARN")
                    && actual.getComponent().contains("AutoFollowCoordinator")
                    && actual.getNodeName().startsWith("follow-cluster-0")
                    && actual.getMessage().contains("failure occurred while fetching cluster state for auto follow pattern [test_pattern]")
                    && actual.stacktrace()
                        .get(0)
                        .contains(
                            "org.elasticsearch.ElasticsearchStatusException: can not fetch remote cluster state "
                                + "as the remote cluster [leader_cluster] is not licensed for [ccr]; the license mode [BASIC]"
                                + " on cluster [leader_cluster] does not enable [ccr]"
                        );
            }
        };
    }

    private void createNewIndexAndIndexDocs(RestClient client, String index) throws IOException {
        Request request = new Request("PUT", "/" + index);
        request.setJsonEntity("""
            {"mappings": {"properties": {"field": {"type": "keyword"}}}}""");
        assertOK(client.performRequest(request));

        for (int i = 0; i < 5; i++) {
            String id = Integer.toString(i);
            index(client, index, id, "field", i, "filtered_field", "true");
        }
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

}
