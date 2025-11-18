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
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;

import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;

public class CcrMultiClusterLicenseIT extends AbstractCCRRestTestCase {

    public static LocalClusterConfigProvider commonConfig = c -> c.module("x-pack-ccr")
        .module("analysis-common")
        .setting("xpack.security.enabled", "true")
        .user("admin", "admin-password", "superuser", false);

    public static ElasticsearchCluster leaderCluster = ElasticsearchCluster.local().name("leader-cluster").apply(commonConfig).build();

    public static ElasticsearchCluster followerCluster = ElasticsearchCluster.local()
        .name("follow-cluster")
        .apply(commonConfig)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("cluster.remote.leader_cluster.seeds", () -> "\"" + leaderCluster.getTransportEndpoints() + "\"")
        .build();

    @ClassRule
    public static RuleChain ruleChain = RuleChain.outerRule(leaderCluster).around(followerCluster);

    public CcrMultiClusterLicenseIT(@Name("targetCluster") TargetCluster targetCluster) {
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

    public void testFollow() {
        if (targetCluster == TargetCluster.FOLLOWER) {
            final Request request = new Request("PUT", "/follower/_ccr/follow");
            request.setJsonEntity("""
                {"remote_cluster": "leader_cluster", "leader_index": "leader"}
                """);
            assertNonCompliantLicense(request, "remote index [leader_cluster:leader] metadata");
        }
    }

    public void testAutoFollow() {
        if (targetCluster == TargetCluster.FOLLOWER) {
            final Request request = new Request("PUT", "/_ccr/auto_follow/test_pattern");
            request.setJsonEntity("""
                {"leader_index_patterns":["*"], "remote_cluster": "leader_cluster"}
                """);
            assertNonCompliantLicense(request, "remote cluster state");
        }
    }

    private static void assertNonCompliantLicense(final Request request, final String fetch) {
        final ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        final String expected = String.format(
            Locale.ROOT,
            "can not fetch %s as the remote cluster [%s] is not licensed for [ccr]; "
                + "the license mode [BASIC] on cluster [%2$s] does not enable [ccr]",
            fetch,
            "leader_cluster"
        );
        assertThat(e, hasToString(containsString(expected)));
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }
}
