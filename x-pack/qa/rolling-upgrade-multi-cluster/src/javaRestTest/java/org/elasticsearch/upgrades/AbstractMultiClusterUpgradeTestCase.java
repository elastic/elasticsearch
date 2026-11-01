/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public abstract class AbstractMultiClusterUpgradeTestCase extends ESRestTestCase {

    private static final String OLD_CLUSTER_VERSION = System.getProperty("tests.upgrade_from_version");

    protected static final Version UPGRADE_FROM_VERSION = Version.fromString(OLD_CLUSTER_VERSION);

    private static final ElasticsearchCluster leaderCluster = ElasticsearchCluster.local()
        .name("leader")
        .distribution(DistributionType.DEFAULT)
        .version(OLD_CLUSTER_VERSION, isOldClusterDetachedVersion())
        .nodes(3)
        .setting("repositories.url.allowed_urls", "http://snapshot.test*")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.watcher.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .build();

    private static final ElasticsearchCluster followerCluster = ElasticsearchCluster.local()
        .name("follower")
        .distribution(DistributionType.DEFAULT)
        .version(OLD_CLUSTER_VERSION, isOldClusterDetachedVersion())
        .nodes(3)
        .setting("repositories.url.allowed_urls", "http://snapshot.test*")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.watcher.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(leaderCluster).around(followerCluster);

    // Tracks which nodes of each cluster have been upgraded
    private static final Set<Integer> upgradedLeaderNodes = new HashSet<>();
    private static final Set<Integer> upgradedFollowerNodes = new HashSet<>();

    private static RestClient leaderClient;
    private static RestClient followerClient;

    private static boolean remoteClustersConfigured = false;
    private static ClusterName lastClusterName = null;

    protected final ClusterName clusterName;
    protected final UpgradeState upgradeState;
    private final int targetLeaderNodesUpgraded;
    private final int targetFollowerNodesUpgraded;

    /**
     * Parameterized over (clusterName, leaderNodesUpgraded, followerNodesUpgraded).
     * The `follower` cluster is fully upgraded before the leader starts upgrading.
     */
    @ParametersFactory(shuffle = false)
    public static Iterable<Object[]> parameters() {
        return List.of(
            new Object[] { ClusterName.LEADER, 0, 0 },
            new Object[] { ClusterName.FOLLOWER, 0, 0 },
            new Object[] { ClusterName.FOLLOWER, 0, 1 },
            new Object[] { ClusterName.FOLLOWER, 0, 2 },
            new Object[] { ClusterName.FOLLOWER, 0, 3 },
            new Object[] { ClusterName.LEADER, 1, 3 },
            new Object[] { ClusterName.LEADER, 2, 3 },
            new Object[] { ClusterName.LEADER, 3, 3 }
        );
    }

    protected AbstractMultiClusterUpgradeTestCase(
        @Name("clusterName") ClusterName clusterName,
        @Name("leaderNodesUpgraded") int leaderNodesUpgraded,
        @Name("followerNodesUpgraded") int followerNodesUpgraded
    ) {
        this.clusterName = clusterName;
        this.targetLeaderNodesUpgraded = leaderNodesUpgraded;
        this.targetFollowerNodesUpgraded = followerNodesUpgraded;
        this.upgradeState = UpgradeState.fromNodesUpgraded(clusterName == ClusterName.LEADER ? leaderNodesUpgraded : followerNodesUpgraded);
    }

    @Before
    public void upgradeNodesAndConfigureClusters() throws IOException {
        boolean didClose = false;

        // Switch main test client when the target cluster changes between parameter sets.
        if (clusterName != lastClusterName) {
            closeClients();
            didClose = true;
            lastClusterName = clusterName;
        }

        // Upgrade follower nodes if needed (follower is always upgraded first).
        if (upgradedFollowerNodes.size() < targetFollowerNodesUpgraded) {
            if (didClose == false) {
                closeClients();
                didClose = true;
            }
            for (int i = 0; i < targetFollowerNodesUpgraded; i++) {
                if (upgradedFollowerNodes.add(i)) {
                    logger.info("Upgrading follower node {} to current version", i);
                    followerCluster.upgradeNodeToVersion(i, org.elasticsearch.test.cluster.util.Version.CURRENT);
                }
            }
            IOUtils.close(followerClient);
            followerClient = buildClusterClient(followerCluster);
            // Upgraded nodes get new transport ports, so update the remote cluster seed
            if (remoteClustersConfigured) {
                configureFollowerRemoteClusters();
            }
        }

        // Upgrade leader nodes if needed.
        if (upgradedLeaderNodes.size() < targetLeaderNodesUpgraded) {
            if (didClose == false) {
                closeClients();
                didClose = true;
            }
            for (int i = 0; i < targetLeaderNodesUpgraded; i++) {
                if (upgradedLeaderNodes.add(i)) {
                    logger.info("Upgrading leader node {} to current version", i);
                    leaderCluster.upgradeNodeToVersion(i, org.elasticsearch.test.cluster.util.Version.CURRENT);
                }
            }
            IOUtils.close(leaderClient);
            leaderClient = buildClusterClient(leaderCluster);
            // Upgraded nodes get new transport ports, so update the remote cluster seed
            if (remoteClustersConfigured) {
                configureLeaderRemoteClusters();
            }
        }

        if (didClose) {
            initClient();
        }

        // Configure cross-cluster remote settings once, after both clusters are started.
        if (remoteClustersConfigured == false) {
            leaderClient = buildClusterClient(leaderCluster);
            followerClient = buildClusterClient(followerCluster);
            configureLeaderRemoteClusters();
            configureFollowerRemoteClusters();
            remoteClustersConfigured = true;
        }
    }

    @AfterClass
    public static void destroyClients() throws IOException {
        try {
            IOUtils.close(leaderClient, followerClient);
        } finally {
            leaderClient = null;
            followerClient = null;
            remoteClustersConfigured = false;
            lastClusterName = null;
            upgradedLeaderNodes.clear();
            upgradedFollowerNodes.clear();
        }
    }

    @Override
    protected String getTestRestCluster() {
        return clusterName == ClusterName.LEADER ? leaderCluster.getHttpAddresses() : followerCluster.getHttpAddresses();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    protected static RestClient leaderClient() {
        return leaderClient;
    }

    protected static RestClient followerClient() {
        return followerClient;
    }

    protected static Map<?, ?> toMap(Response response) throws IOException {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
    }

    private RestClient buildClusterClient(ElasticsearchCluster cluster) throws IOException {
        final HttpHost[] hosts = Stream.of(cluster.getHttpAddresses().split(","))
            .map(String::trim)
            .map(HttpHost::create)
            .toArray(HttpHost[]::new);
        return buildClient(restAdminSettings(), hosts);
    }

    private void configureLeaderRemoteClusters() throws IOException {
        final String leaderSeed = leaderCluster.getTransportEndpoint(2);
        logger.info("Configuring leader remote cluster [{}]", leaderSeed);
        final Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(Strings.format("""
            {
              "persistent": {
                "cluster.remote.leader.seeds": "%s"
              }
            }""", leaderSeed));
        assertOK(leaderClient.performRequest(request));
        assertOK(followerClient.performRequest(request));
    }

    private void configureFollowerRemoteClusters() throws IOException {
        final String followerSeed = followerCluster.getTransportEndpoint(2);
        logger.info("Configuring follower remote cluster [{}]", followerSeed);
        final Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(Strings.format("""
            {
              "persistent": {
                "cluster.remote.follower.seeds": "%s"
              }
            }""", followerSeed));
        assertOK(leaderClient.performRequest(request));
        assertOK(followerClient.performRequest(request));
    }

    protected enum UpgradeState {
        NONE,
        ONE_THIRD,
        TWO_THIRD,
        ALL;

        static UpgradeState fromNodesUpgraded(int count) {
            return switch (count) {
                case 0 -> NONE;
                case 1 -> ONE_THIRD;
                case 2 -> TWO_THIRD;
                case 3 -> ALL;
                default -> throw new AssertionError("unexpected nodes upgraded: " + count);
            };
        }
    }

    protected enum ClusterName {
        LEADER,
        FOLLOWER
    }
}
