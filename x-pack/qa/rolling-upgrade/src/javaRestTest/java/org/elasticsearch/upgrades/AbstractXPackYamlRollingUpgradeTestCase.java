/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.util.HashSet;
import java.util.Set;

/**
 * Base class for the x-pack YAML rolling upgrade suite ({@link UpgradeClusterClientYamlTestSuiteIT}), running
 * against the same cluster lifecycle as {@link AbstractXPackRollingUpgradeTestCase} but for
 * {@link ESClientYamlSuiteTestCase} subclasses.
 *
 * <p>This class deliberately duplicates the small slice of upgrade-lifecycle bookkeeping found in
 * {@code ParameterizedRollingUpgradeTestCase} (tracking how many nodes have been upgraded, exposing
 * {@code isOldCluster()}/{@code isMixedCluster()}/etc., and upgrading nodes lazily in a {@code @Before} method)
 * rather than inheriting or delegating to it. Both {@code ParameterizedRollingUpgradeTestCase} and
 * {@link ESClientYamlSuiteTestCase} each already own the single {@code @ParametersFactory} method a JUnit
 * parameterized test class is allowed to have: the former uses it to enumerate {@code upgradedNodes}, and the
 * latter uses it to enumerate YAML test candidates. {@link UpgradeClusterClientYamlTestSuiteIT} needs both
 * dimensions at once (a cross product), so there is no single class it could extend that already provides
 * exactly one of these two behaviors without conflicting with the other. Hence the duplication here, instead of
 * trying to force a shared ancestor. Do not attempt to collapse this into
 * {@code ParameterizedRollingUpgradeTestCase} - that would reintroduce the conflict this class exists to avoid.
 */
public abstract class AbstractXPackYamlRollingUpgradeTestCase extends ESClientYamlSuiteTestCase {

    private static final TemporaryFolder repoDirectory = new TemporaryFolder();

    private static final ElasticsearchCluster cluster = XPackRollingUpgradeClusterConfig.buildCluster(
        getOldClusterVersion(),
        isOldClusterDetachedVersion(),
        repoDirectory
    );

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(repoDirectory).around(cluster);

    private static final Set<Integer> upgradedNodes = new HashSet<>();
    private static boolean upgradeFailed = false;

    private final int requestedUpgradedNodes;

    protected AbstractXPackYamlRollingUpgradeTestCase(@Name("upgradedNodes") int upgradedNodes, ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
        this.requestedUpgradedNodes = upgradedNodes;
    }

    /**
     * The version of the "old" (initial) cluster. It is an opaque string, do not even think about parsing it for
     * version comparison.
     */
    protected static String getOldClusterVersion() {
        return System.getProperty("tests.bwc.main.version", System.getProperty("tests.old_cluster_version"));
    }

    @Before
    public void upgradeNode() throws Exception {
        assumeFalse("Cluster upgrade failed", upgradeFailed);

        if (upgradedNodes.size() < requestedUpgradedNodes) {
            closeClients();
            for (int n = 0; n < requestedUpgradedNodes; n++) {
                if (upgradedNodes.add(n)) {
                    try {
                        Version upgradeVersion = System.getProperty("tests.new_cluster_version") == null
                            ? Version.CURRENT
                            : Version.fromString(System.getProperty("tests.new_cluster_version"));

                        logger.info("Upgrading node {} to version {}", n, upgradeVersion);
                        cluster.upgradeNodeToVersion(n, upgradeVersion);
                    } catch (Exception e) {
                        upgradeFailed = true;
                        throw e;
                    }
                }
            }
            initClient();
        }
    }

    @AfterClass
    public static void resetNodes() {
        upgradedNodes.clear();
        upgradeFailed = false;
    }

    protected static boolean isOldCluster() {
        return upgradedNodes.isEmpty();
    }

    protected static boolean isFirstMixedCluster() {
        return upgradedNodes.size() == 1;
    }

    protected static boolean isMixedCluster() {
        return upgradedNodes.isEmpty() == false && upgradedNodes.size() < XPackRollingUpgradeClusterConfig.NODE_NUM;
    }

    protected static boolean isUpgradedCluster() {
        return upgradedNodes.size() == XPackRollingUpgradeClusterConfig.NODE_NUM;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        return Settings.builder()
            .put(super.restClientSettings())
            .put(
                ThreadContext.PREFIX + ".Authorization",
                basicAuthHeaderValue("test_user", new SecureString("x-pack-test-password".toCharArray()))
            )
            // increase the timeout here to 90 seconds to handle long waits for a green
            // cluster health. the waits for green need to be longer than a minute to
            // account for delayed shards
            .put(ESRestTestCase.CLIENT_SOCKET_TIMEOUT, "90s")
            .build();
    }

    @Override
    protected boolean resetFeatureStates() {
        return false;
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveTemplatesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveRollupJobsUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveILMPoliciesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveDataStreamsUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveReposUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveSnapshotsUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveSearchableSnapshotsIndicesUponCompletion() {
        return true;
    }
}
