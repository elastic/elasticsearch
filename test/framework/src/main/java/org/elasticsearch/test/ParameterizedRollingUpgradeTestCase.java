/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.RollingUpgradePerformer.ClusterAndClients;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.AfterClass;
import org.junit.Before;

import java.util.stream.IntStream;

/**
 * Base class for rolling upgrade suites. Each suite is run once per upgrade phase, from a fully old cluster through to a
 * fully upgraded one, driving the shared cluster one node further with every phase. Phases therefore build on each other:
 * an early phase typically indexes the documents or creates the resources a later phase asserts on.
 * <p>
 * Because of that, a project hosting such a suite must opt out of smart retry's individual test pruning in its
 * {@code build.gradle}, otherwise a phase that passed in an earlier build attempt is skipped on retry and the later phase
 * it was setting up fails:
 * <pre>{@code smartRetry.pruneIndividualTests.set(false)}</pre>
 */
public abstract class ParameterizedRollingUpgradeTestCase extends ESRestTestCase {

    protected static final int NODE_NUM = 3;
    protected static final RollingUpgradePerformer rollingUpgrade = new RollingUpgradePerformer(NODE_NUM);

    private final int requestedUpgradedNodes;

    protected ParameterizedRollingUpgradeTestCase(@Name("upgradedNodes") int upgradedNodes) {
        this.requestedUpgradedNodes = upgradedNodes;
    }

    @ParametersFactory(shuffle = false)
    public static Iterable<Object[]> parameters() {
        return IntStream.rangeClosed(0, NODE_NUM).boxed().map(n -> new Object[] { n }).toList();
    }

    @Before
    public final void upgradeNode() throws Exception {
        rollingUpgrade.upgradeNode(
            requestedUpgradedNodes,
            testFeatureService,
            new ClusterAndClients(getUpgradeCluster(), client(), this::initClient, ESRestTestCase::closeClients)
        );
    }

    protected abstract ElasticsearchCluster getUpgradeCluster();

    @Override
    protected String getTestRestCluster() {
        return getUpgradeCluster().getHttpAddresses();
    }

    @AfterClass
    public static void resetNodes() {
        rollingUpgrade.reset();
    }

    protected static boolean oldClusterHasFeature(String featureId) {
        return rollingUpgrade.oldClusterHasFeature(featureId);
    }

    protected static boolean oldClusterHasFeature(NodeFeature feature) {
        return rollingUpgrade.oldClusterHasFeature(feature);
    }

    protected static IndexVersion getOldClusterIndexVersion() {
        return rollingUpgrade.getOldClusterIndexVersion();
    }

    /**
     * The version of the "old" (initial) cluster. It is an opaque string, do not even think about parsing it for version
     * comparison. Use (test) cluster features and {@link ParameterizedRollingUpgradeTestCase#oldClusterHasFeature} instead.
     */
    protected static String getOldClusterVersion() {
        return RollingUpgradePerformer.getOldClusterVersion();
    }

    protected static boolean isOldClusterVersion(String nodeVersion, String buildHash) {
        return RollingUpgradePerformer.isOldClusterVersion(nodeVersion, buildHash);
    }

    protected static boolean isOldCluster() {
        return rollingUpgrade.isOldCluster();
    }

    protected static boolean isFirstMixedCluster() {
        return rollingUpgrade.isFirstMixedCluster();
    }

    protected static boolean isMixedCluster() {
        return rollingUpgrade.isMixedCluster();
    }

    protected static boolean isUpgradedCluster() {
        return rollingUpgrade.isUpgradedCluster();
    }

    @Override
    protected final boolean resetFeatureStates() {
        return false;
    }

    @Override
    protected final boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected final boolean preserveDataStreamsUponCompletion() {
        return true;
    }

    @Override
    protected final boolean preserveReposUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveTemplatesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @Override
    protected Settings restClientSettings() {
        return Settings.builder()
            .put(super.restClientSettings())
            // increase the timeout here to 90 seconds to handle long waits for a green
            // cluster health. the waits for green need to be longer than a minute to
            // account for delayed shards
            .put(ESRestTestCase.CLIENT_SOCKET_TIMEOUT, "90s")
            .build();
    }

    @Override
    protected final String getEnsureGreenTimeout() {
        // increase the timeout here to 70 seconds to handle long waits for a green
        // cluster health. the waits for green need to be longer than a minute to
        // account for delayed shards
        return "70s";
    }
}
