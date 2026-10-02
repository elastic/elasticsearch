/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.rest.yaml;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.RollingUpgradePerformer;
import org.elasticsearch.test.RollingUpgradePerformer.ClusterAndClients;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.AfterClass;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class for rolling upgrade YAML suites. YAML suites are run in phases: first the suites from the {@code old_cluster} directory are
 * run, then the {@code mixed_cluster} (twice), and finally the {@code upgraded_cluster}, driving the shared cluster one node further with
 * every phase. Phases therefore build on each other: an early phase typically indexes the documents or creates the resources a later phase
 * asserts on.
 * <p>
 * Because of that, a project hosting such a suite must opt out of smart retry's individual test pruning in its
 * {@code build.gradle}, otherwise a phase that passed in an earlier build attempt is skipped on retry and the later phase
 * it was setting up fails:
 * <pre>{@code smartRetry.pruneIndividualTests.set(false)}</pre>
 */
public abstract class ParameterizedYamlRollingUpgradeTestCase extends ESClientYamlSuiteTestCase {

    protected static final int NODE_NUM = 3;
    protected static final RollingUpgradePerformer rollingUpgrade = new RollingUpgradePerformer(NODE_NUM);

    private final int requestedUpgradedNodes;

    protected ParameterizedYamlRollingUpgradeTestCase(
        @Name("upgradedNodes") int upgradedNodes,
        @Name("yaml") ClientYamlTestCandidate testCandidate
    ) {
        super(testCandidate);
        this.requestedUpgradedNodes = upgradedNodes;
    }

    protected abstract ElasticsearchCluster getUpgradeCluster();

    /**
     * Builds the parameterized test list by loading YAML candidates from the directory that
     * corresponds to each upgrade phase. The four upgrade phases map to YAML test directories as follows:
     * <ul>
     *   <li>upgradedNodes=0 -> {@code old_cluster}</li>
     *   <li>upgradedNodes=1 -> {@code mixed_cluster}</li>
     *   <li>upgradedNodes=2 -> {@code mixed_cluster}</li>
     *   <li>upgradedNodes=3 -> {@code upgraded_cluster}</li>
     * </ul>
     */
    @ParametersFactory(shuffle = false)
    public static Iterable<Object[]> parameters() throws Exception {
        List<Object[]> result = new ArrayList<>();
        for (int i = 0; i <= NODE_NUM; i++) {
            String testPath = switch (i) {
                case 0 -> "old_cluster";
                case NODE_NUM -> "upgraded_cluster";
                default -> "mixed_cluster";
            };
            for (Object[] candidate : createParameters(testPath)) {
                result.add(new Object[] { i, candidate[0] });
            }
        }
        return result;
    }

    @Before
    public void upgradeNode() throws Exception {
        rollingUpgrade.upgradeNode(requestedUpgradedNodes, testFeatureService, new ClusterAndClients(getUpgradeCluster(), client(), () -> {
            initClient();
            initAndResetContext(); // YAML client
        }, () -> {
            closeClient(); // YAML client
            closeClients();
        }));
    }

    @AfterClass
    public static void resetNodes() {
        rollingUpgrade.reset();
    }

    @Override
    protected String getTestRestCluster() {
        return getUpgradeCluster().getHttpAddresses();
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
    protected boolean preserveDataStreamsUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveReposUponCompletion() {
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
    protected String getEnsureGreenTimeout() {
        // increase the timeout here to 70 seconds to handle long waits for a green
        // cluster health. the waits for green need to be longer than a minute to
        // account for delayed shards
        return "70s";
    }
}
