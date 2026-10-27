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
import org.elasticsearch.test.rest.TestFeatureService;
import org.elasticsearch.test.rest.yaml.section.ExecutableSection;
import org.junit.AfterClass;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

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
 * <p>
 * <b>Synthetic cluster features for phase gating</b><br>
 * The following synthetic feature IDs are injected into the YAML test runner so that individual test sections can gate themselves
 * on the current upgrade phase using {@code requires.cluster_features} or {@code skip.cluster_features}:
 * <ul>
 *   <li>{@code rolling_upgrade.old_cluster}: all nodes are still on the old version</li>
 *   <li>{@code rolling_upgrade.first_mixed_cluster}: exactly one node has been upgraded</li>
 *   <li>{@code rolling_upgrade.mixed_cluster}: at least one but not all nodes have been upgraded</li>
 *   <li>{@code rolling_upgrade.upgraded_cluster}: all nodes have been upgraded to the new version</li>
 * </ul>
 * Example: run a test only in the second mixed-cluster pass (two nodes upgraded, not one):
 * <pre>{@code
 * - skip:
 *     cluster_features: ["rolling_upgrade.first_mixed_cluster"]
 *     reason: "Creates durable state — must run exactly once"
 * }</pre>
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
     * The per-task scoped {@code tests.rest.suite.<task path>} filter selects suites by file name in every phase, because a
     * phase depends on the same-named suites of the phases before it.
     */
    @ParametersFactory(shuffle = false)
    public static Iterable<Object[]> parameters() throws Exception {
        String task = System.getProperty("tests.task");
        String scoped = task == null ? null : System.getProperty(REST_TESTS_SUITE + "." + task);
        return phaseParameters(scoped, "old_cluster", "mixed_cluster", "mixed_cluster", "upgraded_cluster");
    }

    // pkg private for tests
    static List<Object[]> phaseParameters(String scoped, String... testPaths) throws Exception {
        Predicate<String> suiteFilter = suiteNameFilter(scoped);
        String global = scoped == null ? System.getProperty(REST_TESTS_SUITE) : null;
        List<Object[]> result = new ArrayList<>();
        for (int i = 0; i < testPaths.length; i++) {
            for (Object[] candidate : explicitPathParameters(ExecutableSection.XCONTENT_REGISTRY, Map.of(), null, global, testPaths[i])) {
                if (suiteFilter.test(((ClientYamlTestCandidate) candidate[0]).getName())) {
                    result.add(new Object[] { i, candidate[0] });
                }
            }
        }
        return result;
    }

    /** Matches suites by file name, or every suite when the filter is unset or names a whole directory. */
    private static Predicate<String> suiteNameFilter(String scoped) {
        if (scoped == null) {
            return name -> true;
        }
        Set<String> names = new HashSet<>();
        for (String path : scoped.split(",")) {
            int slash = path.lastIndexOf('/');
            if (slash < 0) {
                return name -> true;
            }
            names.add(path.substring(slash + 1));
        }
        return names::contains;
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

    /**
     * Wraps the real {@link TestFeatureService} with synthetic rolling-upgrade phase features so that YAML test sections can use
     * {@code requires.cluster_features} / {@code skip.cluster_features} to gate themselves on the current upgrade phase.
     */
    @Override
    protected ClientYamlTestExecutionContext createRestTestExecutionContext(
        ClientYamlTestCandidate clientYamlTestCandidate,
        ClientYamlTestClient clientYamlTestClient,
        Set<String> nodesVersions,
        TestFeatureService testFeatureService,
        Set<String> osSet
    ) {
        TestFeatureService wrapped = (featureId, any) -> switch (featureId) {
            case "rolling_upgrade.old_cluster" -> rollingUpgrade.isOldCluster();
            case "rolling_upgrade.first_mixed_cluster" -> rollingUpgrade.isFirstMixedCluster();
            case "rolling_upgrade.mixed_cluster" -> rollingUpgrade.isMixedCluster();
            case "rolling_upgrade.upgraded_cluster" -> rollingUpgrade.isUpgradedCluster();
            default -> testFeatureService.clusterHasFeature(featureId, any);
        };
        return super.createRestTestExecutionContext(clientYamlTestCandidate, clientYamlTestClient, nodesVersions, wrapped, osSet);
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
