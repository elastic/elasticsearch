/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.test.rest.TestFeatureService;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

/**
 * Encapsulates the state and logic for performing a rolling upgrade of an Elasticsearch cluster
 * during test execution. This class is designed to be used as a {@code private static} field in
 * rolling upgrade test classes, enabling composition over inheritance and allowing tests that
 * must extend a different base class to still participate in rolling upgrade suites.
 *
 * <p>Instances of this class are <em>not</em> thread-safe; rolling upgrade tests are inherently
 * sequential (each phase builds on the previous one), so no synchronization is required.
 *
 * <p>Usage pattern:
 * <pre>{@code
 * public class MyRollingUpgradeTest extends ESRestTestCase {
 *
 *     static final int NODE_NUM = 3;
 *     static final RollingUpgradePerformer rollingUpgrade = new RollingUpgradePerformer(NODE_NUM);
 *
 *     @ParametersFactory(shuffle = false)
 *     public static Iterable<Object[]> parameters() {
 *         return IntStream.rangeClosed(0, NODE_NUM).boxed().map(n -> new Object[] { n }).toList();
 *     }
 *
 *     @Before
 *     public void upgradeNode() throws Exception {
 *         rollingUpgrade.upgradeNode(
 *             requestedUpgradedNodes,
 *             testFeatureService,
 *             new ClusterAndClients(getCluster(), client(), this::initClient, ESRestTestCase::closeClients)
 *         );
 *     }
 *
 *     @AfterClass
 *     public static void resetNodes() {
 *         rollingUpgrade.reset();
 *     }
 * }
 * }</pre>
 *
 * @see ParameterizedRollingUpgradeTestCase
 */
public class RollingUpgradePerformer {

    private static final Logger logger = LogManager.getLogger(RollingUpgradePerformer.class);
    private static final String CURRENT_ES_VERSION = Build.current().version();

    private final int totalNodeCount;
    private final Set<Integer> upgradedNodes = new HashSet<>();

    private boolean upgradeFailed = false;
    @Nullable
    private TestFeatureService oldClusterTestFeatureService = null;
    @Nullable
    private IndexVersion oldIndexVersion = null;

    /**
     * Creates a rolling upgrade performer for a cluster with the given number of nodes.
     *
     * @param totalNodeCount the total number of nodes in the cluster being upgraded
     */
    public RollingUpgradePerformer(int totalNodeCount) {
        this.totalNodeCount = totalNodeCount;
    }

    /**
     * Performs the rolling upgrade step for the current test phase.
     * This method is intended to be called from a {@code @Before} method on the test class.
     *
     * @param requestedUpgradedNodes the number of nodes that should be upgraded for this phase
     * @param currentFeatureService  the current test's feature service
     * @param clusterAndClients      the cluster to upgrade together with the REST clients needed to
     *                               close, re-initialize, and query it
     */
    public void upgradeNode(int requestedUpgradedNodes, TestFeatureService currentFeatureService, ClusterAndClients clusterAndClients)
        throws Exception {
        // Capture old cluster features before any node is upgraded
        if (isOldCluster() && oldClusterTestFeatureService == null) {
            oldClusterTestFeatureService = currentFeatureService;
        }

        // Capture old index version before any node is upgraded
        if (oldIndexVersion == null && upgradedNodes.isEmpty()) {
            oldIndexVersion = readOldIndexVersion(clusterAndClients.client());
        }

        // Skip remaining tests if a previous upgrade step failed
        assumeFalse("Cluster upgrade failed", upgradeFailed);

        // SNAPSHOT old clusters must carry the pattern_text mapper rename before we can proceed
        if (getOldClusterVersion().endsWith("-SNAPSHOT")) {
            assumeTrue("rename of pattern_text mapper", oldClusterHasFeature("mapper.pattern_text_rename"));
        }

        // Upgrade any nodes that have not yet been upgraded for this phase.
        // We might be running a specific upgrade test by itself, so we also catch up on prior nodes.
        if (upgradedNodes.size() < requestedUpgradedNodes) {
            clusterAndClients.closeAllClients().run();
            for (int n = 0; n < requestedUpgradedNodes; n++) {
                if (upgradedNodes.add(n)) {
                    try {
                        String newClusterVersion = getNewClusterVersion();
                        logger.info("Upgrading node {} to version {}", n, newClusterVersion);
                        clusterAndClients.cluster().upgradeNodeToVersion(n, Version.fromString(newClusterVersion));
                    } catch (Exception e) {
                        upgradeFailed = true;
                        throw e;
                    }
                }
            }
            clusterAndClients.initAllClients().run();
        }
    }

    /**
     * Resets all upgrade state. Must be called from an {@code @AfterClass} method on the test
     * class so that state does not leak between test classes sharing the same JVM.
     */
    public void reset() {
        oldIndexVersion = null;
        upgradedNodes.clear();
        oldClusterTestFeatureService = null;
        upgradeFailed = false;
    }

    public boolean oldClusterHasFeature(String featureId) {
        assert oldClusterTestFeatureService != null : "Run upgradeNode() at least once before calling this";
        return oldClusterTestFeatureService.clusterHasFeature(featureId);
    }

    public boolean oldClusterHasFeature(NodeFeature feature) {
        return oldClusterHasFeature(feature.id());
    }

    public IndexVersion getOldClusterIndexVersion() {
        assert oldIndexVersion != null : "Run upgradeNode() at least once before calling this";
        return oldIndexVersion;
    }

    /**
     * Returns the version string of the initial (old) cluster.
     *
     * <p>This is an opaque string — do not parse it for version comparisons. Use
     * {@link #oldClusterHasFeature} instead.
     */
    public static String getOldClusterVersion() {
        return System.getProperty("tests.bwc.main.version", System.getProperty("tests.old_cluster_version"));
    }

    public static String getNewClusterVersion() {
        return System.getProperty("tests.new_cluster_version", CURRENT_ES_VERSION);
    }

    /**
     * Returns {@code true} if the node identified by the given version string and build hash
     * belongs to the old (pre-upgrade) cluster.
     */
    public static boolean isOldClusterVersion(String nodeVersion, String buildHash) {
        if (ESRestTestCase.isOldClusterDetachedVersion()) {
            return System.getProperty("tests.bwc.refspec.main").equals(buildHash);
        }
        return getOldClusterVersion().equals(nodeVersion);
    }

    public boolean isOldCluster() {
        return upgradedNodes.isEmpty();
    }

    public boolean isFirstMixedCluster() {
        return upgradedNodes.size() == 1;
    }

    public boolean isMixedCluster() {
        return upgradedNodes.isEmpty() == false && upgradedNodes.size() < totalNodeCount;
    }

    public boolean isUpgradedCluster() {
        return upgradedNodes.size() == totalNodeCount;
    }

    private static IndexVersion readOldIndexVersion(RestClient client) throws IOException {
        IndexVersion indexVersion = null;   // these should all be the same version

        Request request = new Request("GET", "_nodes");
        request.addParameter("filter_path", "nodes.*.index_version,nodes.*.name");
        Response response = client.performRequest(request);
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        Map<String, Object> nodeMap = objectPath.evaluate("nodes");
        for (String id : nodeMap.keySet()) {
            Number ix = objectPath.evaluate("nodes." + id + ".index_version");
            final IndexVersion version;
            if (ix != null) {
                version = IndexVersion.fromId(ix.intValue());
            } else {
                // Node does not report index_version (pre-8.11): infer it from the release version
                version = ESRestTestCase.parseLegacyVersion(getOldClusterVersion())
                    .map(v -> IndexVersion.fromId(v.id))
                    .orElse(IndexVersions.MINIMUM_COMPATIBLE);
            }

            if (indexVersion == null) {
                indexVersion = version;
            } else {
                String name = objectPath.evaluate("nodes." + id + ".name");
                assertThat("Node " + name + " has a different index version to other nodes", version, equalTo(indexVersion));
            }
        }

        assertThat("Index version could not be read", indexVersion, notNullValue());
        return indexVersion;
    }

    /**
     * Bundles the cluster under upgrade together with the REST client callbacks needed to
     * tear down and re-establish connections around a node restart.
     *
     * @param cluster         the cluster whose nodes are being upgraded
     * @param client          a REST client connected to the cluster
     * @param initAllClients  callback that initializes (or re-initializes) all REST clients after a node has restarted
     * @param closeAllClients callback that closes all REST clients before a node is restarted
     */
    public record ClusterAndClients(
        ElasticsearchCluster cluster,
        RestClient client,
        CheckedRunnable<Exception> initAllClients,
        CheckedRunnable<Exception> closeAllClients
    ) {}
}
