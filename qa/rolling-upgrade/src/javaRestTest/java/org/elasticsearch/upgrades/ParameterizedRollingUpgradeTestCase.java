/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.test.rest.TestFeatureService;
import org.junit.AfterClass;
import org.junit.Before;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public abstract class ParameterizedRollingUpgradeTestCase extends ESRestTestCase {
    protected static final int NODE_NUM = 3;
    private static final String OLD_CLUSTER_VERSION = System.getProperty("tests.old_cluster_version");
    private static final Set<Integer> upgradedNodes = new HashSet<>();
    private static TestFeatureService oldClusterTestFeatureService = null;
    private static boolean upgradeFailed = false;
    private static IndexVersion oldIndexVersion;
    private final int requestedUpgradedNodes;

    protected ParameterizedRollingUpgradeTestCase(@Name("upgradedNodes") int upgradedNodes) {
        this.requestedUpgradedNodes = upgradedNodes;
    }

    @ParametersFactory(shuffle = false)
    public static Iterable<Object[]> parameters() {
        return IntStream.rangeClosed(0, NODE_NUM).boxed().map(n -> new Object[] { n }).toList();
    }

    protected abstract ElasticsearchCluster getUpgradeCluster();

    @Before
    public void upgradeNode() throws Exception {
        // extract old cluster features
        if (isOldCluster() && oldClusterTestFeatureService == null) {
            oldClusterTestFeatureService = testFeatureService;
        }

        // extract old index version
        if (oldIndexVersion == null && upgradedNodes.isEmpty()) {
            IndexVersion indexVersion = null;   // these should all be the same version

            Request request = new Request("GET", "_nodes");
            request.addParameter("filter_path", "nodes.*.index_version,nodes.*.name");
            Response response = client().performRequest(request);
            ObjectPath objectPath = ObjectPath.createFromResponse(response);
            Map<String, Object> nodeMap = objectPath.evaluate("nodes");
            for (String id : nodeMap.keySet()) {
                Number ix = objectPath.evaluate("nodes." + id + ".index_version");
                final IndexVersion version;
                if (ix != null) {
                    version = IndexVersion.fromId(ix.intValue());
                } else {
                    // it doesn't have index version (pre 8.11) - just infer it from the release version
                    version = parseLegacyVersion(getOldClusterVersion()).map(v -> IndexVersion.fromId(v.id))
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
            oldIndexVersion = indexVersion;
        }

        // Skip remaining tests if upgrade failed
        assumeFalse("Cluster upgrade failed", upgradeFailed);

        // finally, upgrade node
        if (upgradedNodes.size() < requestedUpgradedNodes) {
            closeClients();
            // we might be running a specific upgrade test by itself - check previous nodes too
            for (int n = 0; n < requestedUpgradedNodes; n++) {
                if (upgradedNodes.add(n)) {
                    try {
                        Version upgradeVersion = System.getProperty("tests.new_cluster_version") == null
                            ? Version.CURRENT
                            : Version.fromString(System.getProperty("tests.new_cluster_version"));

                        logger.info("Upgrading node {} to version {}", n, upgradeVersion);
                        getUpgradeCluster().upgradeNodeToVersion(n, upgradeVersion);
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
        oldIndexVersion = null;
        upgradedNodes.clear();
        oldClusterTestFeatureService = null;
        upgradeFailed = false;
    }

    protected static boolean oldClusterHasFeature(String featureId) {
        assert oldClusterTestFeatureService != null;
        return oldClusterTestFeatureService.clusterHasFeature(featureId);
    }

    protected static boolean oldClusterHasFeature(NodeFeature feature) {
        return oldClusterHasFeature(feature.id());
    }

    protected static IndexVersion getOldClusterIndexVersion() {
        assert oldIndexVersion != null;
        return oldIndexVersion;
    }

    /**
     * The version of the "old" (initial) cluster. It is an opaque string, do not even think about parsing it for version
     * comparison. Use (test) cluster features and {@link ParameterizedRollingUpgradeTestCase#oldClusterHasFeature} instead.
     */
    protected static String getOldClusterVersion() {
        return System.getProperty("tests.bwc.main.version", OLD_CLUSTER_VERSION);
    }

    protected static boolean isOldClusterVersion(String nodeVersion) {
        return getOldClusterVersion().equals(nodeVersion);
    }

    protected static boolean isOldClusterVersion(String nodeVersion, String buildHash) {
        if (Objects.equals(System.getProperty("tests.bwc.refspec.main"), buildHash)) {
            return true;
        }
        return isOldClusterVersion(nodeVersion);
    }

    protected static boolean isOldCluster() {
        return upgradedNodes.isEmpty();
    }

    protected static boolean isFirstMixedCluster() {
        return upgradedNodes.size() == 1;
    }

    protected static boolean isMixedCluster() {
        return upgradedNodes.isEmpty() == false && upgradedNodes.size() < NODE_NUM;
    }

    protected static boolean isUpgradedCluster() {
        return upgradedNodes.size() == NODE_NUM;
    }

    @Override
    protected String getTestRestCluster() {
        return getUpgradeCluster().getHttpAddresses();
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
