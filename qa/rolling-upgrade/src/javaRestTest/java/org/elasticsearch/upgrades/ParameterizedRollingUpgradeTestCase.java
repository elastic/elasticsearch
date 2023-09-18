/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.AfterClass;
import org.junit.Before;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public abstract class ParameterizedRollingUpgradeTestCase extends ESRestTestCase {
    private static final Version OLD_CLUSTER_VERSION = Version.fromString(System.getProperty("tests.old_cluster_version"));

    private static int totalNodes = -1;
    private static final Set<Integer> upgradedNodes = new HashSet<>();
    private static boolean upgradeFailed = false;
    private static IndexVersion oldIndexVersion;

    private final Integer requestedUpgradeNode;

    protected ParameterizedRollingUpgradeTestCase(@Name("upgradeNode") Integer upgradeNode, @Name("totalNodes") int totalNodes) {
        if (ParameterizedRollingUpgradeTestCase.totalNodes == -1) {
            ParameterizedRollingUpgradeTestCase.totalNodes = totalNodes;
        } else {
            assertThat("The total number of nodes has changed", totalNodes, equalTo(ParameterizedRollingUpgradeTestCase.totalNodes));
        }
        this.requestedUpgradeNode = upgradeNode;
    }

    protected static Iterable<Object[]> testNodes(int numNodes) {
        return Stream.concat(Stream.of((Integer) null), IntStream.range(0, numNodes).boxed())
            .map(n -> new Object[] { n, numNodes })
            .toList();
    }

    @Before
    public void extractOldIndexVersion() throws Exception {
        if (oldIndexVersion == null && upgradedNodes.isEmpty()) {
            IndexVersion indexVersion = null;   // these should all be the same version

            Request request = new Request("GET", "_nodes");
            request.addParameter("filter_path", "nodes.*.index_version,nodes.*.name");
            Response response = client().performRequest(request);
            ObjectPath objectPath = ObjectPath.createFromResponse(response);
            Map<String, Object> nodeMap = objectPath.evaluate("nodes");
            for (String id : nodeMap.keySet()) {
                Number ix = objectPath.evaluate("nodes." + id + ".index_version");
                IndexVersion version;
                if (ix != null) {
                    version = IndexVersion.fromId(ix.intValue());
                } else {
                    // it doesn't have index version (pre 8.11) - just infer it from the release version
                    version = IndexVersion.fromId(getOldClusterVersion().id);
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
    }

    @Before
    public void checkUpgrade() throws Exception {
        // Skip remaining tests if upgrade failed
        assumeFalse("Cluster upgrade failed", upgradeFailed);

        if (requestedUpgradeNode != null && upgradedNodes.add(requestedUpgradeNode)) {
            try {
                getUpgradeCluster().upgradeNodeToVersion(requestedUpgradeNode, Version.CURRENT);
                closeClients();
                initClient();
            } catch (Exception e) {
                upgradeFailed = true;
                throw e;
            }
        }
    }

    @AfterClass
    public static void resetNodes() {
        oldIndexVersion = null;
        totalNodes = -1;
        upgradedNodes.clear();
        upgradeFailed = false;
    }

    protected static org.elasticsearch.Version getOldClusterVersion() {
        return org.elasticsearch.Version.fromString(OLD_CLUSTER_VERSION.toString());
    }

    protected static IndexVersion getOldClusterIndexVersion() {
        assert oldIndexVersion != null;
        return oldIndexVersion;
    }

    protected static Version getOldClusterTestVersion() {
        return Version.fromString(OLD_CLUSTER_VERSION.toString());
    }

    protected abstract ElasticsearchCluster getUpgradeCluster();

    protected static boolean isOldCluster() {
        return upgradedNodes.isEmpty();
    }

    protected static boolean isFirstMixedCluster() {
        return upgradedNodes.size() == 1;
    }

    protected static boolean isMixedCluster() {
        return upgradedNodes.isEmpty() == false && upgradedNodes.size() < totalNodes;
    }

    protected static boolean isUpgradedCluster() {
        return upgradedNodes.size() == totalNodes;
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
    protected final Settings restClientSettings() {
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
